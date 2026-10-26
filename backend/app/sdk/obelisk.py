"""
Obelisk SDK: Zero-config notebook and analytical client.
Usage:
    import obelisk
    df = obelisk.table("raw.orders").to_polars()
    res = obelisk.query("SELECT * FROM read_parquet(...)")
    obelisk.save(df, "marts/monthly_revenue")
    spark = obelisk.spark()

A table is a Parquet file at a path. There are no layers to create: the folder you
name is the schema, and the file you write is the table.
"""

import io
import logging
import os
import sys
import json
import uuid
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

from app.config import settings
from app.core.catalog import SOURCE, fqn_for, path_for, split_table_path, sql_name_for
from app.core.resource_advisor import advisor
from app.core.dsn import build_pg_dsn, sql_string, validate_alias
from app.core.storage import warehouse_storage
from app.engines.duckdb_engine import DuckDBEngine

duckdb_engine = DuckDBEngine()
logger = logging.getLogger(__name__)

# One shared connection for SDK helpers that do not receive an explicit one.
# Creating a fresh in-memory connection per call leaked a DuckDB instance every
# time to_duckdb()/to_arrow() was used (audit M4).
_IMPLICIT_DUCKDB_CONN = None


def _implicit_connection():
    """Returns the shared SDK connection, creating it on first use."""
    global _IMPLICIT_DUCKDB_CONN
    if _IMPLICIT_DUCKDB_CONN is None:
        _IMPLICIT_DUCKDB_CONN = duckdb_engine.get_connection()
    return _IMPLICIT_DUCKDB_CONN


class TableWrapper:
    """Wrapper around lakehouse datasets and external tables supporting zero-copy format conversions."""

    def __init__(self, name: str, folder: str, file_path: Path, full_identifier: Optional[str] = None):
        self.name = name
        self.folder = folder
        self.file_path = file_path
        self.full_identifier = full_identifier or (f"{folder}.{name}" if folder else name)

    @property
    def _readable_file(self) -> Optional[Path]:
        """The single Parquet file behind this table, when there is one.

        With the catalog in charge, a table lives in a *directory* named after it, so a
        path that exists is not necessarily a table — reading it as one returned an empty
        dataset instead of an error. Only a real ``.parquet`` file is read directly;
        everything else goes through SQL.
        """
        path = self.file_path
        if path is None:
            return None
        return path if path.is_file() and path.suffix.lower() == ".parquet" else None

    def exists(self) -> bool:
        if self._readable_file:
            return True
        try:
            res = query(f"SELECT 1 FROM {self.full_identifier} LIMIT 1")
            return len(res) > 0
        except Exception:
            return False

    def to_arrow(self) -> pa.Table:
        """Reads the table into a PyArrow Table, or queries an external database table."""
        if self._readable_file:
            return pq.read_table(str(self._readable_file))
        try:
            res = query_arrow(f"SELECT * FROM {self.full_identifier}")
            if hasattr(res, "read_all"):
                return res.read_all()
            return res
        except Exception as e:
            raise FileNotFoundError(
                f"Dataset '{self.full_identifier}' not found in the lakehouse or "
                f"attached external sources: {e}"
            ) from e

    def to_polars(self) -> pl.DataFrame:
        """Reads the table into a Polars DataFrame, or queries an external database table."""
        if self._readable_file:
            return pl.read_parquet(str(self._readable_file))
        try:
            return query(f"SELECT * FROM {self.full_identifier}")
        except Exception as e:
            raise FileNotFoundError(
                f"Dataset '{self.full_identifier}' not found in the lakehouse or "
                f"attached external sources: {e}"
            ) from e

    def to_df(self) -> Any:
        """Reads dataset into a Pandas DataFrame."""
        arrow_obj = self.to_arrow()
        if hasattr(arrow_obj, "read_all"):
            arrow_obj = arrow_obj.read_all()
        return arrow_obj.to_pandas()

    def to_duckdb(self, conn=None):
        """Returns a DuckDB relation referencing the dataset.

        Pass ``conn`` to control the connection's lifetime. When omitted, the
        shared SDK connection is reused instead of opening a new one per call
        (audit M4).
        """
        c = conn or _implicit_connection()
        if self._readable_file:
            return c.from_parquet(self._readable_file.resolve().as_posix())
        for att in _get_sdk_pg_attachments(self.full_identifier):
            duckdb_engine.attach_postgres(c, **att)
        return c.query(f"SELECT * FROM {self.full_identifier}")

    def to_spark(self, spark_session=None):
        """
        Converts the dataset to a PySpark DataFrame.
        Bypasses Hadoop's winutils.exe on Windows by passing in-memory Arrow/Pandas
        data directly to spark.createDataFrame().
        """
        spark_sess = spark_session or spark()
        pandas_df = self.to_df()
        return spark_sess.createDataFrame(pandas_df)

    def __repr__(self) -> str:
        status = "exists" if self.exists() else "not_found"
        return f"<ObeliskTable: {self.full_identifier} [{status}] -> {self.full_identifier}>"


def _resolve_external_identifier(identifier: str) -> str:
    """
    Checks registered connections in the SQLite vault to resolve an external table.
    Examples:
        'supabase.public.venues_old' -> 'supabase.public.venues_old'
        'public.venues_old' -> 'supabase.public.venues_old' (if venues_old in supabase)
        'venues_old' -> 'supabase.public.venues_old'
    """
    import re
    db_path = settings.sqlite_db_path
    if not Path(db_path).exists():
        return identifier

    clean = identifier.strip().replace("`", "").replace('"', "")
    parts = clean.split(".")

    try:
        with sqlite3.connect(db_path, timeout=5.0) as con:
            cur = con.cursor()
            rows = cur.execute(
                "SELECT name, database FROM connections WHERE source_type = 'postgres';"
            ).fetchall()
            aliases = [re.sub(r"[^a-zA-Z0-9_]", "_", r[0].lower()) for r in rows]

            # If already starts with a known connection alias
            if parts[0].lower() in aliases:
                return clean

            # Try prefixing with registered postgres connections
            for alias in aliases:
                test_sql = f"SELECT 1 FROM {alias}.{clean} LIMIT 1"
                try:
                    res = query(test_sql)
                    if len(res) >= 0:
                        return f"{alias}.{clean}"
                except Exception:
                    pass

            # Fallback: if single connection exists, prefix it
            if len(aliases) == 1:
                return f"{aliases[0]}.{clean}"
    except Exception:
        pass

    return clean


def table(identifier: str) -> TableWrapper:
    """Resolves a warehouse table by its address, or an external table by its FQN.

    Accepts the canonical address (``lakehouse.marts.sales.orders``), a path
    (``marts/sales/orders``), a folder plus name (``marts.orders``), or a bare name
    when exactly one table answers to it. Resolution reads the warehouse — the same
    scan the editor and the catalog use — so there is no second registry to disagree
    with.
    """
    from app.core.catalog import scan
    from app.core.storage import warehouse_storage

    clean_id = identifier.strip().replace("`", "").replace('"', "")
    store = warehouse_storage()
    stripped = (
        clean_id[len("lakehouse."):] if clean_id.lower().startswith("lakehouse.") else clean_id
    )
    wanted = stripped.replace(".", "/").lower()

    result = scan(store)

    # 1. An exact address: folder path plus table name.
    for entry in result.tables:
        if entry.path[: -len(".parquet")].lower() == wanted:
            return TableWrapper(
                name=entry.name,
                folder=entry.folder,
                file_path=store.root / entry.path,
                full_identifier=entry.fqn,
            )

    # 2. A bare name, or a folder/name whose folder was spelled with dots, is still
    # unambiguous while only one table carries that name.
    leaf = wanted.rsplit("/", 1)[-1]
    matches = [entry for entry in result.tables if entry.name.lower() == leaf]
    if len(matches) == 1:
        entry = matches[0]
        return TableWrapper(
            name=entry.name,
            folder=entry.folder,
            file_path=store.root / entry.path,
            full_identifier=entry.fqn,
        )

    # 3. Not a warehouse table: it may be an attached database's.
    parts = clean_id.split(".")
    resolved = _resolve_external_identifier(clean_id)
    return TableWrapper(
        name=parts[-1],
        folder=".".join(parts[:-2]) if len(parts) >= 3 else "main",
        file_path=None,
        full_identifier=resolved,
    )


def _get_sdk_pg_attachments(sql: str) -> list:
    """Discovers and resolves referenced PostgreSQL connections from the SQLite vault."""
    attachments = []
    try:
        import re
        from app.core.vault import decrypt
        from app.core.host_resolver import resolve_connection_host
        db_path = settings.sqlite_db_path
        if not Path(db_path).exists():
            return attachments
        with sqlite3.connect(db_path, timeout=5.0) as con:
            cur = con.cursor()
            rows = cur.execute(
                "SELECT name, host, port, database, username, encrypted_credentials, options FROM connections WHERE source_type = 'postgres';"
            ).fetchall()
            for name, host, port, dbname, user, enc_pw, options_json in rows:
                alias = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
                clean_sql = re.sub(r"--[^\n]*", "", sql)
                clean_sql = re.sub(r"/\*.*?\*/", "", clean_sql, flags=re.DOTALL)
                tokens = set(re.findall(r"[a-zA-Z0-9_]+", clean_sql.lower()))
                if alias.lower() in tokens or (dbname and dbname.lower() in tokens):
                    pw = decrypt(enc_pw)
                    opts = json.loads(options_json) if options_json else {}
                    attachments.append({
                        "alias": alias,
                        "host": resolve_connection_host(host, int(port)),
                        "port": int(port),
                        "database": dbname,
                        "username": user,
                        "password": pw,
                        "sslmode": opts.get("sslmode", "disable"),
                    })
    except Exception:
        pass
    return attachments


def query(sql: str) -> pl.DataFrame:
    """Executes SQL query using DuckDB in-process engine and returns a Polars DataFrame."""
    conn = duckdb_engine.get_connection()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for att in _get_sdk_pg_attachments(sql):
            duckdb_engine.attach_postgres(
                conn=conn,
                alias=att["alias"],
                host=att["host"],
                port=att["port"],
                database=att["database"],
                username=att["username"],
                password=att["password"],
                sslmode=att.get("sslmode", "disable"),
            )
        exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
        rel = conn.sql(exec_sql)
        if rel is None:
            return pl.DataFrame()
        arrow_table = rel.arrow()
        return pl.from_arrow(arrow_table)
    finally:
        conn.close()


def query_arrow(sql: str) -> pa.Table:
    """Executes SQL query using DuckDB in-process engine and returns a PyArrow Table."""
    conn = duckdb_engine.get_connection()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for att in _get_sdk_pg_attachments(sql):
            duckdb_engine.attach_postgres(
                conn=conn,
                alias=att["alias"],
                host=att["host"],
                port=att["port"],
                database=att["database"],
                username=att["username"],
                password=att["password"],
                sslmode=att.get("sslmode", "disable"),
            )
        exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
        rel = conn.sql(exec_sql)
        if rel is None:
            return pa.Table.from_arrays([], names=[])
        res = rel.arrow()
        if hasattr(res, "read_all"):
            return res.read_all()
        return res
    finally:
        conn.close()


def save(df: Any, target: str, sync_postgres: Optional[bool] = None) -> Dict[str, Any]:
    """Persists a DataFrame as a table in the lakehouse.

    ``target`` is a warehouse path — ``"supabase/auth/table1"``, the fully qualified
    ``"lakehouse.supabase.auth.table1"``, or a bare ``"orders"`` for the root. Dots and
    slashes are both separators, so a layer-style ``"marts.sales.orders"`` still works
    and lands in schema ``marts.sales``.

    The catalog is the table: one statement replaces it, and the row count, schema and
    size follow from the catalog without anything being registered by hand.
    """
    folder, stem = split_table_path(target)
    schema_name = folder.replace("/", ".") if folder else "main"
    rel_path = f"{folder}/{stem}" if folder else stem

    # 1. Normalize input to PyArrow Table
    if isinstance(df, pl.DataFrame):
        arrow_table = df.to_arrow()
    elif isinstance(df, pa.Table):
        arrow_table = df
    elif hasattr(df, "to_arrow"):
        arrow_table = df.to_arrow()
    elif hasattr(df, "toPandas"):  # PySpark DataFrame
        arrow_table = pa.Table.from_pandas(df.toPandas())
    elif hasattr(df, "to_pandas"):
        arrow_table = pa.Table.from_pandas(df.to_pandas())
    elif str(type(df)).find("pandas.core.frame.DataFrame") != -1:
        arrow_table = pa.Table.from_pandas(df)
    else:
        raise TypeError(f"Unsupported DataFrame type '{type(df)}' for obelisk.save().")

    # 2. One statement, through the same connection factory as everything else.
    from app.core.catalog import quote_ident
    from app.engines.duckdb_engine import duckdb_engine

    address = f'{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}'
    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
        conn.register("_obelisk_save", arrow_table)
        try:
            conn.execute(
                f"CREATE OR REPLACE TABLE {address} AS SELECT * FROM _obelisk_save"
            )
        finally:
            conn.unregister("_obelisk_save")
    finally:
        conn.close()

    num_rows = arrow_table.num_rows

    # 3. Mirror into PostgreSQL when the caller wants it. Read from the lake, not from a
    # file, and report a failure rather than swallow it: the save itself already landed.
    warehouse_sync: Optional[Dict[str, Any]] = None
    if sync_postgres is not False:
        try:
            _sync_table_to_postgres(schema=schema_name, table_name=stem)
            warehouse_sync = {"status": "success", "schema": schema_name}
        except Exception as exc:
            warehouse_sync = {
                "status": "error",
                "schema": schema_name,
                "error": str(exc),
            }

    return {
        "status": "success",
        "target": target,
        "path": rel_path,
        "fqn": fqn_for(f"{rel_path}.parquet"),
        "sql_name": sql_name_for(f"{rel_path}.parquet"),
        "folder": folder,
        "warehouse_schema": schema_name,
        "warehouse_sync": warehouse_sync,
        "name": stem,
        "num_rows": num_rows,
        "file_path": address,
    }


def _sync_table_to_postgres(schema: str, table_name: str) -> None:
    """Publishes one lakehouse table into PostgreSQL through DuckDB.

    The source is the lakehouse table, so this needs no Parquet path and no copy of the
    data in Python: DuckDB reads from the catalog and writes to the attached database in
    one statement.
    """
    from app.core.catalog import SOURCE as _SOURCE
    from app.core.catalog import attach_lakehouse
    from app.core.dsn import sql_ident
    from app.core.warehouse_resolver import resolve_write_warehouse_postgres_params

    pg = resolve_write_warehouse_postgres_params()
    conn_str = build_pg_dsn(
        dbname=pg["database"],
        host=pg["host"],
        port=pg["port"],
        user=pg["username"],
        password=pg["password"],
        sslmode=pg.get("sslmode") or "disable",
    )

    import duckdb
    conn = duckdb.connect(":memory:")
    try:
        attach_lakehouse(conn)
        conn.execute("LOAD postgres;")
        alias = validate_alias("pg_sync_wh")
        conn.execute(f"ATTACH {sql_string(conn_str)} AS {alias} (TYPE POSTGRES);")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{sql_ident(schema)};")
        conn.execute(
            f"CREATE OR REPLACE TABLE {alias}.{sql_ident(schema)}.{sql_ident(table_name)} AS "
            f"SELECT * FROM {_SOURCE}.{quote_ident(schema)}.{quote_ident(table_name)};"
        )
        conn.execute(f"DETACH {alias};")
    finally:
        conn.close()


def is_java_compatible() -> bool:
    """Verifies that a valid Java 17+ runtime is available on the host."""
    import subprocess
    import re

    # Validate JAVA_HOME if set
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if not candidate.is_file():
            # Stale or invalid JAVA_HOME; remove to avoid batch script crash
            os.environ.pop("JAVA_HOME", None)

    try:
        res = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=2)
        if res.returncode == 0:
            out = res.stderr or res.stdout
            m = re.search(r'version "(?:1\.)?(\d+)', out)
            if m:
                major = int(m.group(1))
                return major >= 17
    except Exception:
        pass
    return False


def spark(
    get_or_create: bool = True,
    remote_url: Optional[str] = None,
    app_name: str = "ObeliskAnalytics",
):
    """
    Initializes a PySpark session.
    - If SPARK_REMOTE_URL (sc://...) is set, configures Spark Connect without requiring local Java.
    - Otherwise, configures local[*] with driver memory capped dynamically via ResourceAdvisor,
      disables UI for headless efficiency, and enables PyArrow acceleration.
    """
    from pyspark.sql import SparkSession

    remote = remote_url or settings.SPARK_REMOTE_URL
    if remote:
        builder = SparkSession.builder.remote(remote).appName(app_name)
        return builder.getOrCreate() if get_or_create else builder

    # Local mode: check Java 17+ runtime
    if get_or_create and not is_java_compatible():
        raise RuntimeError(
            "Java 17+ runtime is required for local PySpark. "
            "Please install OpenJDK 17+ or set SPARK_REMOTE_URL (e.g. sc://spark-host:15002) for Spark Connect."
        )

    mem_limit = advisor.get_spark_memory_limit()
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", mem_limit)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    if get_or_create:
        return builder.getOrCreate()
    return builder


def var(key: str, default: Any = None) -> Any:
    """
    Retrieves an Obelisk variable or secret by key.
    If the variable is an encrypted secret, it is automatically decrypted with the AES-128 Fernet vault.
    Usage:
        import obelisk
        env = obelisk.var("ENV", default="development")
        slack_token = obelisk.var("SLACK_WEBHOOK_URL")
    """
    clean_key = key.strip().upper()
    db_path = settings.sqlite_db_path
    try:
        with sqlite3.connect(db_path, timeout=5.0) as conn:
            cur = conn.cursor()
            cur.execute("SELECT value, is_secret FROM variables WHERE key = ?", (clean_key,))
            row = cur.fetchone()
            if not row:
                return default
            val, is_secret = row
            if is_secret:
                from app.core.vault import vault
                return vault.decrypt(val)
            return val
    except Exception:
        return default

