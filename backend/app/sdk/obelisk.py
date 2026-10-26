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
from app.core.storage import warehouse_storage
from app.engines.duckdb_engine import DuckDBEngine

duckdb_engine = DuckDBEngine()
logger = logging.getLogger(__name__)

__all__ = [
    "table",
    "query",
    "query_arrow",
    "save",
    "materialize_sql",
    "spark",
    "var",
    "TableWrapper",
]

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
        for attachment in _attachments_for_sql(self.full_identifier):
            duckdb_engine.attach(c, attachment)
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


class SourceNotQueryable(RuntimeError):
    """Raised when an identifier names a source that cannot be queried live.

    Only an *attached* source (PostgreSQL, through DuckDB's own scanner) can be read as
    SQL. MySQL, MongoDB, Oracle and HTTP sources are pulled: their rows reach DuckDB only
    after an ingest copies them into the lakehouse. Saying that here beats letting DuckDB
    answer with ``schema "..." does not exist``.
    """


def _registered_connection(segment: str) -> Optional[tuple[str, str, str]]:
    """``(name, source_type, driver)`` for a connection whose name matches ``segment``."""
    db_path = settings.sqlite_db_path
    if not Path(db_path).exists():
        return None
    try:
        with sqlite3.connect(db_path, timeout=5.0) as con:
            row = con.execute(
                "SELECT name, source_type, driver FROM connections WHERE lower(name) = ?",
                (segment.strip().lower(),),
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return row[0], (row[1] or "").lower(), (row[2] or "").lower()


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

    # 2. The identifier may name a source, which is a different question than a lakehouse
    #    table: ``table("mongo_x.db.coll")`` says "read this source", and if that source has
    #    no DuckDB scanner the answer is no. Checked before the bare-name shortcut, so a
    #    unique lakehouse table with the same leaf can never be substituted for it.
    parts = clean_id.split(".")
    connection = _registered_connection(parts[0]) if len(parts) >= 2 else None
    if connection:
        conn_name, source_type, driver = connection
        from app.core.sources import ATTACH, UnsupportedSource, infer_driver

        try:
            resolved_driver = driver or infer_driver(source_type)
        except UnsupportedSource:
            resolved_driver = ""
        if resolved_driver != ATTACH:
            same_name = next(
                (entry for entry in result.tables if entry.name.lower() == parts[-1].lower()),
                None,
            )
            hint = (
                f" It is already in the lakehouse as {same_name.fqn} — read that instead."
                if same_name
                else f" Ingest '{parts[-1]}' first (Data Catalog → Ingest), then read the "
                "lakehouse table it creates."
            )
            raise SourceNotQueryable(
                f"'{clean_id}' names the {source_type or 'external'} source {conn_name!r}, which "
                f"cannot be queried live: DuckDB has no scanner for it (Postgres, MySQL and "
                f"Mongo do; Oracle and SQL Server need an ODBC DSN).{hint}"
            )
        # Naming an attached source means *that* table, even if a lakehouse table happens to
        # share the leaf name.
        return TableWrapper(
            name=parts[-1],
            folder=".".join(parts[:-2]) if len(parts) >= 3 else "main",
            file_path=None,
            full_identifier=_resolve_external_identifier(clean_id),
        )

    # 3. A bare name, or a folder/name whose folder was spelled with dots, is still
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

    # 4. Not a warehouse table and not a known source: let the Postgres resolver try.
    resolved = _resolve_external_identifier(clean_id)
    return TableWrapper(
        name=parts[-1],
        folder=".".join(parts[:-2]) if len(parts) >= 3 else "main",
        file_path=None,
        full_identifier=resolved,
    )


def _attachments_for_sql(sql: str) -> list:
    """Sources the SQL names that DuckDB can attach live, credentials decrypted.

    The vault is read directly because the SDK is synchronous. The *shape* of each dict
    comes from :func:`app.core.sources.attachment_for`, so a notebook, the SQL editor and
    the orchestrator all attach a source the same way — and a source with no scanner never
    appears here.
    """
    from types import SimpleNamespace

    from app.core.sources import UnsupportedSource, attachment_for, resolve_source
    from app.core.vault import decrypt
    from app.services.connections import is_connection_referenced

    attachments: list = []
    try:
        db_path = settings.sqlite_db_path
        if not Path(db_path).exists():
            return attachments
        with sqlite3.connect(db_path, timeout=5.0) as con:
            rows = con.execute(
                "SELECT name, source_type, driver, host, port, database, username, "
                "encrypted_credentials, options, read_only FROM connections;"
            ).fetchall()
    except Exception:
        return attachments

    for row in rows:
        (name, source_type, driver, host, port, database, username,
         enc_pw, options_json, read_only) = row
        connection = SimpleNamespace(
            name=name,
            source_type=source_type,
            driver=driver,
            host=host,
            port=port,
            database=database,
            username=username,
            options=json.loads(options_json) if options_json else {},
            read_only=None if read_only is None else bool(read_only),
        )
        try:
            recipe = resolve_source(connection)
        except UnsupportedSource:
            continue
        if not recipe.is_attachable:
            continue
        if not is_connection_referenced(sql, recipe.alias, database):
            continue
        attachments.append(
            attachment_for(recipe, connection, password=decrypt(enc_pw))
        )
    return attachments


def query(sql: str) -> pl.DataFrame:
    """Executes SQL query using DuckDB in-process engine and returns a Polars DataFrame."""
    conn = duckdb_engine.get_connection()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
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
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
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


def _coerce_to_arrow(df: Any) -> pa.Table:
    """Normalizes a Polars / Arrow / pandas / Spark DataFrame to a PyArrow Table."""
    if isinstance(df, pl.DataFrame):
        return df.to_arrow()
    if isinstance(df, pa.Table):
        return df
    if hasattr(df, "to_arrow"):
        return df.to_arrow()
    if hasattr(df, "toPandas"):  # PySpark DataFrame
        return pa.Table.from_pandas(df.toPandas())
    if hasattr(df, "to_pandas"):
        return pa.Table.from_pandas(df.to_pandas())
    if str(type(df)).find("pandas.core.frame.DataFrame") != -1:
        return pa.Table.from_pandas(df)
    raise TypeError(f"Unsupported DataFrame type '{type(df)}' for obelisk.save().")


def save(df: Any, target: str) -> Dict[str, Any]:
    """Persists a DataFrame as a table in the DuckLake lakehouse.

    ``target`` is a warehouse path — ``"supabase/auth/table1"``, the fully qualified
    ``"lakehouse.supabase.auth.table1"``, or a bare ``"orders"`` for the root. Dots and
    slashes are both separators, so a layer-style ``"marts.sales.orders"`` still works
    and lands in schema ``marts.sales``.

    DuckLake is the single source of truth: the table is created through its catalog and
    nothing is mirrored into PostgreSQL. The row count, schema and size follow from the
    catalog without anything being registered by hand.
    """
    folder, stem = split_table_path(target)
    schema_name = folder.replace("/", ".") if folder else "main"
    rel_path = f"{folder}/{stem}" if folder else stem

    arrow_table = _coerce_to_arrow(df)

    from app.core.catalog import quote_ident

    address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"
    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
        conn.register("_obelisk_save_src", arrow_table)
        try:
            conn.execute(
                f"CREATE OR REPLACE TABLE {address} AS SELECT * FROM _obelisk_save_src;"
            )
        finally:
            try:
                conn.unregister("_obelisk_save_src")
            except Exception:
                pass
    finally:
        conn.close()

    num_rows = int(arrow_table.num_rows)
    fqn = fqn_for(f"{rel_path}.parquet")
    sql_name = sql_name_for(f"{rel_path}.parquet")

    logger.info(
        "obelisk.save: wrote %s rows -> %s (fqn=%s)",
        f"{num_rows:,}",
        address,
        fqn,
    )

    return {
        "status": "success",
        "target": target,
        "path": rel_path,
        "fqn": fqn,
        "sql_name": sql_name,
        "folder": folder,
        "name": stem,
        "num_rows": num_rows,
        "file_path": address,
    }


def materialize_sql(sql: str, target: str) -> Dict[str, Any]:
    """Materializes a SQL query directly into a DuckLake table.

    DuckDB reads the sources named by ``sql`` and writes the result through the catalog in
    one statement, so a multi-GB result never becomes a Python / Arrow object first. Use
    this in place of ``save(query_arrow(sql), target)`` when the result is large.
    """
    from app.core.catalog import quote_ident

    folder, stem = split_table_path(target)
    schema_name = folder.replace("/", ".") if folder else "main"
    rel_path = f"{folder}/{stem}" if folder else stem
    address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"

    conn = duckdb_engine.get_connection()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
        exec_sql = (
            duckdb_engine._prepare_sql_and_views(conn, sql, storage_path).strip().rstrip(";")
        )
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
        conn.execute(f"CREATE OR REPLACE TABLE {address} AS {exec_sql};")
        num_rows = int(conn.execute(f"SELECT count(*) FROM {address};").fetchone()[0])
    finally:
        conn.close()

    fqn = fqn_for(f"{rel_path}.parquet")
    sql_name = sql_name_for(f"{rel_path}.parquet")

    logger.info(
        "obelisk.materialize_sql: wrote %s rows -> %s (fqn=%s)",
        f"{num_rows:,}",
        address,
        fqn,
    )

    return {
        "status": "success",
        "target": target,
        "path": rel_path,
        "fqn": fqn,
        "sql_name": sql_name,
        "folder": folder,
        "name": stem,
        "num_rows": num_rows,
        "file_path": address,
    }


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

