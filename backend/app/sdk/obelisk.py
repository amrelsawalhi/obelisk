"""
Obelisk SDK: Zero-config notebook and analytical client.
Usage:
    import obelisk
    df = obelisk.table("bronze.orders").to_polars()
    res = obelisk.query("SELECT * FROM read_parquet(...)")
    obelisk.save(df, "gold.monthly_revenue")
    spark = obelisk.spark()
"""

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
from app.core.resource_advisor import advisor
from app.core.dsn import build_pg_dsn, sql_string, validate_alias
from app.engines.duckdb_engine import DuckDBEngine

duckdb_engine = DuckDBEngine()


class TableWrapper:
    """Wrapper around lakehouse datasets and external tables supporting zero-copy format conversions."""

    def __init__(self, name: str, layer: str, file_path: Path, full_identifier: Optional[str] = None):
        self.name = name
        self.layer = layer
        self.file_path = file_path
        self.full_identifier = full_identifier or f"{layer}.{name}"

    def exists(self) -> bool:
        if self.file_path and self.file_path.exists():
            return True
        try:
            res = query(f"SELECT 1 FROM {self.full_identifier} LIMIT 1")
            return len(res) > 0
        except Exception:
            return False

    def to_arrow(self) -> pa.Table:
        """Reads Parquet dataset into a PyArrow Table, or queries external database table."""
        if self.file_path and self.file_path.exists():
            return pq.read_table(str(self.file_path))
        try:
            res = query_arrow(f"SELECT * FROM {self.full_identifier}")
            if hasattr(res, "read_all"):
                return res.read_all()
            return res
        except Exception as e:
            raise FileNotFoundError(
                f"Dataset '{self.full_identifier}' not found at {self.file_path} or attached external sources: {e}"
            ) from e

    def to_polars(self) -> pl.DataFrame:
        """Reads Parquet dataset into a Polars DataFrame, or queries external database table."""
        if self.file_path and self.file_path.exists():
            return pl.read_parquet(str(self.file_path))
        try:
            return query(f"SELECT * FROM {self.full_identifier}")
        except Exception as e:
            raise FileNotFoundError(
                f"Dataset '{self.full_identifier}' not found at {self.file_path} or attached external sources: {e}"
            ) from e

    def to_df(self) -> Any:
        """Reads dataset into a Pandas DataFrame."""
        arrow_obj = self.to_arrow()
        if hasattr(arrow_obj, "read_all"):
            arrow_obj = arrow_obj.read_all()
        return arrow_obj.to_pandas()

    def to_duckdb(self, conn=None):
        """Returns a DuckDB relation referencing the dataset."""
        if self.file_path and self.file_path.exists():
            c = conn or duckdb_engine.get_connection()
            posix_path = self.file_path.resolve().as_posix()
            return c.from_parquet(posix_path)
        c = conn or duckdb_engine.get_connection()
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
        return f"<ObeliskTable: {self.full_identifier} [{status}] -> {self.file_path}>"


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
    """
    Resolves a lakehouse dataset or external registered table by canonical FQN or identifier.
    Supports:
        3-tier Lakehouse: 'lakehouse.bronze.venues' -> reads Parquet from Zero-Cost STORAGE_URI
        3-tier External:  'supabase.public.venues_old' -> queries attached external database
        2-tier Lakehouse: 'bronze.venues' -> backwards-compatible Parquet resolution
    """
    clean_id = identifier.strip().replace("`", "").replace('"', "")
    parts = clean_id.split(".")
    lakehouse_root = settings.lakehouse_root_path

    # 1. Canonical 3-tier Lakehouse FQN: lakehouse.<folder>.<stem>
    if len(parts) >= 3 and parts[0].lower() == "lakehouse":
        folder = parts[1]
        stem = parts[2]
        full_id = f"lakehouse.{folder}.{stem}"
        file_path = lakehouse_root / folder / f"{stem}.parquet"
        return TableWrapper(name=stem, layer=folder, file_path=file_path, full_identifier=full_id)

    # 2. Canonical 3-tier External Source FQN: <catalog>.<schema>.<stem>
    if len(parts) >= 3:
        catalog, schema, stem = parts[0].lower(), parts[1], parts[2]
        full_id = f"{catalog}.{schema}.{stem}"
        return TableWrapper(name=stem, layer=schema, file_path=None, full_identifier=full_id)

    # 3. 2-tier Lakehouse or External fallback:
    if len(parts) == 2:
        prefix, stem = parts[0].lower(), parts[1]
        # Case A: lakehouse.<stem> (flat table directly in lakehouse root or in any subfolder)
        if prefix == "lakehouse":
            flat_candidate = lakehouse_root / f"{stem}.parquet"
            if flat_candidate.exists():
                return TableWrapper(name=stem, layer="main", file_path=flat_candidate, full_identifier=f"lakehouse.{stem}")
            if lakehouse_root.exists() and lakehouse_root.is_dir():
                for sub in lakehouse_root.iterdir():
                    if sub.is_dir() and (sub / f"{stem}.parquet").exists():
                        return TableWrapper(name=stem, layer=sub.name, file_path=sub / f"{stem}.parquet", full_identifier=f"lakehouse.{sub.name}.{stem}")

        # Case B: <folder>.<stem> in lakehouse
        candidate = lakehouse_root / prefix / f"{stem}.parquet"
        if candidate.exists():
            return TableWrapper(name=stem, layer=prefix, file_path=candidate, full_identifier=f"lakehouse.{prefix}.{stem}")

        # Case C: Check flat directly in lakehouse root
        flat_candidate = lakehouse_root / f"{stem}.parquet"
        if flat_candidate.exists():
            return TableWrapper(name=stem, layer="main", file_path=flat_candidate, full_identifier=f"lakehouse.{stem}")

        # If not found on disk, resolve external identifier
        resolved = _resolve_external_identifier(clean_id)
        return TableWrapper(name=stem, layer=prefix, file_path=None, full_identifier=resolved)

    # 4. 1-tier bare table name (e.g. 'orders', 'venues'):
    stem = clean_id
    # A. Check flat directly in lakehouse root:
    flat_candidate = lakehouse_root / f"{stem}.parquet"
    if flat_candidate.exists():
        return TableWrapper(name=stem, layer="main", file_path=flat_candidate, full_identifier=f"lakehouse.{stem}")

    # B. Dynamically search any subfolder in lakehouse_root:
    if lakehouse_root.exists() and lakehouse_root.is_dir():
        for sub in lakehouse_root.iterdir():
            if sub.is_dir():
                sub_candidate = sub / f"{stem}.parquet"
                if sub_candidate.exists():
                    return TableWrapper(name=stem, layer=sub.name, file_path=sub_candidate, full_identifier=f"lakehouse.{sub.name}.{stem}")

    # C. External database fallback
    resolved = _resolve_external_identifier(clean_id)
    return TableWrapper(name=stem, layer="main", file_path=None, full_identifier=resolved)


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


def save(df: Any, target: str, sync_postgres: bool = True) -> Dict[str, Any]:
    """
    Persists a Polars, Pandas, Arrow, or Spark DataFrame to the Lakehouse.
    Target format: '<layer>.<name>' (e.g., 'gold.monthly_revenue' or 'silver.cleaned_users').
    Automatically registers asset metadata in SQLite brain and syncs to PostgreSQL if gold/silver.
    """
    if "." in target:
        layer, stem = target.split(".", 1)
        layer = layer.lower()
    else:
        layer, stem = "main", target

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

    # 2. Write to Parquet atomically inside data/lakehouse/{layer} or flat in root
    storage_uri_clean = settings.STORAGE_URI.rstrip("/")
    if layer != "main":
        storage_dir = settings.lakehouse_root_path / layer
        asset_storage_uri = f"{storage_uri_clean}/{layer}/{stem}.parquet"
    else:
        storage_dir = settings.lakehouse_root_path
        asset_storage_uri = f"{storage_uri_clean}/{stem}.parquet"

    storage_dir.mkdir(parents=True, exist_ok=True)
    target_file = storage_dir / f"{stem}.parquet"
    temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")

    pq.write_table(arrow_table, str(temp_file), compression="SNAPPY")
    os.replace(temp_file, target_file)

    num_rows = arrow_table.num_rows
    columns = [
        {"name": col_name, "type": str(arrow_table.schema.field(col_name).type)}
        for col_name in arrow_table.column_names
    ]
    schema_meta = {"num_rows": num_rows, "columns": columns}

    # 3. Register in SQLite assets table
    db_path = settings.sqlite_db_path
    now_iso = datetime.now(timezone.utc).isoformat()
    asset_id = str(uuid.uuid4())

    try:
        with sqlite3.connect(db_path, timeout=10.0) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute(
                """
                INSERT INTO assets (id, name, layer, storage_uri, format, record_count, schema_metadata, created_at, last_materialized_at)
                VALUES (?, ?, ?, ?, 'parquet', ?, ?, ?, ?)
                ON CONFLICT(layer, name) DO UPDATE SET
                    storage_uri=excluded.storage_uri,
                    record_count=excluded.record_count,
                    schema_metadata=excluded.schema_metadata,
                    last_materialized_at=excluded.last_materialized_at;
                """,
                (asset_id, stem, layer, asset_storage_uri, num_rows, json.dumps(schema_meta), now_iso, now_iso),
            )
            conn.commit()
    except Exception:
        pass

    # 4. Sync to PostgreSQL warehouse if Silver/Gold and warehouse configured
    if sync_postgres and layer in ("silver", "gold"):
        _sync_table_to_postgres(target_file, schema=layer, table_name=stem)

    return {
        "status": "success",
        "target": target,
        "layer": layer,
        "name": stem,
        "num_rows": num_rows,
        "file_path": target_file.as_posix(),
    }


def _sync_table_to_postgres(parquet_file: Path, schema: str, table_name: str) -> None:
    """Syncs Parquet file into PostgreSQL warehouse using DuckDB native COPY."""
    # Resolve the warehouse the same way Query Studio, dbt and pipelines do.
    from app.core.warehouse_resolver import resolve_warehouse_postgres_params
    pg = resolve_warehouse_postgres_params()
    pg_host = pg["host"]
    pg_port = pg["port"]
    pg_user = pg["username"]
    pg_pass = pg["password"]
    pg_db = pg["database"]
    pg_sslmode = pg.get("sslmode") or "disable"

    import duckdb
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("LOAD postgres;")
        conn_str = build_pg_dsn(
            dbname=pg_db,
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_pass,
            sslmode=pg_sslmode,
        )
        alias = "pg_sync_wh"
        conn.execute(f"ATTACH {sql_string(conn_str)} AS {validate_alias(alias)} (TYPE POSTGRES);")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{schema};")
        posix_src = parquet_file.resolve().as_posix()
        conn.execute(f"CREATE OR REPLACE TABLE {alias}.{schema}.\"{table_name}\" AS SELECT * FROM read_parquet('{posix_src}');")
        conn.execute(f"DETACH {alias};")
    except Exception:
        pass
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
    If the variable is an encrypted secret, it is automatically decrypted with the AES-256 Fernet vault.
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

