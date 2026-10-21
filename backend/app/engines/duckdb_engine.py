import os
import re
import time
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import duckdb
import polars as pl
from app.config import settings
from app.core.resource_advisor import advisor

logger = logging.getLogger(__name__)

# TTL cache for recursive lakehouse Parquet scans: avoids re-scanning the entire
# lakehouse directory tree (rglob) on every single SQL query.
_PARQUET_FILE_CACHE: Dict[str, Tuple[float, List[Path]]] = {}
_CACHE_TTL_SECS = 5.0

class DuckDBEngine:
    """
    In-process DuckDB analytical engine with:
    - Zero-cost storage URI integration (reads from settings.duckdb_storage_path)
    - Dynamic memory sizing via ResourceAdvisor
    - Dynamic scanner attaching for PostgreSQL (ATTACH ... TYPE POSTGRES)
    - Non-blocking query execution & Parquet/CSV streaming exports
    """

    def __init__(self):
        self._ensure_storage()

    def _ensure_storage(self) -> str:
        storage_path = settings.duckdb_storage_path
        # Guard local directory creation against remote cloud URIs
        if not settings.STORAGE_URI.startswith(("s3://", "gs://", "abfss://")):
            Path(storage_path).mkdir(parents=True, exist_ok=True)
        return storage_path

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Creates an in-memory DuckDB connection, sets memory limit from ResourceAdvisor,
        and registers the storage_uri setting.
        """
        conn = duckdb.connect(":memory:")
        mem_limit = advisor.get_duckdb_memory_limit()
        try:
            conn.execute(f"SET memory_limit = '{mem_limit}';")
        except Exception as e:
            logger.debug("DuckDB memory_limit setting failed: %s", e)

        # Set duckdb storage directory setting for convenience
        storage_path = self._ensure_storage()
        try:
            conn.execute(f"SET storage_directory = '{storage_path}';")
        except Exception as e:
            logger.debug("DuckDB storage_directory setting failed: %s", e)

        # Enforce secure HTTPS for DuckDB extension downloads (prevents ISP HTTP 307 interception)
        try:
            conn.execute("SET custom_extension_repository = 'https://extensions.duckdb.org';")
        except Exception as e:
            logger.debug("DuckDB custom_extension_repository setting failed: %s", e)

        # Load httpfs extension for remote HTTPS and S3 queries
        try:
            conn.execute("LOAD httpfs;")
        except Exception as e:
            logger.debug("DuckDB httpfs extension load failed: %s", e)

        return conn

    def attach_postgres(
        self,
        conn: duckdb.DuckDBPyConnection,
        alias: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        schema: Optional[str] = None,
        sslmode: str = "disable",
        connect_timeout: int = 8,
    ) -> None:
        """
        Dynamically attaches a remote or local PostgreSQL database to the DuckDB connection.
        Enables cross-database federated SQL joins without blocking on WAN view loops.
        """
        try:
            conn.execute("INSTALL postgres; LOAD postgres;")
        except Exception as e:
            logger.debug("DuckDB postgres extension load failed: %s", e)

        from app.core.host_resolver import resolve_connection_host
        host = resolve_connection_host(host, port)
        dsn = f"host={host} port={port} dbname={database} user={username} password={password} sslmode={sslmode} connect_timeout={connect_timeout}"
        if schema and schema.lower() != "all":
            conn.execute(f"ATTACH '{dsn}' AS {alias} (TYPE POSTGRES, SCHEMA '{schema}');")
        else:
            conn.execute(f"ATTACH '{dsn}' AS {alias} (TYPE POSTGRES);")

    def _get_lakehouse_files(self, lakehouse_root: Path) -> List[Path]:
        """Returns all visible Parquet files under lakehouse_root, TTL-cached for 5 seconds."""
        now = time.time()
        cache_key = str(lakehouse_root)
        cached = _PARQUET_FILE_CACHE.get(cache_key)
        if cached is not None and now - cached[0] < _CACHE_TTL_SECS:
            return cached[1]

        found = [
            p for p in lakehouse_root.rglob("*.parquet")
            if not any(part.startswith(".") for part in p.relative_to(lakehouse_root).parts)
        ]
        _PARQUET_FILE_CACHE[cache_key] = (now, found)
        return found

    def _prepare_sql_and_views(self, conn: duckdb.DuckDBPyConnection, sql: str, storage_path: str) -> str:
        """Registers schemas and views on-demand for Lakehouse Parquet directories referenced in the query."""
        lakehouse_root = settings.lakehouse_root_path
        clean_sql = re.sub(r"--[^\n]*", "", sql)
        clean_sql = re.sub(r"/\*.*?\*/", "", clean_sql, flags=re.DOTALL)
        tokens = set(re.findall(r"[a-zA-Z0-9_]+", clean_sql.lower()))

        # Unconditionally attach in-memory 'lakehouse' catalog for 3-tier FQN support
        try:
            conn.execute("ATTACH ':memory:' AS lakehouse;")
        except Exception as e:
            logger.debug("Failed to attach in-memory lakehouse catalog: %s", e)

        if lakehouse_root.exists() and lakehouse_root.is_dir():
            matched_files = []
            if tokens:
                # Fast direct probe: flat in lakehouse root or inside any existing subfolder
                for t in tokens:
                    flat_cand = lakehouse_root / f"{t}.parquet"
                    if flat_cand.is_file():
                        matched_files.append(flat_cand)
                    for sub in lakehouse_root.iterdir():
                        if sub.is_dir():
                            sub_cand = sub / f"{t}.parquet"
                            if sub_cand.is_file():
                                matched_files.append(sub_cand)

            # If no direct candidates found or tokens empty, scan directory (TTL-cached
            # to avoid a recursive disk traversal on every single query)
            if not matched_files:
                for parquet_file in self._get_lakehouse_files(lakehouse_root):
                    if not tokens or parquet_file.stem.lower() in tokens:
                        matched_files.append(parquet_file)

            for parquet_file in matched_files:
                rel_parts = parquet_file.relative_to(lakehouse_root).parts
                table_name = parquet_file.stem
                escaped_path = parquet_file.as_posix()

                if len(rel_parts) > 1:
                    rel_dir = "/".join(rel_parts[:-1])
                    clean_tier = rel_dir.replace("/", ".")
                    tier_name = rel_parts[0]

                    # 1. 3-tier canonical catalog view: lakehouse.<folder>.<table_name>
                    try:
                        conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{clean_tier};")
                        conn.execute(
                            f"CREATE OR REPLACE VIEW lakehouse.{clean_tier}.{table_name} AS SELECT * FROM read_parquet('{escaped_path}');"
                        )
                    except Exception as e:
                        logger.debug("Failed to create 3-tier view for %s: %s", parquet_file.name, e)

                    # 2. 2-tier folder view: <folder>.<table_name>
                    try:
                        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {tier_name};")
                        conn.execute(
                            f"CREATE OR REPLACE VIEW {tier_name}.{table_name} AS SELECT * FROM read_parquet('{escaped_path}');"
                        )
                    except Exception as e:
                        logger.debug("Failed to create 2-tier view for %s: %s", parquet_file.name, e)
                else:
                    # Flat file in lakehouse root: lakehouse.<table_name>
                    try:
                        conn.execute(
                            f"CREATE OR REPLACE VIEW lakehouse.{table_name} AS SELECT * FROM read_parquet('{escaped_path}');"
                        )
                    except Exception as e:
                        logger.debug("Failed to create flat view for %s: %s", parquet_file.name, e)

                # 3. 1-tier bare table view: <table_name>
                try:
                    conn.execute(
                        f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{escaped_path}');"
                    )
                except Exception as e:
                    logger.debug("Failed to create bare view for %s: %s", parquet_file.name, e)

        # Normalize relative parquet paths (e.g. data/lakehouse/silver/customers.parquet -> absolute path)
        lakehouse_root_posix = lakehouse_root.resolve().as_posix()

        def _replace_parquet_path(m: re.Match) -> str:
            path_str = m.group(1).strip()
            if (
                path_str.startswith(("http://", "https://", "s3://", "gs://", "abfss://"))
                or Path(path_str).is_absolute()
                or re.match(r"^[A-Za-z]:[/\\]", path_str)
            ):
                return f"read_parquet('{path_str}')"
            # Strip relative lakehouse prefix if provided
            clean_subpath = re.sub(r"^(?:/?data/lakehouse/|lakehouse/)", "", path_str, flags=re.IGNORECASE)
            return f"read_parquet('{lakehouse_root_posix}/{clean_subpath}')"

        normalized_sql = re.sub(
            r"read_parquet\(\s*['\"]([^'\"]+\.parquet)['\"]\s*\)",
            _replace_parquet_path,
            sql,
            flags=re.IGNORECASE,
        )
        return normalized_sql

    def execute_query(
        self,
        sql: str,
        postgres_attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Executes a SQL query against DuckDB in a thread-safe synchronous runner.
        Can resolve tables in Bronze Parquet directly via read_parquet() or table name.
        """
        start_time = time.time()
        storage_path = self._ensure_storage()
        conn = self.get_connection()

        try:
            # Attach any requested PostgreSQL sources
            if postgres_attachments:
                for att in postgres_attachments:
                    self.attach_postgres(
                        conn=conn,
                        alias=att["alias"],
                        host=att["host"],
                        port=att["port"],
                        database=att["database"],
                        username=att["username"],
                        password=att["password"],
                        schema=att.get("schema"),
                        sslmode=att.get("sslmode", "disable"),
                        connect_timeout=att.get("connect_timeout", 8),
                    )

            # Register bronze parquet views and normalize relative paths
            exec_sql = self._prepare_sql_and_views(conn, sql, storage_path)

            # Execute the user's query
            rel = conn.sql(exec_sql)
            if rel is None:
                duration_ms = int((time.time() - start_time) * 1000)
                return {
                    "columns": [],
                    "rows": [],
                    "row_count": 0,
                    "duration_ms": duration_ms,
                }

            arrow_reader = rel.arrow()
            arrow_table = arrow_reader.read_all() if hasattr(arrow_reader, "read_all") else arrow_reader
            columns = arrow_table.column_names
            # Convert arrow to list of dicts for JSON serialization
            rows = arrow_table.to_pylist()
            row_count = len(rows)
            duration_ms = int((time.time() - start_time) * 1000)

            return {
                "columns": columns,
                "rows": rows,
                "row_count": row_count,
                "duration_ms": duration_ms,
            }
        finally:
            conn.close()

    def export_query(
        self,
        sql: str,
        output_format: str = "parquet",
        postgres_attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> bytes:
        """Executes a query and returns the serialized result as Parquet or CSV bytes."""
        storage_path = self._ensure_storage()
        conn = self.get_connection()
        try:
            if postgres_attachments:
                for att in postgres_attachments:
                    self.attach_postgres(
                        conn=conn,
                        alias=att["alias"],
                        host=att["host"],
                        port=att["port"],
                        database=att["database"],
                        username=att["username"],
                        password=att["password"],
                        schema=att.get("schema"),
                        sslmode=att.get("sslmode", "disable"),
                        connect_timeout=att.get("connect_timeout", 8),
                    )

            # Register bronze parquet views and normalize relative paths
            exec_sql = self._prepare_sql_and_views(conn, sql, storage_path)

            rel = conn.sql(exec_sql)
            arrow_reader = rel.arrow()
            arrow_table = arrow_reader.read_all() if hasattr(arrow_reader, "read_all") else arrow_reader

            if output_format.lower() == "parquet":
                import pyarrow.parquet as pq
                import io
                buf = io.BytesIO()
                pq.write_table(arrow_table, buf, compression="snappy")
                return buf.getvalue()
            elif output_format.lower() == "csv":
                import io
                import pyarrow.csv as pacsv
                buf = io.BytesIO()
                pacsv.write_csv(arrow_table, buf)
                return buf.getvalue()
            else:
                raise ValueError(f"Unsupported export format: {output_format}")
        finally:
            conn.close()

    def query_bronze_to_polars(self, table_name: str) -> pl.DataFrame:
        """Reads a Bronze Parquet file into a Polars DataFrame."""
        storage_path = self._ensure_storage()
        file_path = Path(storage_path) / f"{table_name}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"Bronze table file {file_path.as_posix()} does not exist.")
        return pl.read_parquet(file_path.as_posix())

duckdb_engine = DuckDBEngine()
