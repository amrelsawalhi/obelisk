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
from app.core.catalog import SOURCE, WarehouseScan, quote_ident, scan
from app.core.resource_advisor import advisor
from app.core.storage import warehouse_storage

logger = logging.getLogger(__name__)

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
        """Returns the resolved lakehouse root.

        Path resolution (and the remote-scheme rejection) lives in Settings so
        every caller agrees on a single answer; this method no longer re-derives
        it from the raw URI (audit B1).
        """
        return settings.duckdb_storage_path

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
        from app.core.dsn import build_pg_dsn, sql_string, validate_alias

        host = resolve_connection_host(host, port)
        dsn = build_pg_dsn(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            sslmode=sslmode,
            connect_timeout=connect_timeout,
        )
        safe_alias = validate_alias(alias)
        if schema and schema.lower() != "all":
            conn.execute(
                f"ATTACH {sql_string(dsn)} AS {safe_alias} "
                f"(TYPE POSTGRES, SCHEMA {sql_string(schema)});"
            )
        else:
            conn.execute(f"ATTACH {sql_string(dsn)} AS {safe_alias} (TYPE POSTGRES);")

    def _warehouse_scan(self) -> WarehouseScan:
        """The tables that exist, read from the warehouse itself.

        No cache here: the walk is a directory listing, and the expensive part — reading
        each footer — is cached in :func:`app.core.catalog.read_footer` against the
        file's size and timestamp. A table written a moment ago is therefore visible to
        the very next query, which a time-based cache could not promise.
        """
        return scan(warehouse_storage())

    @staticmethod
    def _schema_for(folder: str) -> str:
        """The DuckDB schema a folder maps to, quoted only where it must be.

        SQL identifiers carry three parts, so a nested path becomes one schema whose
        name contains dots — `supabase/auth` -> `"supabase.auth"`. Root-level files
        live in the attached catalog's default schema.
        """
        if not folder:
            return "main"
        return quote_ident(folder.replace("/", "."))

    def _register_view(self, conn: duckdb.DuckDBPyConnection, table, location: str) -> None:
        """Creates the canonical view for one table, plus a bare alias when unambiguous.

        The canonical name is the three-slot address — `lakehouse.supabase.table1`, or
        `lakehouse."supabase.auth".table1` when the folder path is deeper than one
        level. The bare alias exists because `SELECT * FROM orders` is what people
        type; it is only registered while that name is unique in the warehouse.
        """
        schema = self._schema_for(table.folder)
        table_name = quote_ident(table.name)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{schema};")
        conn.execute(
            f"CREATE OR REPLACE VIEW {SOURCE}.{schema}.{table_name} AS "
            f"SELECT * FROM read_parquet('{location}');"
        )
        if table.bare_alias:
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{location}');"
            )

    @staticmethod
    def _rewrite_names(sql: str, mapping: Dict[str, str]) -> str:
        """Rewrites the dotted address someone typed into the form DuckDB parses.

        `lakehouse.supabase.auth.table1` is four parts, and the parser stops at three.
        Only names the warehouse actually has are rewritten, in code regions only —
        string literals, quoted identifiers and comments are copied untouched — so a
        native three-part name, a Postgres schema or a filename is never disturbed.
        """
        if not mapping:
            return sql

        ordered = sorted(mapping, key=len, reverse=True)
        pattern = re.compile(
            r"(?<![A-Za-z0-9_.])(" + "|".join(re.escape(name) for name in ordered) + r")(?![A-Za-z0-9_])",
            re.IGNORECASE,
        )

        def replace(match: re.Match) -> str:
            return mapping.get(match.group(1).lower(), match.group(1))

        out: List[str] = []
        index = 0
        length = len(sql)
        while index < length:
            char = sql[index]
            if char in ("'", '"'):
                end = index + 1
                while end < length:
                    if sql[end] == char:
                        if end + 1 < length and sql[end + 1] == char:
                            end += 2
                            continue
                        end += 1
                        break
                    end += 1
                out.append(sql[index:end])
                index = end
            elif sql.startswith("--", index):
                end = sql.find("\n", index)
                end = length if end == -1 else end
                out.append(sql[index:end])
                index = end
            elif sql.startswith("/*", index):
                end = sql.find("*/", index + 2)
                end = length if end == -1 else end + 2
                out.append(sql[index:end])
                index = end
            else:
                end = index
                while end < length and sql[end] not in ("'", '"') and not sql.startswith(("--", "/*"), end):
                    end += 1
                out.append(pattern.sub(replace, sql[index:end]))
                index = end
        return "".join(out)

    def _prepare_sql_and_views(self, conn: duckdb.DuckDBPyConnection, sql: str, storage_path: str) -> str:
        """Makes the warehouse queryable: views from the scan, then name resolution.

        This engine keeps no catalog of its own. It asks :func:`app.core.catalog.scan`
        what exists and creates a view per file, so a table is queryable exactly when
        its Parquet file is there — the same answer the catalog tree shows.
        """
        store = warehouse_storage()
        result = self._warehouse_scan()
        tokens = set(re.findall(r"[a-zA-Z0-9_]+", sql.lower()))

        try:
            conn.execute(f"ATTACH ':memory:' AS {SOURCE};")
        except Exception as exc:
            logger.debug("Failed to attach in-memory %s catalog: %s", SOURCE, exc)

        # Rewriting is independent of what this query mentions: the map is the
        # warehouse's own address list, so it costs one dictionary pass.
        names: Dict[str, str] = {}
        for table in result.tables:
            if table.fqn.lower() != table.sql_name.lower():
                names[table.fqn.lower()] = table.sql_name
            # The layer shorthand -- ``bronze.orders`` -- is what SQL steps and the docs
            # use, while ``sql_name`` is the canonical address DuckDB parses. Mapping it
            # here is what makes the shorthand resolve instead of depending on a bare
            # alias that only exists when the file name happens to be unique.
            shorthand = f"{table.folder.strip('/').replace('/', '.')}.{table.name}".lower()
            if table.folder.strip("/"):
                names.setdefault(shorthand, table.sql_name)

        for table in result.tables:
            segments = [segment.lower() for segment in table.folder.split("/") if segment]
            if not (table.name.lower() in tokens or any(part in tokens for part in segments)):
                continue
            try:
                self._register_view(conn, table, store.location(table.path))
            except Exception as exc:
                logger.debug("Could not register a view for %s: %s", table.path, exc)

        return self._normalize_parquet_paths(self._rewrite_names(sql, names))

    @staticmethod
    def _normalize_parquet_paths(sql: str) -> str:
        """Turns a relative ``read_parquet('...')`` argument into an absolute path."""
        lakehouse_root_posix = settings.lakehouse_root_path.resolve().as_posix()

        def _replace_parquet_path(m: re.Match) -> str:
            path_str = m.group(1).strip()
            if (
                path_str.startswith(("http://", "https://", "s3://", "gs://", "abfss://"))
                or Path(path_str).is_absolute()
                or re.match(r"^[A-Za-z]:[/\\]", path_str)
            ):
                return f"read_parquet('{path_str}')"
            clean_subpath = re.sub(
                r"^(?:/?data/lakehouse/|lakehouse/)", "", path_str, flags=re.IGNORECASE
            )
            return f"read_parquet('{lakehouse_root_posix}/{clean_subpath}')"

        return re.sub(
            r"read_parquet\(\s*['\"]([^'\"]+\.parquet)['\"]\s*\)",
            _replace_parquet_path,
            sql,
            flags=re.IGNORECASE,
        )

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
