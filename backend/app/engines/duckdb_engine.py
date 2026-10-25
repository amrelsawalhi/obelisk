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
from app.core.catalog import (
    RESERVED,
    SOURCE,
    attach_lakehouse,
    lakehouse_tables,
    quote_ident,
)
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

    def get_connection(self, *, with_lakehouse: bool = True) -> duckdb.DuckDBPyConnection:
        """An in-memory DuckDB connection with this process's budget applied.

        Every connection in the application comes from here (Rule 5): the memory limit
        from ``ResourceAdvisor``, a local temp directory so a large query spills instead
        of dying, a cap on that spill, and a thread ceiling so concurrent queries do not
        each claim every core.

        The lakehouse catalog is attached by default, because a connection without it can
        neither read nor write a table.
        """
        conn = duckdb.connect(":memory:")
        self._apply_session_budget(conn)
        try:
            conn.execute("LOAD httpfs;")
        except Exception as e:
            logger.debug("DuckDB httpfs extension load failed: %s", e)
        if with_lakehouse:
            self.attach_lakehouse(conn)
        return conn

    def _apply_session_budget(self, conn: duckdb.DuckDBPyConnection) -> None:
        """One place that decides how much of the machine a connection may use."""
        statements = [
            f"SET memory_limit = '{advisor.get_duckdb_memory_limit()}';",
            f"SET temp_directory = '{settings.duckdb_temp_path.as_posix()}';",
            f"SET max_temp_directory_size = '{settings.DUCKDB_TEMP_SIZE_LIMIT}';",
            "SET preserve_insertion_order = false;",
            f"SET storage_directory = '{self._ensure_storage()}';",
        ]
        if settings.DUCKDB_THREADS:
            statements.append(f"SET threads = {int(settings.DUCKDB_THREADS)};")
        for statement in statements:
            try:
                conn.execute(statement)
            except Exception as exc:
                logger.debug("DuckDB setting failed (%s): %s", statement, exc)

    def attach_lakehouse(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Attaches the DuckLake catalog under the name ``lakehouse``.

        The address people already type — ``lakehouse.raw.orders`` — is unchanged; only
        who owns the metadata is. See :func:`app.core.catalog.attach_lakehouse`.
        """
        attach_lakehouse(conn)

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

    def _lakehouse_tables(self, conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
        """``(schema, table)`` pairs the catalog knows about."""
        try:
            rows = lakehouse_tables(conn)
        except Exception as exc:
            logger.debug("Could not read DuckLake metadata: %s", exc)
            return []
        return [(row["schema"], row["table"]) for row in rows]

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

    def _prepare_sql_and_views(
        self, conn: duckdb.DuckDBPyConnection, sql: str, storage_path: str
    ) -> str:
        """Makes warehouse names resolvable, without walking the filesystem.

        The tables already exist — the catalog is attached — so the per-query scan and
        the ``CREATE VIEW`` per file are gone. Two conveniences remain:

        * the folder shorthand (``raw.orders``) is rewritten to the address DuckDB
          parses, because that is what SQL steps and the docs use;
        * a bare name (``orders``) becomes a temporary view while it is unique in the
          warehouse, so ``SELECT * FROM orders`` keeps working and an ambiguous name
          still fails loudly instead of picking a schema.
        """
        tables = self._lakehouse_tables(conn)
        tokens = set(re.findall(r"[a-zA-Z0-9_]+", sql.lower()))
        counts: Dict[str, int] = {}
        for _, table in tables:
            counts[table.lower()] = counts.get(table.lower(), 0) + 1

        names: Dict[str, str] = {}
        for schema, table in tables:
            target = f"{SOURCE}.{quote_ident(schema)}.{quote_ident(table)}"
            fqn = f"{SOURCE}.{schema}.{table}"
            if fqn.lower() != target.lower():
                names[fqn.lower()] = target
            if schema.strip() and schema.lower() != "main":
                names.setdefault(f"{schema}.{table}".lower(), target)
            else:
                # A table in the catalog's default schema keeps its documented two-slot
                # address: `lakehouse.orders`, not `lakehouse.main.orders`.
                names.setdefault(f"{SOURCE}.{table}".lower(), target)

            # A bare alias is a convenience, not an address: only when exactly one table
            # answers to that name, and never for a word SQL already uses.
            bare = table.lower()
            if counts[bare] == 1 and bare in tokens and bare not in RESERVED:
                try:
                    conn.execute(
                        f"CREATE OR REPLACE TEMP VIEW {quote_ident(table)} AS "
                        f"SELECT * FROM {target};"
                    )
                except Exception as exc:
                    logger.debug("Could not alias %s: %s", table, exc)

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
