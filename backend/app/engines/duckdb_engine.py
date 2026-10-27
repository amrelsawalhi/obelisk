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
    - Dynamic scanner attaching (PostgreSQL, MySQL, MongoDB)
    - Non-blocking query execution & Parquet/CSV streaming exports
    """

    # Scanners DuckDB keeps in its community repository rather than the signed core.
    COMMUNITY_EXTENSIONS: tuple[str, ...] = ("mongo",)

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
            f"SET memory_limit = '{advisor.get_duckdb_memory_limit(settings.DUCKDB_MAX_CONCURRENT_HEAVY)}';",
            f"SET temp_directory = '{settings.duckdb_temp_path.as_posix()}';",
            # The spill budget follows the free space on the volume that holds it, so a
            # 50 GB ceiling on a 40 GB disk cannot promise space the disk does not have.
            "SET max_temp_directory_size = '"
            f"{advisor.get_duckdb_temp_limit(settings.duckdb_temp_path.as_posix(), settings.DUCKDB_TEMP_SIZE_LIMIT)}';",
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

    def attach_mysql(
        self,
        conn: duckdb.DuckDBPyConnection,
        alias: str,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        connect_timeout: int = 8,
    ) -> None:
        """Attaches a MySQL/MariaDB database through DuckDB's own ``mysql`` scanner.

        ``READ_ONLY`` is not decoration: DuckDB can ``INSERT`` through this scanner, and a
        source attached by the platform is a source it does not own.
        """
        try:
            conn.execute("LOAD mysql;")
        except Exception:
            try:
                conn.execute("INSTALL mysql; LOAD mysql;")
            except Exception as exc:
                logger.debug("DuckDB mysql extension load failed: %s", exc)

        from app.core.dsn import build_kv_dsn, sql_string, validate_alias
        from app.core.host_resolver import resolve_connection_host

        dsn = build_kv_dsn(
            host=resolve_connection_host(host, port),
            port=port,
            database=database,
            user=username,
            password=password,
            connect_timeout=connect_timeout,
        )
        safe_alias = validate_alias(alias)
        conn.execute(
            f"ATTACH {sql_string(dsn)} AS {safe_alias} (TYPE MYSQL, READ_ONLY);"
        )

    def attach_mongo(self, conn: duckdb.DuckDBPyConnection, alias: str, uri: str) -> None:
        """Attaches a MongoDB deployment through the community ``mongo`` scanner.

        Only ``LOAD`` happens here. ``INSTALL`` needs ``allow_unsigned_extensions``, which
        DuckDB refuses to change once a database is running, so the install is a startup
        step (:meth:`install_community_extensions`) and a session can only load a scanner
        that is already on disk.
        """
        try:
            conn.execute("LOAD mongo;")
        except Exception as exc:
            raise RuntimeError(
                "The mongo scanner is not installed, so this source cannot be attached. "
                "Start the backend once with network access (and "
                "OBELISK_ALLOW_COMMUNITY_EXTENSIONS=true), or keep the source pull-only."
            ) from exc

        from app.core.dsn import sql_string, validate_alias

        safe_alias = validate_alias(alias)
        conn.execute(
            f"ATTACH {sql_string(uri)} AS {safe_alias} (TYPE MONGO, READ_ONLY);"
        )

    def attach(
        self, conn: duckdb.DuckDBPyConnection, attachment: Dict[str, Any]
    ) -> None:
        """Attaches one dict from :func:`app.core.sources.attachment_for`.

        A dict without ``kind`` is the pre-MySQL shape, which could only ever mean
        Postgres, so it is still accepted: the notebooks and the editor ship their own
        helper, and an out-of-tree caller should not break because a scanner was added.
        """
        kind = attachment.get("kind") or "postgres"
        fields = {key: value for key, value in attachment.items() if key != "kind"}
        if kind == "mongo":
            self.attach_mongo(conn, uri=fields.pop("uri"), **fields)
        elif kind == "mysql":
            self.attach_mysql(conn, **fields)
        elif kind == "postgres":
            self.attach_postgres(conn, **fields)
        else:
            raise ValueError(
                f"Unknown attachment kind {kind!r}; expected postgres, mysql or mongo."
            )

    def install_community_extensions(self) -> List[str]:
        """Installs the community scanners once, where the unsigned flag can be set.

        ``allow_unsigned_extensions`` is a connection-level config that cannot be changed
        while a database runs, so this opens its own short-lived connection, installs, and
        closes it. Sessions then load a signed-on-disk copy without the flag. Failure is
        logged, not fatal: an air-gapped install simply keeps such sources pull-only.
        """
        if not settings.ALLOW_COMMUNITY_EXTENSIONS:
            logger.info(
                "Community DuckDB extensions are disabled; MongoDB sources stay pull-only."
            )
            return []

        installed: List[str] = []
        for name in self.COMMUNITY_EXTENSIONS:
            try:
                con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
            except Exception as exc:
                logger.warning("Could not open a connection to install %s: %s", name, exc)
                continue
            try:
                try:
                    con.execute(f"LOAD {name};")
                except Exception:
                    con.execute(f"INSTALL {name} FROM community; LOAD {name};")
                installed.append(name)
            except Exception as exc:
                logger.warning(
                    "DuckDB community extension %r is unavailable (%s); sources that need "
                    "it stay pull-only.",
                    name,
                    exc,
                )
            finally:
                con.close()
        return installed

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
        attachments: Optional[List[Dict[str, Any]]] = None,
        postgres_attachments: Optional[List[Dict[str, Any]]] = None,
        max_rows: Optional[int] = None,
        on_connection: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Executes a SQL query against DuckDB in a thread-safe synchronous runner.
        Can resolve tables in Bronze Parquet directly via read_parquet() or table name.

        ``attachments`` carries one dict per source to attach (see
        :func:`app.core.sources.attachment_for`); ``postgres_attachments`` is the old name
        and is still accepted so an out-of-tree caller keeps working.

        Rows are capped and never materialised beyond ``max_rows``
        (``DUCKDB_MAX_QUERY_ROWS`` by default): a ``SELECT *`` on a huge table returns a
        truncated preview instead of loading the whole relation into Python memory.
        ``on_connection`` receives the live connection so a caller waiting on a timeout can
        call ``conn.interrupt()`` and stop the DuckDB C++ work instead of leaking it.
        """
        start_time = time.time()
        storage_path = self._ensure_storage()
        conn = self.get_connection()
        if on_connection is not None:
            on_connection(conn)

        try:
            for attachment in attachments or postgres_attachments or []:
                self.attach(conn, attachment)

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
                    "truncated": False,
                    "duration_ms": duration_ms,
                }

            limit_cap = max_rows if max_rows is not None else settings.DUCKDB_MAX_QUERY_ROWS
            columns = list(rel.columns or [])
            # DuckDB 2.x relations keep no result between calls (and have no fetchmany), so
            # the cap is pushed into the plan with limit() — only cap + 1 rows are ever
            # materialised, which is what makes `SELECT *` on a huge table safe.
            if limit_cap > 0:
                fetched_tuples = rel.limit(limit_cap + 1).fetchall()
                truncated = len(fetched_tuples) > limit_cap
                if truncated:
                    fetched_tuples = fetched_tuples[:limit_cap]
            else:
                fetched_tuples = rel.fetchall()
                truncated = False

            rows = [dict(zip(columns, tup)) for tup in fetched_tuples]
            duration_ms = int((time.time() - start_time) * 1000)

            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
                "duration_ms": duration_ms,
            }
        finally:
            conn.close()

    def export_query_to_file(
        self,
        sql: str,
        dest_path: Path,
        output_format: str = "parquet",
        attachments: Optional[List[Dict[str, Any]]] = None,
        postgres_attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Path:
        """Streams query results directly to ``dest_path`` using DuckDB's native COPY.

        The result never passes through Python: a multi-GB export is written by DuckDB to
        the file, not accumulated in an Arrow table and an ``io.BytesIO`` buffer.
        """
        storage_path = self._ensure_storage()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.get_connection()
        try:
            for attachment in attachments or postgres_attachments or []:
                self.attach(conn, attachment)

            # Register bronze parquet views and normalize relative paths
            exec_sql = self._prepare_sql_and_views(conn, sql, storage_path).strip().rstrip(";")
            dest_posix = dest_path.resolve().as_posix().replace("'", "''")
            fmt = output_format.lower()
            if fmt == "parquet":
                conn.execute(
                    f"COPY ({exec_sql}) TO '{dest_posix}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
                )
            elif fmt == "csv":
                conn.execute(
                    f"COPY ({exec_sql}) TO '{dest_posix}' (FORMAT CSV, HEADER TRUE);"
                )
            else:
                raise ValueError(f"Unsupported export format: {output_format}")
            return dest_path
        finally:
            conn.close()

    def export_query(
        self,
        sql: str,
        output_format: str = "parquet",
        attachments: Optional[List[Dict[str, Any]]] = None,
        postgres_attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> bytes:
        """Executes a query and returns Parquet or CSV bytes.

        Compatibility helper for callers that need bytes in hand. The HTTP export route
        uses :meth:`export_query_to_file` instead, so a large export is streamed from
        disk rather than held in RAM here.
        """
        import tempfile

        fmt = output_format.lower()
        with tempfile.TemporaryDirectory(prefix="obelisk_export_") as scratch:
            dest = Path(scratch) / f"export.{fmt}"
            self.export_query_to_file(
                sql,
                dest,
                output_format=fmt,
                attachments=attachments,
                postgres_attachments=postgres_attachments,
            )
            return dest.read_bytes()


    def query_bronze_to_polars(self, table_name: str) -> pl.DataFrame:
        """Reads a Bronze Parquet file into a Polars DataFrame."""
        storage_path = self._ensure_storage()
        file_path = Path(storage_path) / f"{table_name}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"Bronze table file {file_path.as_posix()} does not exist.")
        return pl.read_parquet(file_path.as_posix())

duckdb_engine = DuckDBEngine()
