import os
import re
import time
import asyncio
import functools
import logging
import threading
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

# The safety-path expression is constant, but it used to be compiled for every query.
_UNSAFE_PATH_RE = re.compile(
    r"\b(?:read_csv|read_csv_auto|read_json|read_text|read_blob|copy\s+.*?to)\s*\(\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)


@functools.lru_cache(maxsize=8)
def _compiled_rewrite_pattern(
    items: tuple[tuple[str, str], ...]
) -> re.Pattern:
    """Compile one catalog-name mapping once; the catalog changes far less than queries."""
    ordered = sorted((name for name, _ in items), key=len, reverse=True)
    return re.compile(
        r"(?<![A-Za-z0-9_.])("
        + "|".join(re.escape(name) for name in ordered)
        + r")(?![A-Za-z0-9_])",
        re.IGNORECASE,
    )


# --------------------------------------------------------------- light queries
# A "light" query cannot allocate memory proportional to the table and reads at most a
# handful of row groups: a projection/filter with a small pushed-down LIMIT, or a
# metadata statement. Those are what a dashboard issues constantly, so they must not
# queue behind an analytical scan or a dbt build. Anything ambiguous stays heavy.
MAX_LIGHT_LIMIT = 1000
_METADATA_LEADERS = ("EXPLAIN", "DESCRIBE", "SHOW", "SUMMARIZE", "PRAGMA")
_HEAVY_KEYWORDS = re.compile(
    r"\b(JOIN|UNION|INTERSECT|EXCEPT|GROUP\s+BY|ORDER\s+BY|DISTINCT|QUALIFY|WINDOW"
    r"|WITH|HAVING)\b"
)
_WINDOW_CALL = re.compile(r"\bOVER\s*\(")
_AGGREGATE_CALL = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|MEDIAN|STDDEV|STDDEV_POP|STDDEV_SAMP|VAR_POP|VAR_SAMP"
    r"|VARIANCE|CORR|COVAR_POP|COVAR_SAMP|APPROX_[A-Z_]+|LIST|ARRAY_AGG|STRING_AGG"
    r"|BIT_AND|BIT_OR|BIT_XOR|PRODUCT|FIRST|LAST|ANY_VALUE|BOOL_AND|BOOL_OR)\s*\("
)
_SUBQUERY = re.compile(r"\(\s*SELECT\b")
_LIMIT_TAIL = re.compile(r"\bLIMIT\s+(\d+)\s*$")
_COMMENTS = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")


def _sql_skeleton(sql: object) -> str:
    """Comment- and string-free, upper-cased, whitespace-collapsed SQL.

    A keyword inside a comment or a string literal must not change an admission
    decision (``SELECT 'GROUP BY' AS joke FROM t LIMIT 1`` is still a light query),
    so both are removed before the classifier looks at the text.
    """
    text = _COMMENTS.sub(" ", str(sql or ""))
    text = _STRING_LITERAL.sub("''", text)
    return re.sub(r"\s+", " ", text).strip().upper()


def classify_light_query(sql: object, *, max_limit: int = MAX_LIGHT_LIMIT) -> bool:
    """Whether ``sql`` is cheap enough for the light pool. Conservative by design.

    Light requires *all* of: a single statement; either a metadata statement
    (``EXPLAIN``/``DESCRIBE``/``SHOW``/``SUMMARIZE``/``PRAGMA``) or a ``SELECT`` with a
    final ``LIMIT`` no larger than ``max_limit``; and none of the operators that
    allocate memory proportional to the table — joins (explicit or comma), set
    operations, grouping, ordering, distinct, windows, aggregates, CTEs or
    subqueries. A misclassified light query only loses an optimisation; a
    misclassified heavy query would defeat the point of the pool, so the answer is
    ``False`` unless everything above is proven.
    """
    text = _sql_skeleton(sql)
    if not text:
        return False
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if ";" in text:
        return False  # more than one statement
    leader = text.split(" ", 1)[0]
    if leader in _METADATA_LEADERS:
        return True
    if leader != "SELECT":
        return False
    match = _LIMIT_TAIL.search(text)
    if match is None or int(match.group(1)) > max(1, int(max_limit)):
        return False
    if (
        _HEAVY_KEYWORDS.search(text)
        or _WINDOW_CALL.search(text)
        or _AGGREGATE_CALL.search(text)
        or _SUBQUERY.search(text)
    ):
        return False
    # An implicit comma join (`FROM a, b`) is still a join.
    from_clause = text.split(" FROM ", 1)
    if len(from_clause) > 1 and "," in from_clause[1]:
        return False
    return True


class DuckDBEngine:
    """
    In-process DuckDB analytical engine with:
    - Zero-cost storage URI integration (reads from settings.duckdb_storage_path)
    - Dynamic memory, thread, and spill sizing via ResourceAdvisor
    - Bounded concurrency semaphore for heavy analytical queries/exports
    - Dynamic scanner attaching (PostgreSQL, MySQL, MongoDB)
    - Non-blocking query execution & Parquet/CSV streaming exports
    """

    COMMUNITY_EXTENSIONS: tuple[str, ...] = ("mongo",)

    # Operations that write to the lakehouse. Two of them must never overlap: DuckLake
    # DDL (CREATE OR REPLACE, compaction, merges) conflicts on schema/table renames.
    # Readers are deliberately not in this set — they may run beside a writer, which is
    # exactly what the old "dbt reserves the whole pool" rule forbade.
    WRITER_KINDS: frozenset[str] = frozenset(
        {"dbt", "ingest", "maintenance", "warehouse_sync"}
    )
    # Lease kinds that belong to the cheap-query pool. Telemetry subtracts these from
    # the heavy accounting so a preview never looks like an analytical scan.
    LIGHT_KINDS: frozenset[str] = frozenset({"query_light"})

    def __init__(self):
        self._ensure_storage()
        self._max_heavy_slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
        self._heavy_semaphore = threading.BoundedSemaphore(self._max_heavy_slots)
        # Cheap, row-capped queries get their own pool so a point lookup or catalog
        # preview is never queued behind a multi-second scan or a dbt build. The
        # ceiling is half the heavy pool, so (N + N/2) connections x (pool/N) memory
        # keeps the worst case at 63.75% of RAM — inside the 85% safe allocation.
        self._max_light_slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_LIGHT))
        self._light_semaphore = threading.BoundedSemaphore(self._max_light_slots)
        # Reentrant: a writer that nests another writer operation in the same thread
        # (a dbt run publishing warehouse views, for example) must not deadlock itself.
        self._writer_lock = threading.RLock()
        self._telemetry_lock = threading.Lock()
        self._active_heavy_queries = 0
        self._waiting_heavy_queries = 0
        self._active_light_queries = 0
        self._waiting_light_queries = 0
        self._active_writers = 0
        self._total_queries_executed = 0
        self._lease_local = threading.local()

    def _push_lease(
        self,
        lease_id: Optional[str],
        slots: int,
        semaphore: threading.BoundedSemaphore,
        writer_locked: bool,
    ) -> None:
        stack = getattr(self._lease_local, "stack", None)
        if stack is None:
            stack = []
            self._lease_local.stack = stack
        stack.append((lease_id, slots, semaphore, writer_locked))

    def _pop_lease(
        self,
    ) -> Optional[tuple[Optional[str], int, threading.BoundedSemaphore, bool]]:
        stack = getattr(self._lease_local, "stack", None)
        if not stack:
            return None
        return stack.pop()

    from contextlib import contextmanager

    @contextmanager
    def heavy_slot(
        self,
        slots: int = 1,
        *,
        kind: str = "query",
        memory_limit_mb: int | None = None,
        temp_limit_mb: int | None = None,
        temp_path: str | None = None,
        detail: str | None = None,
        light: bool = False,
    ):
        """The shared heavy-work budget, for callers outside this class.

        Every heavy job reserves one or more permits. Reservations are also written to
        the cross-process lease directory so scripts and notebooks started in their own
        Python process are visible to the backend telemetry. ``light`` draws from the
        cheap-query pool instead of the analytical one.
        """
        reserved = self.begin_heavy(
            slots,
            kind=kind,
            memory_limit_mb=memory_limit_mb,
            temp_limit_mb=temp_limit_mb,
            temp_path=temp_path,
            detail=detail,
            light=light,
        )
        try:
            yield reserved
        finally:
            self.end_heavy(reserved)

    def begin_heavy(
        self,
        slots: int = 1,
        *,
        kind: str = "query",
        memory_limit_mb: int | None = None,
        temp_limit_mb: int | None = None,
        temp_path: str | None = None,
        detail: str | None = None,
        light: bool = False,
    ) -> int:
        """Takes ``slots`` permits and returns how many were actually acquired.

        ``light`` reserves from the cheap-query pool instead of the analytical one.
        Writer kinds additionally take the process-wide writer lock, so two lakehouse
        writers never overlap — the guarantee the old "dbt reserves everything" rule
        was buying, without banning readers.
        """
        semaphore = self._light_semaphore if light else self._heavy_semaphore
        ceiling = self._max_light_slots if light else self._max_heavy_slots
        count = max(1, min(int(slots), ceiling))
        writer_locked = kind in self.WRITER_KINDS
        if writer_locked:
            # Before the permits, never after: a writer holding permits while waiting
            # for the lock could starve the writer that holds it.
            self._writer_lock.acquire()
        with self._telemetry_lock:
            if light:
                self._waiting_light_queries += 1
            else:
                self._waiting_heavy_queries += 1
        acquired = 0
        try:
            for _ in range(count):
                semaphore.acquire()
                acquired += 1
        except BaseException:
            for _ in range(acquired):
                semaphore.release()
            if writer_locked:
                self._writer_lock.release()
            with self._telemetry_lock:
                if light:
                    self._waiting_light_queries = max(0, self._waiting_light_queries - 1)
                else:
                    self._waiting_heavy_queries = max(0, self._waiting_heavy_queries - 1)
            raise
        lease_id = None
        try:
            from app.core.resource_leases import register_resource_lease

            lease_id = register_resource_lease(
                kind=kind,
                slots=acquired,
                memory_limit_mb=memory_limit_mb,
                temp_limit_mb=temp_limit_mb,
                temp_path=temp_path,
                detail=detail,
            )
        except Exception:
            lease_id = None
        self._push_lease(lease_id, acquired, semaphore, writer_locked)
        with self._telemetry_lock:
            if light:
                self._waiting_light_queries = max(0, self._waiting_light_queries - 1)
                self._active_light_queries += acquired
            else:
                self._waiting_heavy_queries = max(0, self._waiting_heavy_queries - 1)
                self._active_heavy_queries += acquired
            if writer_locked:
                self._active_writers += 1
            self._total_queries_executed += 1
        return acquired

    def end_heavy(self, slots: int = 1) -> None:
        """Releases permits taken by :meth:`begin_heavy`."""
        popped = self._pop_lease()
        writer_locked = False
        if popped is not None:
            lease_id, count, semaphore, writer_locked = popped
        else:
            lease_id = None
            count = max(1, min(int(slots), self._max_heavy_slots))
            semaphore = self._heavy_semaphore
        try:
            from app.core.resource_leases import release_resource_lease

            release_resource_lease(lease_id)
        except Exception:
            pass
        with self._telemetry_lock:
            if semaphore is self._light_semaphore:
                self._active_light_queries = max(0, self._active_light_queries - count)
            else:
                self._active_heavy_queries = max(0, self._active_heavy_queries - count)
            if writer_locked:
                self._active_writers = max(0, self._active_writers - 1)
        for _ in range(count):
            semaphore.release()
        if writer_locked:
            self._writer_lock.release()

    @contextmanager
    def _acquire_heavy_slot(self, slots: int = 1, *, kind: str = "query"):
        with self.heavy_slot(slots, kind=kind):
            yield

    @contextmanager
    def _acquire_light_slot(self, slots: int = 1, *, kind: str = "query_light"):
        with self.heavy_slot(slots, kind=kind, light=True):
            yield

    def get_telemetry(self) -> Dict[str, Any]:
        """Live DuckDB engine concurrency, spill directory usage, and lakehouse headroom."""
        from app.core.catalog import get_active_load_tables
        from app.core.resource_leases import leases_by_kind, list_resource_leases

        slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
        threads = (
            int(settings.DUCKDB_THREADS)
            if settings.DUCKDB_THREADS
            else advisor.get_duckdb_threads(slots)
        )
        temp_path = settings.duckdb_temp_path
        leases = list_resource_leases()
        heavy_leases = [
            item
            for item in leases
            if str(item.get("kind") or "") not in self.LIGHT_KINDS
        ]
        light_leases = [
            item for item in leases if str(item.get("kind") or "") in self.LIGHT_KINDS
        ]
        leased_slots = sum(max(1, int(item.get("slots") or 1)) for item in heavy_leases)
        light_leased_slots = sum(
            max(1, int(item.get("slots") or 1)) for item in light_leases
        )
        active_by_kind = leases_by_kind(heavy_leases)

        with self._telemetry_lock:
            local_active = self._active_heavy_queries
            waiting_q = self._waiting_heavy_queries
            local_light = self._active_light_queries
            waiting_light = self._waiting_light_queries
            active_writers = self._active_writers
            total_q = self._total_queries_executed
        active_q = max(local_active, leased_slots)
        active_light = max(local_light, light_leased_slots)
        if local_active > leased_slots:
            active_by_kind["unknown"] = active_by_kind.get("unknown", 0) + (
                local_active - leased_slots
            )

        spill_roots: list[Path] = []
        try:
            base_root = temp_path.resolve()
        except Exception:
            base_root = temp_path
        if base_root.is_dir():
            spill_roots.append(base_root)
        for lease in leases:
            raw = str(lease.get("temp_path") or "").strip()
            if not raw:
                continue
            try:
                candidate = Path(raw).resolve()
            except Exception:
                continue
            if candidate not in spill_roots:
                spill_roots.append(candidate)

        temp_bytes = 0
        seen_files: set[str] = set()
        for root in spill_roots:
            if not root.is_dir():
                continue
            for entry in root.rglob("*"):
                try:
                    if not entry.is_file():
                        continue
                    name = entry.name
                    if any(part.startswith("dbt_run_") for part in entry.parts):
                        continue
                    if name.startswith("dbt_scratch_") and ".duckdb" in name:
                        continue
                    key = str(entry)
                    if key in seen_files:
                        continue
                    seen_files.add(key)
                    temp_bytes += entry.stat().st_size
                except OSError:
                    continue
        per_slot_temp_mb = advisor.get_duckdb_temp_limit_mb(
            temp_path.as_posix(),
            settings.DUCKDB_TEMP_SIZE_LIMIT,
            slots,
            settings.DUCKDB_TEMP_SPILL_FREE_DISK_PCT,
        )
        per_slot_memory_mb = advisor.get_duckdb_memory_limit_mb(slots)
        active_temp_limit_mb = sum(
            int(item.get("temp_limit_mb") or 0)
            or per_slot_temp_mb * max(1, int(item.get("slots") or 1))
            for item in leases
        ) if leases else per_slot_temp_mb * max(1, active_q)
        active_memory_limit_mb = sum(
            int(item.get("memory_limit_mb") or 0)
            or per_slot_memory_mb * max(1, int(item.get("slots") or 1))
            for item in leases
        ) if leases else per_slot_memory_mb * max(1, active_q)
        reservations = [
            {
                "kind": str(item.get("kind") or "unknown"),
                "slots": max(1, int(item.get("slots") or 1)),
                "memory_limit_mb": item.get("memory_limit_mb"),
                "temp_limit_mb": item.get("temp_limit_mb"),
                "temp_path": item.get("temp_path"),
                "detail": item.get("detail"),
                "pid": item.get("pid"),
                "started_at": item.get("started_at"),
            }
            for item in leases
        ]
        disk = advisor.get_disk_info(settings.duckdb_storage_path)
        return {
            "active_heavy_queries": active_q,
            "waiting_heavy_queries": waiting_q,
            "max_concurrent_heavy": slots,
            # Cheap-query pool: previews and point lookups run here, so they are
            # visible without being counted against the analytical slots.
            "active_light_queries": active_light,
            "waiting_light_queries": waiting_light,
            "max_concurrent_light": self._max_light_slots,
            "active_writers": active_writers,
            "total_queries_executed": total_q,
            "active_by_kind": active_by_kind,
            "active_reservations": reservations,
            "memory_limit_per_slot": advisor.get_duckdb_memory_limit(slots),
            "active_memory_limit_mb": active_memory_limit_mb,
            "threads_per_slot": threads,
            "temp_directory": temp_path.as_posix(),
            "temp_spill_bytes": temp_bytes,
            "temp_spill_mb": round(temp_bytes / (1024 ** 2), 2),
            "temp_spill_limit": f"{per_slot_temp_mb}MiB",
            "temp_spill_limit_mb": per_slot_temp_mb,
            "active_temp_limit_mb": active_temp_limit_mb,
            "temp_spill_scope": "active leases + shared temp directory",
            "lakehouse_free_gb": disk.get("free_gb", 0.0),
            "lakehouse_percent_used": disk.get("percent_used", 0.0),
            "active_load_tables": len(get_active_load_tables()),
        }

    def _ensure_storage(self) -> str:
        """Returns the resolved lakehouse root."""
        return settings.duckdb_storage_path

    def get_connection(self, *, with_lakehouse: bool = True) -> duckdb.DuckDBPyConnection:
        """An in-memory DuckDB connection with this process's budget applied."""
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
        slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
        threads = (
            int(settings.DUCKDB_THREADS)
            if settings.DUCKDB_THREADS
            else advisor.get_duckdb_threads(slots)
        )
        statements = [
            f"SET memory_limit = '{advisor.get_duckdb_memory_limit(slots)}';",
            f"SET temp_directory = '{settings.duckdb_temp_path.as_posix()}';",
            "SET max_temp_directory_size = '"
            f"{advisor.get_duckdb_temp_limit(settings.duckdb_temp_path.as_posix(), settings.DUCKDB_TEMP_SIZE_LIMIT, slots, settings.DUCKDB_TEMP_SPILL_FREE_DISK_PCT)}';",
            f"SET threads = {threads};",
            "SET preserve_insertion_order = false;",
            f"SET storage_directory = '{self._ensure_storage()}';",
        ]
        for statement in statements:
            try:
                conn.execute(statement)
            except Exception as exc:
                logger.debug("DuckDB setting failed (%s): %s", statement, exc)

    def attach_lakehouse(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Attaches the DuckLake catalog under the name ``lakehouse``."""
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
        """Dynamically attaches a remote or local PostgreSQL database to the DuckDB connection."""
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
        """Attaches a MySQL/MariaDB database through DuckDB's own ``mysql`` scanner."""
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
        """Attaches a MongoDB deployment through the community ``mongo`` scanner."""
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
        """Attaches one dict from :func:`app.core.sources.attachment_for`."""
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
        """Installs the community scanners once, where the unsigned flag can be set."""
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
        """Rewrites the dotted address someone typed into the form DuckDB parses."""
        if not mapping:
            return sql

        items = tuple(
            sorted(
                ((str(name), str(value)) for name, value in mapping.items()),
                key=lambda item: len(item[0]),
                reverse=True,
            )
        )
        pattern = _compiled_rewrite_pattern(items)
        lookup = dict(items)

        def replace(match: re.Match) -> str:
            return lookup.get(match.group(1).lower(), match.group(1))

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
        """Makes warehouse names resolvable, without walking the filesystem."""
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
                names.setdefault(f"{SOURCE}.{table}".lower(), target)

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
    def _assert_safe_query_paths(sql: str) -> None:
        """Blocks arbitrary file reading/writing outside allowed workspace roots."""
        pattern = _UNSAFE_PATH_RE
        lakehouse_root = settings.lakehouse_root_path.resolve()
        temp_root = settings.duckdb_temp_path.resolve()
        git_root = settings.git_repo_path.resolve()

        for match in pattern.finditer(sql):
            raw_path = match.group(1).strip()
            if raw_path.startswith(("http://", "https://", "s3://", "gs://", "abfss://")):
                continue
            if ".." in raw_path or Path(raw_path).is_absolute() or re.match(r"^[A-Za-z]:[/\\]", raw_path):
                resolved = Path(raw_path).resolve()
                is_allowed = False
                for root in (lakehouse_root, temp_root, git_root):
                    try:
                        resolved.relative_to(root)
                        is_allowed = True
                        break
                    except ValueError:
                        continue
                if not is_allowed:
                    raise PermissionError(f"Access to path '{raw_path}' is forbidden: outside allowed workspace storage.")

    @staticmethod
    def _normalize_parquet_paths(sql: str) -> str:
        """Turns a relative ``read_parquet('...')`` argument into an absolute path."""
        lakehouse_root_posix = settings.lakehouse_root_path.resolve().as_posix()
        lakehouse_root = settings.lakehouse_root_path.resolve()
        temp_root = settings.duckdb_temp_path.resolve()
        git_root = settings.git_repo_path.resolve()

        def _replace_parquet_path(m: re.Match) -> str:
            path_str = m.group(1).strip()
            if path_str.startswith(("http://", "https://", "s3://", "gs://", "abfss://")):
                return f"read_parquet('{path_str}')"
            if Path(path_str).is_absolute() or re.match(r"^[A-Za-z]:[/\\]", path_str):
                resolved = Path(path_str).resolve()
                is_allowed = False
                for root in (lakehouse_root, temp_root, git_root):
                    try:
                        resolved.relative_to(root)
                        is_allowed = True
                        break
                    except ValueError:
                        continue
                if not is_allowed:
                    raise PermissionError(f"Access to path '{path_str}' is forbidden: outside allowed workspace storage.")
                return f"read_parquet('{resolved.as_posix()}')"

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
        light: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Executes a SQL query against DuckDB in a thread-safe synchronous runner.

        ``light`` selects the cheap-query pool explicitly. When it is ``None`` the
        statement is classified (:func:`classify_light_query`): a point lookup or a
        metadata statement must not queue behind a multi-second analytical scan or a
        dbt build, while anything that can allocate memory proportional to the table
        stays on the heavy pool.
        """
        self._assert_safe_query_paths(sql)
        start_time = time.time()
        storage_path = self._ensure_storage()
        use_light = classify_light_query(sql) if light is None else bool(light)
        slot = self._acquire_light_slot if use_light else self._acquire_heavy_slot
        with slot(kind="query_light" if use_light else "query"):
            conn = self.get_connection()
            if on_connection is not None:
                on_connection(conn)

            try:
                for attachment in attachments or postgres_attachments or []:
                    self.attach(conn, attachment)

                exec_sql = self._prepare_sql_and_views(conn, sql, storage_path)
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
        """Streams query results directly to ``dest_path`` using DuckDB's native COPY."""
        storage_path = self._ensure_storage()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        advisor.assert_disk_headroom(dest_path.parent.as_posix())
        with self._acquire_heavy_slot(kind="export"):
            conn = self.get_connection()
            try:
                for attachment in attachments or postgres_attachments or []:
                    self.attach(conn, attachment)

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
        """Executes a query and returns Parquet or CSV bytes."""
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
