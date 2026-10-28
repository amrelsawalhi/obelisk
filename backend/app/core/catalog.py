"""The catalog is a projection of the lakehouse's own metadata.

Nothing registers a table. :func:`scan` asks DuckLake which tables exist — schema, row
count and byte size come from its metadata tables — and ``catalog_objects`` is refreshed
from that answer, so a row can never outlive its table or describe data nobody wrote.

A table's address is still three slots, and a folder is still a schema::

    raw/orders            -> lakehouse.raw.orders
    supabase/auth/table1  -> lakehouse."supabase.auth".table1

``source`` is the warehouse itself (``lakehouse``) or an attached database's alias, the
folder path is the middle slot, and the table name is the last. What changed is only who
owns the truth: the Parquet files hold the data, the catalog holds which files belong to
which table at which version.
"""
from __future__ import annotations

from contextlib import contextmanager
import logging
import os
from pathlib import Path
import re
import sqlite3
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from app.core.storage import FileInfo, WarehouseStorage, normalize_rel, warehouse_storage

logger = logging.getLogger(__name__)

SOURCE = "lakehouse"
PARQUET_SUFFIX = ".parquet"

LOAD_TABLE_SUFFIX = "__obelisk_load"

_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

RESERVED = frozenset(
    """
    all alter analyze and as asc attach begin between by case cast check column commit
    copy create cross default delete desc describe detach distinct drop else end escape
    except exists explain export false filter first following for foreign from full
    group groups having ilike import in index inner insert intersect into is join key
    last left like limit natural not null offset on or order outer over partition pivot
    pragma primary qualify range recursive references reset returning right rollback
    rows sample select set table tablesample then to true union unique unpivot update
    use using values view when where window with
    """.split()
)


class InvalidTablePath(ValueError):
    """Raised when a table path cannot be represented as a three-slot address."""


MAX_SEGMENT_LENGTH = 63


def validate_segment(name: object, *, what: str = "name") -> str:
    """One folder or table name: letters, digits, ``_`` and ``-``, not starting with ``-``."""
    value = name.strip() if isinstance(name, str) else ""
    if not value:
        raise InvalidTablePath(f"A {what} is required.")
    if len(value) > MAX_SEGMENT_LENGTH:
        raise InvalidTablePath(
            f"{what.capitalize()} {value[:20]!r}… is {len(value)} characters; "
            f"the limit is {MAX_SEGMENT_LENGTH}."
        )
    if not _SEGMENT_RE.match(value):
        raise InvalidTablePath(
            f"{what.capitalize()} {value!r} may only contain letters, digits, '_' and '-', "
            "and must start with a letter or digit."
        )
    return value


def validate_column(name: object, *, what: str = "column name") -> str:
    """A column name used in generated SQL: ``[A-Za-z_][A-Za-z0-9_]*``."""
    value = name.strip() if isinstance(name, str) else ""
    if not value:
        raise InvalidTablePath(f"A {what} is required.")
    if len(value) > MAX_SEGMENT_LENGTH:
        raise InvalidTablePath(
            f"{what.capitalize()} {value[:20]!r}… is {len(value)} characters; "
            f"the limit is {MAX_SEGMENT_LENGTH}."
        )
    if not _SIMPLE_IDENT_RE.match(value):
        raise InvalidTablePath(
            f"{what.capitalize()} {value!r} may only contain letters, digits and '_', "
            "and may not start with a digit."
        )
    return value


def validate_folder(path: object, *, what: str = "folder") -> str:
    """A folder path: validated segments joined by ``/``, returned normalised."""
    raw = path.strip().replace("\\", "/") if isinstance(path, str) else ""
    parts = [part for part in raw.split("/") if part]
    if not parts:
        raise InvalidTablePath(f"A {what} path is required.")
    for part in parts:
        if part in (".", ".."):
            raise InvalidTablePath(
                f"{what.capitalize()} path {raw!r} may not contain '.' or '..' segments."
            )
        validate_segment(part, what=f"{what} name")
    return "/".join(parts)


class LakehouseUnavailable(RuntimeError):
    """The lakehouse catalog cannot be reached, so no table can be read or written."""


_ATTACH_LOCK = threading.Lock()
_initialised_catalogs: set[str] = set()
_prepared_catalogs: set[str] = set()

# Registry of in-flight load tables and per-table ingestion locks so concurrent ingests
# and maintenance sweeps never drop a live load table mid-flight.
_LOAD_REGISTRY_LOCK = threading.Lock()
_ACTIVE_LOAD_TABLES: set[tuple[str, str]] = set()
_TABLE_INGEST_LOCKS: dict[tuple[str, str], threading.Lock] = {}

CATALOG_BUSY_TIMEOUT_MS = 15_000


def register_active_load_table(schema: str, table: str) -> None:
    """Marks ``(schema, table)`` as an active in-flight load table protected from maintenance."""
    with _LOAD_REGISTRY_LOCK:
        _ACTIVE_LOAD_TABLES.add((str(schema), str(table)))


def unregister_active_load_table(schema: str, table: str) -> None:
    """Releases active protection on ``(schema, table)`` once ingestion finishes."""
    with _LOAD_REGISTRY_LOCK:
        _ACTIVE_LOAD_TABLES.discard((str(schema), str(table)))

_COMMIT_COUNTER_LOCK = threading.Lock()
_COMMIT_COUNTER = 0
AUTO_COMPACT_COMMIT_INTERVAL = 10


def get_active_load_tables() -> list[tuple[str, str]]:
    """Returns a snapshot of currently active in-flight load tables."""
    with _LOAD_REGISTRY_LOCK:
        return sorted(_ACTIVE_LOAD_TABLES)


def record_lakehouse_commit(conn=None, *, interval: int = AUTO_COMPACT_COMMIT_INTERVAL) -> bool:
    """Tracks lakehouse commits and triggers automatic adjacent-file compaction every ``interval`` commits."""
    global _COMMIT_COUNTER
    with _COMMIT_COUNTER_LOCK:
        _COMMIT_COUNTER += 1
        should_compact = interval > 0 and (_COMMIT_COUNTER % interval == 0)
    if not should_compact:
        return False
    with _LOAD_REGISTRY_LOCK:
        if _ACTIVE_LOAD_TABLES:
            return False
    own_conn = conn is None
    c = conn or lakehouse_connection()
    try:
        c.execute(f"CALL ducklake_merge_adjacent_files('{SOURCE}');").fetchall()
        return True
    except Exception as exc:
        logger.debug("Automatic DuckLake file compaction skipped: %s", exc)
        return False
    finally:
        if own_conn:
            try:
                c.close()
            except Exception:
                pass



@contextmanager
def table_ingest_lock(schema: str, table: str):
    """Serializes concurrent ingestion writes targeting the same ``(schema, table)``."""
    key = (str(schema), str(table))
    with _LOAD_REGISTRY_LOCK:
        lock = _TABLE_INGEST_LOCKS.setdefault(key, threading.Lock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _prepare_catalog_file(catalog: Path) -> None:
    """Puts the DuckLake metadata file in WAL mode, once per process."""
    key = str(catalog)
    if key in _prepared_catalogs:
        return
    try:
        con = sqlite3.connect(catalog, timeout=CATALOG_BUSY_TIMEOUT_MS / 1000)
        try:
            mode = con.execute("PRAGMA journal_mode=WAL;").fetchone()
            con.execute(f"PRAGMA busy_timeout={CATALOG_BUSY_TIMEOUT_MS};")
        finally:
            con.close()
    except Exception as exc:
        # Do not add to _prepared_catalogs on transient lock error so the next attach retries.
        logger.debug("Could not put the DuckLake catalog in WAL mode (will retry): %s", exc)
        return
    _prepared_catalogs.add(key)
    if mode and str(mode[0]).lower() != "wal":
        logger.info(
            "DuckLake catalog %s is in %s mode (WAL unavailable here); concurrent writers "
            "will serialise.",
            catalog,
            mode[0],
        )


def attach_lakehouse(conn) -> None:
    """Attaches the DuckLake catalog under the name ``lakehouse``."""
    from app.config import settings

    catalog = settings.lakehouse_catalog_path
    key = str(catalog)
    if key in _initialised_catalogs:
        _attach_catalog(conn, catalog)
        return
    with _ATTACH_LOCK:
        _attach_catalog(conn, catalog)
        _initialised_catalogs.add(key)


def _attach_catalog(conn, catalog: Path) -> None:
    """Loads the DuckLake extension and attaches ``catalog``."""
    from app.config import settings

    try:
        conn.execute("LOAD ducklake;")
    except Exception:
        try:
            conn.execute("INSTALL ducklake;")
        except Exception:
            try:
                conn.execute("FORCE INSTALL ducklake;")
            except Exception as exc:
                raise LakehouseUnavailable(
                    "The ducklake extension is not available and could not be downloaded. "
                    "Air-gapped installs must ship it: run `INSTALL ducklake` once on a "
                    "connected machine and copy the extension directory across."
                ) from exc
        try:
            conn.execute("LOAD ducklake;")
        except Exception as exc:
            raise LakehouseUnavailable(
                f"The ducklake extension is installed but cannot be loaded: {exc}"
            ) from exc

    catalog.parent.mkdir(parents=True, exist_ok=True)
    _prepare_catalog_file(catalog)
    conn.execute(
        f"ATTACH 'ducklake:sqlite:{catalog.as_posix()}' AS {SOURCE} "
        f"(DATA_PATH '{settings.duckdb_storage_path}', BUSY_TIMEOUT {CATALOG_BUSY_TIMEOUT_MS});"
    )


def lakehouse_connection():
    """A throwaway connection with the session budget and catalog attached."""
    import duckdb
    from app.config import settings
    from app.core.resource_advisor import advisor

    conn = duckdb.connect(":memory:")
    try:
        slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
        cpu_cores = os.cpu_count() if "os" in globals() else 4
        threads = int(settings.DUCKDB_THREADS) if settings.DUCKDB_THREADS else max(1, (cpu_cores or 4) // slots)
        for stmt in (
            f"SET memory_limit = '{advisor.get_duckdb_memory_limit(slots)}';",
            f"SET temp_directory = '{settings.duckdb_temp_path.as_posix()}';",
            f"SET max_temp_directory_size = '{advisor.get_duckdb_temp_limit(settings.duckdb_temp_path.as_posix(), settings.DUCKDB_TEMP_SIZE_LIMIT)}';",
            f"SET threads = {threads};",
            "SET preserve_insertion_order = false;",
        ):
            try:
                conn.execute(stmt)
            except Exception:
                pass
        attach_lakehouse(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _live_table_rows(conn) -> list:
    """Every live table, as the catalog's own metadata table describes it."""
    return conn.execute(
        f"""
        SELECT s.schema_name,
               t.table_name,
               coalesce(st.record_count, 0)      AS num_rows,
               coalesce(st.file_size_bytes, 0)   AS size_bytes,
               t.begin_snapshot,
               t.table_id
        FROM __ducklake_metadata_{SOURCE}.ducklake_table AS t
        JOIN __ducklake_metadata_{SOURCE}.ducklake_schema AS s
          ON s.schema_id = t.schema_id
        LEFT JOIN __ducklake_metadata_{SOURCE}.ducklake_table_stats AS st
          ON st.table_id = t.table_id
        WHERE t.end_snapshot IS NULL
        ORDER BY 1, 2
        """
    ).fetchall()


def lakehouse_tables(conn) -> list[dict]:
    """Every live table, as the catalog describes it (excluding load tables)."""
    return [
        {
            "schema": str(schema),
            "table": str(table),
            "num_rows": int(num_rows or 0),
            "size_bytes": int(size_bytes or 0),
            "table_id": int(table_id),
        }
        for schema, table, num_rows, size_bytes, _begin, table_id in _live_table_rows(conn)
        if not str(table).endswith(LOAD_TABLE_SUFFIX)
    ]


def load_tables(conn) -> list[tuple[str, str]]:
    """The orphaned load tables the catalog still holds, excluding any active in-flight loads."""
    with _LOAD_REGISTRY_LOCK:
        active = set(_ACTIVE_LOAD_TABLES)
    return [
        (str(schema), str(table))
        for schema, table, *_rest in _live_table_rows(conn)
        if str(table).endswith(LOAD_TABLE_SUFFIX) and (str(schema), str(table)) not in active
    ]


def drop_load_tables(conn, *, dry_run: bool = False) -> list[tuple[str, str]]:
    """Drops the orphaned load tables :func:`load_tables` describes; returns what it found."""
    orphans = load_tables(conn)
    if not dry_run:
        for schema, table in orphans:
            conn.execute(
                f"DROP TABLE IF EXISTS {SOURCE}.{quote_ident(schema)}.{quote_ident(table)};"
            )
    return orphans


def lakehouse_all_columns(conn) -> dict[tuple[str, str], tuple[dict, ...]]:
    """Every table's columns in one query, keyed by ``(schema, table)``."""
    try:
        rows = conn.execute(
            f"""
            SELECT schema_name,
                   table_name,
                   column_name,
                   data_type,
                   is_nullable
            FROM duckdb_columns()
            WHERE database_name = '{SOURCE}'
            ORDER BY schema_name, table_name, column_index
            """
        ).fetchall()
    except Exception:  # noqa: BLE001
        return {}
    by_table: dict[tuple[str, str], list[dict]] = {}
    for schema, table, column, data_type, nullable in rows:
        by_table.setdefault((str(schema), str(table)), []).append(
            {
                "name": str(column),
                "data_type": str(data_type),
                "is_nullable": bool(nullable),
                "is_primary_key": False,
            }
        )
    return {key: tuple(columns) for key, columns in by_table.items()}


def lakehouse_columns(conn, schema: str, table: str) -> tuple[dict, ...]:
    """The columns of one table, as DuckDB reports them."""
    try:
        described = conn.execute(
            f'DESCRIBE {SOURCE}.{quote_ident(schema)}.{quote_ident(table)}'
        ).fetchall()
    except Exception:
        return ()
    return tuple(
        {
            "name": row[0],
            "data_type": row[1],
            "is_nullable": str(row[2]).upper() != "NO",
            "is_primary_key": False,
        }
        for row in described
    )


def last_snapshot_time(conn) -> datetime:
    """When the catalog last changed, always in UTC."""
    try:
        row = conn.execute(
            f"SELECT max(snapshot_time) FROM ducklake_snapshots('{SOURCE}')"
        ).fetchone()
    except Exception:
        return datetime.now(timezone.utc)
    if not row or row[0] is None:
        return datetime.now(timezone.utc)
    value = row[0]
    if not isinstance(value, datetime):
        return datetime.now(timezone.utc)
    return value.astimezone(timezone.utc)


def quote_ident(value: str) -> str:
    """Quotes an identifier only when it would otherwise not parse."""
    if _SIMPLE_IDENT_RE.match(value) and value.lower() not in RESERVED:
        return value
    return '"' + value.replace('"', '""') + '"'


def split_table_path(value: str) -> tuple[str, str]:
    """``'supabase/auth/table1'`` -> ``('supabase/auth', 'table1')``."""
    if value is None or not str(value).strip():
        raise InvalidTablePath("A table path is required.")
    clean = str(value).strip().replace("\\", "/")
    if clean.lower().endswith(PARQUET_SUFFIX):
        clean = clean[: -len(PARQUET_SUFFIX)]
    if clean.lower().startswith(f"{SOURCE}."):
        clean = clean[len(SOURCE) + 1 :]
        separator: Optional[str] = "."
    elif "/" in clean:
        separator = "/"
    elif "." in clean:
        separator = "."
    else:
        separator = None

    parts = clean.split(separator) if separator else [clean]
    if any(not part for part in parts):
        raise InvalidTablePath(f"Table path {value!r} contains an empty segment.")
    for part in parts:
        if not _SEGMENT_RE.match(part):
            raise InvalidTablePath(
                f"{part!r} in table path {value!r} may only contain letters, digits, "
                "'_' and '-', and must start with a letter or digit."
            )
    return "/".join(parts[:-1]), parts[-1]


def path_for(folder: str, name: str) -> str:
    """The warehouse-relative file path for a table: ``folder/name.parquet``."""
    folder = (folder or "").strip().strip("/")
    clean = f"{folder}/{name}" if folder else str(name)
    folder_part, table_part = split_table_path(clean)
    return "/".join(part for part in (folder_part, f"{table_part}{PARQUET_SUFFIX}") if part)


def lakehouse_schemas(conn) -> list[str]:
    """The lakehouse's own schemas, which is what a folder is."""
    rows = conn.execute(
        f"SELECT schema_name FROM __ducklake_metadata_{SOURCE}.ducklake_schema "
        "WHERE end_snapshot IS NULL ORDER BY 1"
    ).fetchall()
    return [str(name) for (name,) in rows if str(name) != "main"]


def table_directory(root: Path, schema: str, table: str) -> Path:
    """Where DuckLake keeps one table's Parquet: ``<root>/<schema>/<table>``."""
    if schema == "main":
        return root / table
    return root / schema.replace(".", "/") / table


def _address_parts(rel_path: str) -> Sequence[str]:
    """The path segments of a table, with the Parquet suffix removed."""
    clean = normalize_rel(rel_path)
    if clean.lower().endswith(PARQUET_SUFFIX):
        clean = clean[: -len(PARQUET_SUFFIX)]
    return clean.split("/")


def fqn_for(rel_path: str) -> str:
    """``supabase/auth/table1.parquet`` -> ``lakehouse.supabase.auth.table1``."""
    return ".".join((SOURCE, *_address_parts(rel_path)))


def sql_name_for(rel_path: str) -> str:
    """The name DuckDB should register, quoted only where it must be."""
    parts = _address_parts(rel_path)
    table = quote_ident(parts[-1])
    if len(parts) == 1:
        return f"{SOURCE}.{table}"
    if len(parts) == 2:
        return f"{SOURCE}.{quote_ident(parts[0])}.{table}"
    return f"{SOURCE}.{quote_ident('.'.join(parts[:-1]))}.{table}"


@dataclass(frozen=True)
class TableFile:
    """One Parquet file, described by what the file itself says."""

    path: str
    name: str
    folder: str
    fqn: str
    sql_name: str
    num_rows: int
    size_bytes: int
    modified_at: datetime
    columns: tuple[dict, ...]
    bare_alias: bool = True


@dataclass(frozen=True)
class WarehouseScan:
    """Everything the warehouse holds, plus what could not be read and why."""

    root: str
    tables: tuple[TableFile, ...]
    problems: tuple[str, ...]


_FOOTER_CACHE: dict[tuple, tuple[tuple[dict, ...], int]] = {}
_FOOTER_CACHE_LIMIT = 4096


def read_footer(
    storage: WarehouseStorage, info: FileInfo
) -> tuple[tuple[dict, ...], int]:
    """Reads a Parquet footer: column schema and row count, without the data."""
    key = (info.path, info.size_bytes, info.modified_at.isoformat())
    cached = _FOOTER_CACHE.get(key)
    if cached is not None:
        return cached

    import pyarrow.parquet as pq

    with storage.open_binary(info.path) as handle:
        metadata = pq.ParquetFile(handle).metadata
    arrow_schema = metadata.schema.to_arrow_schema()
    columns = tuple(
        {
            "name": field.name,
            "data_type": str(field.type),
            "is_nullable": bool(field.nullable),
            "is_primary_key": False,
        }
        for field in arrow_schema
    )
    result = (columns, int(metadata.num_rows))
    if len(_FOOTER_CACHE) >= _FOOTER_CACHE_LIMIT:
        _FOOTER_CACHE.clear()
    _FOOTER_CACHE[key] = result
    return result


def table_from_file(storage: WarehouseStorage, info: FileInfo) -> TableFile:
    """Turns one storage record into a catalog entry, or raises if it is not a table."""
    folder, name = split_table_path(info.path)
    columns, num_rows = read_footer(storage, info)
    return TableFile(
        path=info.path,
        name=name,
        folder=folder,
        fqn=fqn_for(info.path),
        sql_name=sql_name_for(info.path),
        num_rows=num_rows,
        size_bytes=info.size_bytes,
        modified_at=info.modified_at,
        columns=columns,
    )


def scan(storage: Optional[WarehouseStorage] = None) -> WarehouseScan:
    """Everything the lakehouse holds, read from its catalog."""
    store = storage or warehouse_storage()
    tables: list[TableFile] = []
    problems: list[str] = []

    conn = lakehouse_connection()
    try:
        rows = lakehouse_tables(conn)
        stamp = last_snapshot_time(conn)
        columns_by_table = lakehouse_all_columns(conn)
        counts: dict[str, int] = {}
        for row in rows:
            schema, name = row["schema"], row["table"]
            folder = "" if schema == "main" else schema
            rel = f"{folder}/{name}" if folder else name
            columns = columns_by_table.get((schema, name)) or lakehouse_columns(
                conn, schema, name
            )
            if not columns:
                problems.append(f"{rel}: the catalog lists it but DuckDB cannot describe it")
            counts[name.lower()] = counts.get(name.lower(), 0) + 1
            tables.append(
                TableFile(
                    path=rel,
                    name=name,
                    folder=folder,
                    fqn=fqn_for(f"{rel}{PARQUET_SUFFIX}"),
                    sql_name=sql_name_for(f"{rel}{PARQUET_SUFFIX}"),
                    num_rows=row["num_rows"],
                    size_bytes=row["size_bytes"],
                    modified_at=stamp,
                    columns=columns,
                )
            )
    finally:
        conn.close()

    tables.sort(key=lambda table: table.path)
    tables = [
        replace(table, bare_alias=counts[table.name.lower()] == 1) for table in tables
    ]

    root = store.display_root() if hasattr(store, "display_root") else ""
    return WarehouseScan(root=root, tables=tuple(tables), problems=tuple(problems))


def table_paths(scan_result: WarehouseScan) -> Iterable[str]:
    """The relative paths of every table in a scan, in order."""
    return (table.path for table in scan_result.tables)
