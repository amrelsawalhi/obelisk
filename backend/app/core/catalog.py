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

import re
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from app.core.storage import FileInfo, WarehouseStorage, normalize_rel, warehouse_storage

SOURCE = "lakehouse"
PARQUET_SUFFIX = ".parquet"

# Tables an ingestion writes into while it streams rows: the snapshot lands in
# ``<table>__obelisk_load`` next to its target and is renamed into place when it is
# complete. They are never a user's table — a half-written load must not be offered as one
# — so every catalog read hides them.
LOAD_TABLE_SUFFIX = "__obelisk_load"

# One folder or table name: letters, digits, '_' and '-', never starting with '-'.
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Identifiers that must be quoted or the parser reads them as syntax. A table may
# legitimately be called "order", so the name has to survive being written by hand.
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


# A name is pasted into SQL, into a URL and into the catalog's text columns, so the rule
# is enforced once, here, and every door uses it. None of these characters can end a
# quoted identifier or start a new statement.
MAX_SEGMENT_LENGTH = 63


def validate_segment(name: object, *, what: str = "name") -> str:
    """One folder or table name: letters, digits, ``_`` and ``-``, not starting with ``-``.

    Rejects anything that would stop being a name once quoted: quotes, semicolons,
    spaces, dots, ``..``, and anything non-ASCII. The 63-character cap is Postgres'
    identifier limit, so a name that passes here also survives the round trip through
    an external warehouse.
    """
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
    """A column name used in generated SQL: ``[A-Za-z_][A-Za-z0-9_]*``.

    No dots and no dashes: a column is never a path, and ``"-"`` in one would have to be
    quoted in every statement that mentions it.
    """
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
    """A folder path: validated segments joined by ``/``, returned normalised.

    ``a\\b`` and ``a/b`` are the same folder; ``.`` and ``..`` are refused rather than
    silently rewritten, because the address a caller asked for is the address they get.
    """
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


# The first attach against a catalog is the one that creates DuckLake's metadata tables,
# and SQLite takes a single writer: two threads arriving together meant the loser died with
# "database is locked". The catalog tree attaches twice at once (its scan and its folder
# list), so it was the tree that fell over on a fresh warehouse while everything after it
# ran fine — a race that showed only on the very first read of a data path.
#
# Only that initialising attach is serialised. Attaching an existing catalog is read-only
# and takes ~160 ms, so keeping every attach behind this lock would queue each query behind
# the previous one; once a catalog is known to exist in this process, attaches overlap
# again exactly as before. A second *process* (the maintenance CLI, a dbt run) is not
# covered by this lock.
_ATTACH_LOCK = threading.Lock()
_initialised_catalogs: set[str] = set()


def attach_lakehouse(conn) -> None:
    """Attaches the DuckLake catalog under the name ``lakehouse``.

    The address people already type — ``lakehouse.raw.orders`` — does not change; only
    who owns the metadata does. The catalog is created on first attach, which is what
    makes a fresh install work without a migration step.

    That first attach is serialised against every other first attach in this process,
    because two of them creating the metadata tables at once is what "database is locked"
    was; see :data:`_ATTACH_LOCK`.
    """
    from app.config import settings

    catalog = settings.lakehouse_catalog_path
    key = str(catalog)
    if key in _initialised_catalogs:
        _attach_catalog(conn, catalog)
        return
    with _ATTACH_LOCK:
        # A thread that waited on the lock attaches to a catalog someone else created,
        # which is the cheap, read-only path again — but it still needs its own ATTACH.
        _attach_catalog(conn, catalog)
        _initialised_catalogs.add(key)


def _attach_catalog(conn, catalog: Path) -> None:
    """Loads the DuckLake extension and attaches ``catalog``.

    Loading is tried before installing: DuckDB 2.0 refuses an ``INSTALL`` whose recorded
    origin differs from the one it would download from, and an extension is often already
    present (shipped in an air-gapped bundle, or installed by an earlier version). A
    ``FORCE INSTALL`` is the last resort and the only step that requires a network.
    """
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
    conn.execute(
        f"ATTACH 'ducklake:sqlite:{catalog.as_posix()}' AS {SOURCE} "
        f"(DATA_PATH '{settings.duckdb_storage_path}');"
    )


def lakehouse_connection():
    """A throwaway connection with the catalog attached.

    Callers that already own a connection should use :func:`attach_lakehouse` instead;
    this exists for the few places (a scan, a preview) that only need one answer.
    """
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        attach_lakehouse(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _live_table_rows(conn) -> list:
    """Every live table, as the catalog's own metadata table describes it.

    One query replaces a directory walk plus a footer read per file: the schema and table
    come from ``ducklake_schema`` / ``ducklake_table``, and the physical facts from
    ``ducklake_table_stats``. Some of these rows are load tables, so callers filter.
    """
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
    """Every live table, as the catalog describes it.

    ``modified_at`` is the catalog's newest snapshot — a coarser answer than a file
    timestamp, and the honest one here, because a table is now many files and only the
    catalog knows when any of them last changed.

    A load table (``<table>__obelisk_load``) is skipped: it holds the rows of a snapshot or
    an upsert that is still being written, and until it is renamed into place it is not a
    table anyone may query, preview or join.
    """
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
    """The load tables the catalog still holds, as ``(schema, table)``.

    A load table is not a table anyone asked for: it is where one ingest streams the rows it
    will swap in or merge from, and a run that ends — either way — drops it. Anything listed
    here therefore belongs to a run that was killed before it could clean up (SIGKILL, an
    OOM kill, a lost machine), so its rows can never be applied and it is pure waste: bytes
    no scan reads and every backup copies. Maintenance drops them.
    """
    return [
        (str(schema), str(table))
        for schema, table, *_rest in _live_table_rows(conn)
        if str(table).endswith(LOAD_TABLE_SUFFIX)
    ]


def drop_load_tables(conn, *, dry_run: bool = False) -> list[tuple[str, str]]:
    """Drops the leftovers :func:`load_tables` describes; returns what it found.

    This is the housekeeping half of a crash: the ingest that owned these tables cannot run
    its own cleanup, so something else has to, or the bytes are carried by every scan of the
    catalog and every backup. ``dry_run`` reports without changing anything.
    """
    orphans = load_tables(conn)
    if not dry_run:
        for schema, table in orphans:
            conn.execute(
                f"DROP TABLE IF EXISTS {SOURCE}.{quote_ident(schema)}.{quote_ident(table)};"
            )
    return orphans


def lakehouse_all_columns(conn) -> dict[tuple[str, str], tuple[dict, ...]]:
    """Every table's columns in one query, keyed by ``(schema, table)``.

    ``scan()`` used to ``DESCRIBE`` each table in turn: one statement per table inside a loop
    over the whole catalog, which is where a lakehouse of a few hundred tables spent most of
    a scan's time. DuckDB's own ``duckdb_columns()`` answers for all of them at once.

    It is asked in preference to DuckLake's ``ducklake_column`` metadata table because a
    column's type is shown to a user and the two disagree on the spelling: the metadata
    table stores DuckLake's own names (``int32``, ``decimal(2,1)``), while
    ``duckdb_columns()`` reports exactly what ``DESCRIBE`` would (``INTEGER``,
    ``DECIMAL(2,1)``).

    Returns an empty mapping if the query fails, which leaves the caller to fall back to
    :func:`lakehouse_columns` table by table.
    """
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
    except Exception:  # noqa: BLE001 - a missing fast path is not a failed scan
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
    """When the catalog last changed, always in UTC.

    DuckDB returns ``TIMESTAMPTZ`` converted into the session's timezone, so the value is
    normalised here: every timestamp the platform shows is UTC, and a tree that mixed
    zones would be subtly wrong everywhere it was compared.
    """
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
    """``'supabase/auth/table1'`` -> ``('supabase/auth', 'table1')``.

    Accepts the three spellings an operator might use — a relative path, the fully
    qualified name, or a bare table name — because callers range from a form field to
    the SDK. Dots and slashes are both separators, so neither may appear inside a
    folder or table name; that is what keeps one path mapping to one address.
    """
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
    """The lakehouse's own schemas, which is what a folder is.

    A folder is a schema, so this is the catalog's answer to "which folders exist" — no
    walk of the warehouse and no guessing from what happens to be on disk. ``main`` is the
    root rather than a folder, so it is left out.
    """
    rows = conn.execute(
        f"SELECT schema_name FROM __ducklake_metadata_{SOURCE}.ducklake_schema "
        "WHERE end_snapshot IS NULL ORDER BY 1"
    ).fetchall()
    return [str(name) for (name,) in rows if str(name) != "main"]


def table_directory(root: Path, schema: str, table: str) -> Path:
    """Where DuckLake keeps one table's Parquet: ``<root>/<schema>/<table>``.

    Not part of the address (the catalog is), but the directory exists anyway, and
    removing it once the table is gone is what keeps it out of anything that walks the
    warehouse. Two caveats, both of which the caller's sweep has to live with: a nested
    folder is one schema named with dots and DuckLake names *its* directory itself, and a
    table that was swapped in from a load table keeps its files under the load table's
    name — so this is where a table's files *start*, not always where they end up.
    """
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
    """The name DuckDB should register, quoted only where it must be.

    A single folder is a schema (``lakehouse.supabase.table1``); anything deeper is
    one schema named after the whole folder path, which the quotes allow to contain
    dots (``lakehouse."supabase.auth".table1``).
    """
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

    path: str  # relative, POSIX: "supabase/auth/table1.parquet"
    name: str  # "table1"
    folder: str  # "supabase/auth", or "" at the root
    fqn: str  # "lakehouse.supabase.auth.table1"
    sql_name: str  # 'lakehouse."supabase.auth".table1'
    num_rows: int
    size_bytes: int
    modified_at: datetime
    columns: tuple[dict, ...]  # footer columns: name, data_type, is_nullable, is_primary_key
    # Whether ``SELECT * FROM table1`` may resolve to this table. True unless another
    # folder holds the same name, where guessing would silently pick one of them.
    bare_alias: bool = True


@dataclass(frozen=True)
class WarehouseScan:
    """Everything the warehouse holds, plus what could not be read and why.

    Problems are reported rather than raised: one unreadable file must not make the
    whole warehouse invisible, and a name collision has to be visible instead of
    silently resolving to whichever file the walk happened to reach first.
    """

    root: str
    tables: tuple[TableFile, ...]
    problems: tuple[str, ...]


_FOOTER_CACHE: dict[tuple, tuple[tuple[dict, ...], int]] = {}
_FOOTER_CACHE_LIMIT = 4096


def read_footer(
    storage: WarehouseStorage, info: FileInfo
) -> tuple[tuple[dict, ...], int]:
    """Reads a Parquet footer: column schema and row count, without the data.

    The footer is a few kilobytes at the end of the file, so this is the cheapest
    honest answer to "what is in this table". The result is cached against the file's
    size and timestamp: a rewritten table changes at least one of them, so the cache
    can never describe an older version of the file, and a scan of an unchanged
    warehouse costs one ``stat`` per file instead of one read.
    """
    key = (info.path, info.size_bytes, info.modified_at.isoformat())
    cached = _FOOTER_CACHE.get(key)
    if cached is not None:
        return cached

    import pyarrow.parquet as pq

    with storage.open_binary(info.path) as handle:
        metadata = pq.ParquetFile(handle).metadata
    # pyarrow exposes the file schema as a Parquet schema; the Arrow view carries the
    # column names and types the engine will actually query. The field names match what
    # the catalog tree has always sent the UI, so nothing downstream has to change.
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
    """Everything the lakehouse holds, read from its catalog.

    One query replaces a directory walk plus a footer read per file: DuckLake records
    which tables exist, how many rows they hold and how many bytes they occupy. Folders
    are schemas, so the address is still ``lakehouse.<schema>.<table>`` and a table's
    path stays ``schema/table``.
    """
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
            # One query answered for every table; only a table it did not mention (or could
            # not answer for) costs a DESCRIBE of its own.
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

    # A bare name is a convenience, not an address: register it only when exactly one
    # table answers to it, so `SELECT * FROM orders` never silently picks a schema.
    tables = [
        replace(table, bare_alias=counts[table.name.lower()] == 1) for table in tables
    ]

    root = store.display_root() if hasattr(store, "display_root") else ""
    return WarehouseScan(root=root, tables=tuple(tables), problems=tuple(problems))


def table_paths(scan_result: WarehouseScan) -> Iterable[str]:
    """The relative paths of every table in a scan, in order."""
    return (table.path for table in scan_result.tables)
