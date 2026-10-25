"""The catalog is a projection of the warehouse directory.

Nothing registers a table. :func:`scan` walks the files, reads each Parquet footer and
returns what exists; ``catalog_objects`` is refreshed from that answer, so a row can
never outlive its file or describe data nobody wrote. That single rule is what makes a
directory of files a warehouse: the files are the truth, and the catalog, the editor's
views and the UI tree are all derived from them.

A table's address is its path, in three slots::

    orders.parquet                    -> lakehouse.orders
    supabase/table1.parquet           -> lakehouse.supabase.table1
    supabase/auth/table1.parquet      -> lakehouse.supabase.auth.table1

``source`` is the warehouse itself (``lakehouse``) or an attached database's alias, the
folder path is the middle slot, and the file name is the table. SQL identifiers only
carry three parts, so a path deeper than one folder is held in a single quoted schema
name — ``lakehouse."supabase.auth".table1`` — which DuckDB accepts verbatim and which
:func:`sql_name_for` emits. On disk the tree stays exactly as the operator made it;
the joining exists only in the name the engine registers, never in the files.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Iterable, Optional, Sequence

from app.core.storage import FileInfo, WarehouseStorage, normalize_rel, warehouse_storage

SOURCE = "lakehouse"
PARQUET_SUFFIX = ".parquet"

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
    """Walks the warehouse and returns the tables that actually exist.

    This is the only function that decides what a table is: a visible ``*.parquet``
    file whose path is a legal address. Folders need no record to exist, and nothing
    is created here — a scan never writes, so calling it is always safe.
    """
    store = storage or warehouse_storage()
    tables: list[TableFile] = []
    problems: list[str] = []
    claimed: dict[tuple[str, str], str] = {}

    for info in store.list_parquet():
        try:
            folder, name = split_table_path(info.path)
        except InvalidTablePath as exc:
            problems.append(f"{info.path}: {exc}")
            continue

        # SQL identifiers are case-insensitive, so two files that differ only by case
        # would fight over one name. Windows cannot even hold both, but a bucket can.
        key = (folder.lower(), name.lower())
        if key in claimed:
            problems.append(
                f"{info.path}: same table name as {claimed[key]} (names are case-insensitive)"
            )
            continue

        try:
            table = table_from_file(store, info)
        except Exception as exc:  # a corrupt file must not hide the others
            problems.append(f"{info.path}: unreadable Parquet footer ({exc})")
            continue

        claimed[key] = info.path
        tables.append(table)

    tables.sort(key=lambda table: table.path)

    # A bare name is a convenience, not an address: register it only when exactly one
    # table answers to it, so `SELECT * FROM orders` never silently picks a folder.
    names: dict[str, int] = {}
    for table in tables:
        names[table.name.lower()] = names.get(table.name.lower(), 0) + 1
    tables = [
        replace(table, bare_alias=names[table.name.lower()] == 1) for table in tables
    ]

    root = store.display_root() if hasattr(store, "display_root") else ""
    return WarehouseScan(root=root, tables=tuple(tables), problems=tuple(problems))


def table_paths(scan_result: WarehouseScan) -> Iterable[str]:
    """The relative paths of every table in a scan, in order."""
    return (table.path for table in scan_result.tables)
