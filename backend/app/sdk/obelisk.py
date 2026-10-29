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
import re
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
from app.core.catalog import (
    LOAD_TABLE_SUFFIX,
    PARQUET_SUFFIX,
    SOURCE,
    fqn_for,
    path_for,
    split_table_path,
    sql_name_for,
)
from app.core.resource_advisor import advisor
from app.core.storage import warehouse_storage
from app.engines.duckdb_engine import duckdb_engine

logger = logging.getLogger(__name__)

__all__ = [
    "table",
    "query",
    "query_arrow",
    "lazy_query",
    "iter_batches",
    "LazyQueryBuilder",
    "save",
    "materialize_sql",
    "spark",
    "var",
    "TableWrapper",
    "EagerReadTooLarge",
]

_IMPLICIT_DUCKDB_CONN = None


class EagerReadTooLarge(RuntimeError):
    """An eager read was refused because the table is bigger than this machine can hold."""


def _human_bytes(size: float) -> str:
    """A byte count a person can read, without pulling in a formatter dependency."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


def _eager_refuse_bytes() -> int:
    """The refusal threshold: configured, or a quarter of RAM — never under 8 GiB."""
    configured = int(settings.SDK_EAGER_REFUSE_BYTES or 0)
    if configured > 0:
        return configured
    try:
        import psutil

        total_ram = int(psutil.virtual_memory().total)
    except Exception:
        total_ram = 0
    return max(8 * 1024 ** 3, int(total_ram * 0.25))


def _catalog_table_bytes(identifier: str) -> int:
    """Queries DuckLake catalog metadata for exact active Parquet byte size when disk path is empty."""
    from app.core.catalog import scan

    clean = (identifier or "").strip().replace("`", "").replace('"', "")
    if not clean:
        return 0
    stripped = clean[len("lakehouse."):] if clean.lower().startswith("lakehouse.") else clean
    wanted = stripped.replace(".", "/").lower()
    try:
        for entry in scan().tables:
            entry_rel = (
                entry.path[: -len(PARQUET_SUFFIX)]
                if entry.path.lower().endswith(PARQUET_SUFFIX)
                else entry.path
            )
            if entry_rel.lower() == wanted or entry.fqn.lower() == f"lakehouse.{stripped.lower()}":
                return int(entry.size_bytes or 0)
    except Exception:
        return 0
    return 0


def _guard_eager_read(
    identifier: str,
    files: list[Path],
    what: str,
    warn_bytes: Optional[int] = None,
    refuse_bytes: Optional[int] = None,
) -> None:
    """Warns — or refuses — before an eager read pulls a table into memory."""
    try:
        on_disk = sum(path.stat().st_size for path in files if path.exists())
    except OSError:
        on_disk = 0
    if on_disk <= 0 and not files:
        on_disk = _catalog_table_bytes(identifier)
    if on_disk <= 0:
        return

    limit = refuse_bytes if refuse_bytes is not None else _eager_refuse_bytes()
    warn_at = warn_bytes if warn_bytes is not None else int(settings.SDK_EAGER_WARN_BYTES)

    if limit > 0 and on_disk > limit:
        raise EagerReadTooLarge(
            f"{what} on '{identifier}' reads {_human_bytes(on_disk)} of Parquet into memory, "
            f"above this install's {_human_bytes(limit)} guard. Use "
            "`.to_lazy_polars()` (or `obelisk.lazy_query(sql)`) and collect only the "
            "columns and rows you need; set OBELISK_SDK_EAGER_REFUSE_BYTES to change the "
            "limit."
        )
    if warn_at > 0 and on_disk > warn_at:
        logger.warning(
            "%s on '%s' is loading %s into memory; .to_lazy_polars() avoids the copy.",
            what,
            identifier,
            _human_bytes(on_disk),
        )


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
        """The single Parquet file behind this table, when there is one."""
        path = self.file_path
        if path is None:
            return None
        return path if path.is_file() and path.suffix.lower() == ".parquet" else None

    def _parquet_files(self) -> list[Path]:
        """The Parquet files this table's data lives in, including swapped __obelisk_load directories."""
        path = self.file_path
        if path is None:
            return []
        if self._readable_file:
            return [path]
        found: list[Path] = []
        if path.is_dir():
            found.extend(p for p in path.rglob("*.parquet") if p.is_file())
        load_dir = path.parent / f"{path.name}{LOAD_TABLE_SUFFIX}"
        if load_dir.is_dir():
            found.extend(p for p in load_dir.rglob("*.parquet") if p.is_file())
        return found

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
        _guard_eager_read(self.full_identifier, self._parquet_files(), "to_arrow()")
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
        _guard_eager_read(self.full_identifier, self._parquet_files(), "to_polars()")
        if self._readable_file:
            return pl.read_parquet(str(self._readable_file))
        try:
            return query(f"SELECT * FROM {self.full_identifier}")
        except Exception as e:
            raise FileNotFoundError(
                f"Dataset '{self.full_identifier}' not found in the lakehouse or "
                f"attached external sources: {e}"
            ) from e

    def to_lazy_polars(self) -> pl.LazyFrame:
        """A Polars ``LazyFrame`` over this table: nothing is read until ``collect()``."""
        if self._readable_file:
            return pl.scan_parquet(str(self._readable_file))
        return lazy_query(f"SELECT * FROM {self.full_identifier}")

    def to_df(self) -> Any:
        """Reads dataset into a Pandas DataFrame (eager — see :meth:`to_polars`)."""
        _guard_eager_read(self.full_identifier, self._parquet_files(), "to_df()")
        arrow_obj = self.to_arrow()
        if hasattr(arrow_obj, "read_all"):
            arrow_obj = arrow_obj.read_all()
        return arrow_obj.to_pandas(split_blocks=True, self_destruct=True)

    def to_duckdb(self, conn=None):
        """Returns a DuckDB relation referencing the dataset."""
        c = conn or _implicit_connection()
        if self._readable_file:
            return c.from_parquet(self._readable_file.resolve().as_posix())
        for attachment in _attachments_for_sql(self.full_identifier):
            duckdb_engine.attach(c, attachment)
        return c.query(f"SELECT * FROM {self.full_identifier}")

    def iter_batches(
        self, batch_size: int = 100_000, columns: Optional[list[str]] = None
    ):
        """Streams PyArrow RecordBatches out-of-core in O(batch_size) memory."""
        step = max(1, int(batch_size))
        if self._readable_file:
            pf = pq.ParquetFile(str(self._readable_file))
            for batch in pf.iter_batches(batch_size=step, columns=columns):
                yield batch
            return
        proj = (
            ", ".join(f'"{c.replace(chr(34), chr(34)*2)}"' for c in columns)
            if columns
            else "*"
        )
        yield from iter_batches(
            f"SELECT {proj} FROM {self.full_identifier}", batch_size=step
        )

    def sql(self, clause_or_query: str) -> "LazyQueryBuilder":
        """Builds a scoped lazy/streaming SQL query against this table (use ``self`` or ``WHERE ...``)."""
        raw = (clause_or_query or "").strip().rstrip(";")
        if not raw:
            full_sql = f"SELECT * FROM {self.full_identifier}"
        elif re.match(r"^(?:select|with)\b", raw, flags=re.IGNORECASE):
            full_sql = re.sub(
                r"\bself\b", self.full_identifier, raw, flags=re.IGNORECASE
            )
        else:
            full_sql = f"SELECT * FROM {self.full_identifier} {raw}"
        return LazyQueryBuilder(full_sql)

    def to_spark(self, spark_session=None):
        """Converts the dataset to a PySpark DataFrame."""
        spark_sess = spark_session or spark()
        pandas_df = self.to_df()
        return spark_sess.createDataFrame(pandas_df)

    def __repr__(self) -> str:
        status = "exists" if self.exists() else "not_found"
        return f"<ObeliskTable: {self.full_identifier} [{status}] -> {self.full_identifier}>"


class LazyQueryBuilder:
    """Chainable out-of-core SQL builder returned by ``obelisk.table(...).sql(...)``."""

    def __init__(self, sql: str):
        self.sql = sql

    def to_lazy_polars(self) -> pl.LazyFrame:
        return lazy_query(self.sql)

    def iter_batches(self, batch_size: int = 100_000):
        return iter_batches(self.sql, batch_size=batch_size)

    def to_arrow(self) -> pa.Table:
        res = query_arrow(self.sql)
        return res.read_all() if hasattr(res, "read_all") else res

    def to_polars(self) -> pl.DataFrame:
        return query(self.sql)

    def to_df(self) -> Any:
        return self.to_arrow().to_pandas(split_blocks=True, self_destruct=True)


class SourceNotQueryable(RuntimeError):
    """Raised when an identifier names a source that cannot be queried live."""


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
    """Checks registered connections in the SQLite vault to resolve an external table."""
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

            if parts[0].lower() in aliases:
                return clean

            for alias in aliases:
                test_sql = f"SELECT 1 FROM {alias}.{clean} LIMIT 1"
                try:
                    res = query(test_sql)
                    if len(res) >= 0:
                        return f"{alias}.{clean}"
                except Exception:
                    pass

            if len(aliases) == 1:
                return f"{aliases[0]}.{clean}"
    except Exception:
        pass

    return clean


def table(identifier: str) -> TableWrapper:
    """Resolves a warehouse table by its address, or an external table by its FQN."""
    from app.core.catalog import scan
    from app.core.storage import warehouse_storage

    clean_id = identifier.strip().replace("`", "").replace('"', "")
    store = warehouse_storage()
    stripped = (
        clean_id[len("lakehouse."):] if clean_id.lower().startswith("lakehouse.") else clean_id
    )
    wanted = stripped.replace(".", "/").lower()

    result = scan(store)

    # 1. Exact address match: TableFile.path from scan() does not carry .parquet, so strip
    #    .parquet only when present instead of unconditionally slicing 8 characters off.
    for entry in result.tables:
        entry_rel = (
            entry.path[: -len(PARQUET_SUFFIX)]
            if entry.path.lower().endswith(PARQUET_SUFFIX)
            else entry.path
        )
        if entry_rel.lower() == wanted:
            return TableWrapper(
                name=entry.name,
                folder=entry.folder,
                file_path=store.root / entry.path,
                full_identifier=entry.fqn,
            )

    # 2. Registered external source check
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
        return TableWrapper(
            name=parts[-1],
            folder=".".join(parts[:-2]) if len(parts) >= 3 else "main",
            file_path=None,
            full_identifier=_resolve_external_identifier(clean_id),
        )

    # 3. Unambiguous leaf name match
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

    # 4. External resolver fallback
    resolved = _resolve_external_identifier(clean_id)
    return TableWrapper(
        name=parts[-1],
        folder=".".join(parts[:-2]) if len(parts) >= 3 else "main",
        file_path=None,
        full_identifier=resolved,
    )


def _attachments_for_sql(sql: str) -> list:
    """Sources the SQL names that DuckDB can attach live, credentials decrypted."""
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


def _eager_row_limit() -> int:
    """The most rows an eager reader will materialise (0 = no ceiling).

    ``query()`` and ``query_arrow()`` build the whole result in memory, so past the cap
    the right tool is ``iter_batches()`` (streams batches) or ``lazy_query()`` (Polars
    pushes the work down). Failing with that instruction beats the kernel dying.
    """
    try:
        return max(0, int(getattr(settings, "SDK_QUERY_ROW_LIMIT", 1_000_000)))
    except (TypeError, ValueError):
        return 1_000_000


def _refuse_oversized(table: pa.Table, limit: int, reader: str) -> pa.Table:
    """Raises when ``table`` holds more rows than an eager reader may return."""
    if limit and table.num_rows > limit:
        raise RuntimeError(
            f"obelisk.{reader}() would return more than {limit:,} rows. Use "
            "obelisk.iter_batches() or obelisk.lazy_query() for a result this size, "
            "or raise OBELISK_SDK_QUERY_ROW_LIMIT deliberately."
        )
    return table


def query(sql: str) -> pl.DataFrame:
    """Executes SQL query using DuckDB in-process engine and returns a Polars DataFrame."""
    conn = duckdb_engine.get_connection()
    duckdb_engine.begin_heavy()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
        exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
        rel = conn.sql(exec_sql)
        if rel is None:
            return pl.DataFrame()
        limit = _eager_row_limit()
        res = (rel.limit(limit + 1) if limit else rel).arrow()
        table = res.read_all() if hasattr(res, "read_all") else res
        return pl.from_arrow(_refuse_oversized(table, limit, "query"))
    finally:
        conn.close()
        duckdb_engine.end_heavy()


def lazy_query(sql: str) -> pl.LazyFrame:
    """``query()`` without materialising: a ``LazyFrame`` you ``collect()`` when you need it."""
    if not (sql or "").strip():
        raise ValueError("lazy_query() needs a SQL statement.")
    conn = _implicit_connection()
    storage_path = duckdb_engine._ensure_storage()
    for attachment in _attachments_for_sql(sql):
        duckdb_engine.attach(conn, attachment)
    exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
    rel = conn.sql(exec_sql)
    if rel is None:
        return pl.LazyFrame()
    return rel.pl(lazy=True)


def query_arrow(sql: str) -> pa.Table:
    """Executes SQL query using DuckDB in-process engine and returns a PyArrow Table."""
    conn = duckdb_engine.get_connection()
    duckdb_engine.begin_heavy()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
        exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
        rel = conn.sql(exec_sql)
        if rel is None:
            return pa.Table.from_arrays([], names=[])
        limit = _eager_row_limit()
        res = (rel.limit(limit + 1) if limit else rel).arrow()
        table = res.read_all() if hasattr(res, "read_all") else res
        return _refuse_oversized(table, limit, "query_arrow")
    finally:
        conn.close()
        duckdb_engine.end_heavy()


def iter_batches(sql: str, batch_size: int = 100_000):
    """Streams federated SQL results as PyArrow RecordBatches in O(batch_size) RAM."""
    step = max(1, int(batch_size))
    conn = duckdb_engine.get_connection()
    duckdb_engine.begin_heavy()
    try:
        storage_path = duckdb_engine._ensure_storage()
        for attachment in _attachments_for_sql(sql):
            duckdb_engine.attach(conn, attachment)
        exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
        rel = conn.sql(exec_sql)
        if rel is None:
            return
        if hasattr(rel, "fetch_arrow_reader"):
            reader = rel.fetch_arrow_reader(step)
            for batch in reader:
                yield batch
        else:
            arrow_obj = conn.execute(exec_sql).arrow()
            if hasattr(arrow_obj, "read_next_batch"):
                while True:
                    try:
                        yield arrow_obj.read_next_batch()
                    except StopIteration:
                        break
            elif hasattr(arrow_obj, "to_batches"):
                for batch in arrow_obj.to_batches(max_chunksize=step):
                    yield batch
    finally:
        conn.close()
        duckdb_engine.end_heavy()


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
    """Persists a DataFrame as a table in the DuckLake lakehouse."""
    folder, stem = split_table_path(target)
    schema_name = folder.replace("/", ".") if folder else "main"
    rel_path = f"{folder}/{stem}" if folder else stem

    arrow_table = _coerce_to_arrow(df)
    advisor.assert_disk_headroom(settings.duckdb_storage_path)

    from app.core.catalog import quote_ident, table_ingest_lock

    address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"
    # Lock order matches ingestion: the table first, then a heavy slot. Taking them the
    # other way round deadlocks against an ingest that holds the table and waits for a
    # slot this notebook would be holding.
    with table_ingest_lock(schema_name, stem), duckdb_engine.heavy_slot():
        conn = duckdb_engine.get_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
            conn.register("_obelisk_save_src", arrow_table)
            try:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {address} AS SELECT * FROM _obelisk_save_src;"
                )
                from app.core.catalog import record_lakehouse_commit

                record_lakehouse_commit(conn)
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


def materialize_sql(
    sql: str, target: str, on_connection: Optional[Any] = None
) -> Dict[str, Any]:
    """Materializes a SQL query directly into a DuckLake table.

    ``on_connection`` is handed the live connection so a caller (the pipeline
    orchestrator) can interrupt a statement that outlives its timeout.
    """
    from app.core.catalog import quote_ident, table_ingest_lock

    folder, stem = split_table_path(target)
    schema_name = folder.replace("/", ".") if folder else "main"
    rel_path = f"{folder}/{stem}" if folder else stem
    address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"
    advisor.assert_disk_headroom(settings.duckdb_storage_path)

    with table_ingest_lock(schema_name, stem), duckdb_engine.heavy_slot():
        conn = duckdb_engine.get_connection()
        if on_connection is not None:
            on_connection(conn)
        try:
            storage_path = duckdb_engine._ensure_storage()
            for attachment in _attachments_for_sql(sql):
                duckdb_engine.attach(conn, attachment)
            exec_sql = (
                duckdb_engine._prepare_sql_and_views(conn, sql, storage_path)
                .strip()
                .rstrip(";")
            )
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
            conn.execute(f"CREATE OR REPLACE TABLE {address} AS {exec_sql};")
            from app.core.catalog import record_lakehouse_commit

            record_lakehouse_commit(conn)
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

    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if not candidate.is_file():
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
    """Initializes a PySpark session."""
    from pyspark.sql import SparkSession

    remote = remote_url or settings.SPARK_REMOTE_URL
    if remote:
        builder = SparkSession.builder.remote(remote).appName(app_name)
        return builder.getOrCreate() if get_or_create else builder

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
    """Retrieves an Obelisk variable or secret by key."""
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
