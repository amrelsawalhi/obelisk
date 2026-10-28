"""Ingestion writes into the lakehouse catalog.

No run waits for the whole source to arrive before it writes anything.

Postgres is read by DuckDB itself, so one statement reads the source and writes the
lakehouse table: no staging table, no Arrow, no second copy of the rows.

The other drivers stream their cursor in batches and each chunk is committed straight into
the lake as Parquet. The old path put every row of every source into ``TEMP TABLE
staging`` first, which is capped by ``DUCKDB_TEMP_SIZE_LIMIT`` — a 150 GB table could never
finish, whatever the machine's disk held.

A snapshot streams into a load table beside its target and is swapped in with two catalog
operations, so a reader sees the whole previous table or the whole new one, never a
half-written one. An incremental append has nothing to swap — the rows belong in the
target — and since the watermark is read back from the table, a rerun continues exactly
where a failed run stopped.

The one thing an upsert cannot do is stream *into the target*: deduplication is a
question about the whole incoming set at once. It streams into the same kind of load
table and answers that question there, in the lake — a first load is deduplicated into
the clean table that is swapped in, a later one is ``MERGE``d from it. So no write
strategy is bounded by ``DUCKDB_TEMP_SIZE_LIMIT`` any more: a load table is Parquet on the
machine's disk, and the worst case of a deduplication is a sort DuckDB may spill, not a
copy of the source queued in the temp directory.
"""
from __future__ import annotations

import itertools
import json
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence

import httpx
import oracledb
import pyarrow as pa
import pymysql
from pymongo import MongoClient

from app.config import settings
from app.core.catalog import (
    LOAD_TABLE_SUFFIX,
    SOURCE,
    quote_ident,
    table_directory,
    validate_column,
    validate_segment,
)
from app.core.dsn import build_pg_dsn, sql_ident, sql_string, validate_alias
from app.core.host_resolver import resolve_connection_host
from app.core.storage import prune_empty_dirs
from app.engines.duckdb_engine import duckdb_engine

logger = logging.getLogger(__name__)

BATCH_ROWS = 50_000

# Rows buffered before one commit into the lake. Committing every batch would create a
# DuckLake snapshot — and at least one Parquet file — per 50 000 rows, which is thousands
# of files for a 100 M-row table; the cost of that metadata is paid later by every scan.
# The buffer is what memory holds, so it is a row count and not "however much arrives".
STAGE_COMMIT_ROWS = 500_000


def _watermark_is_newer(value: Any, reference: Any) -> bool:
    """Compares two watermark values, numerically when both look numeric.

    Numeric watermarks must not be compared as text — "9" > "10" is True, which
    silently corrupts incremental loads. Dates and other non-numeric values fall
    back to a string/ISO comparison.
    """
    try:
        return float(value) > float(reference)
    except (TypeError, ValueError):
        return str(value) > str(reference)


def _literal(value: Any) -> str:
    """A SQL literal that keeps numbers comparable as numbers."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return sql_string(str(value))


def _arrow_batch(columns: Iterable[str], rows: Sequence[Sequence[Any]]) -> pa.Table:
    """Builds one Arrow batch from column names and row tuples, preserving types.

    ``zip(*rows)`` transposes in C. The per-cell loop it replaces ran once for every cell
    of every batch, which for a 100 M-row table is the difference between an ingest that
    uses the core it was given and one that spends the night in the interpreter.
    """
    names = list(columns)
    if not rows:
        return pa.Table.from_pydict({name: [] for name in names})
    return pa.Table.from_pydict(dict(zip(names, zip(*rows))))


def _cells(row: Sequence[Any]) -> tuple:
    """One driver row with Arrow-friendly values: BSON/JSON objects become text."""
    return tuple(
        json.dumps(value, default=str) if isinstance(value, (dict, list)) else value
        for value in row
    )


def _bt(value: object) -> str:
    """A MySQL identifier: backticks, with any embedded backtick doubled."""
    return "`" + str(value).replace("`", "``") + "`"


class IngestCancelled(RuntimeError):
    """A load stopped because the operator cancelled it.

    Distinct from a failure so the job ends as ``cancelled`` rather than ``failed``, and so
    callers can tell "the rows never arrived" from "the source broke". A cancelled load
    commits nothing: staging raises before the swap, and the load table it was filling is
    dropped by the ``finally`` in :meth:`IngestEngine.ingest_entity`.
    """


class IngestEngine:
    """Stages rows from a source, then writes them to the lakehouse in one statement."""

    # -- addresses ---------------------------------------------------------

    @staticmethod
    def _address(
        target_folder: Optional[str], entity: str, target_name: Optional[str]
    ) -> tuple[str, str]:
        """Where a table lives: ``(schema, table)`` inside the lakehouse catalog.

        A folder is a schema and a table is a table, so ``raw/orders`` stays
        ``lakehouse.raw.orders`` for SQL, the SDK and the docs alike.

        Both halves are validated here rather than in the caller, because this is the one
        place an ingest turns a request into an identifier — and that name ends up inside
        ``CREATE``/``MERGE`` statements. A name that is not plain text is refused with the
        reason, never quietly rewritten.
        """
        requested = (target_folder or "").strip().strip("/\\").replace("\\", "/")
        parts = [
            validate_segment(part, what="folder name")
            for part in requested.split("/")
            if part
        ]
        schema = ".".join(parts) or "main"
        if target_name and target_name.strip():
            stem = validate_segment(target_name, what="table name")
        elif "." in entity:
            stem = validate_segment(entity.split(".", 1)[-1], what="table name")
        else:
            stem = validate_segment(entity, what="table name")
        return schema, stem

    @staticmethod
    def _target_sql(schema: str, table: str) -> str:
        return f"{SOURCE}.{quote_ident(schema)}.{quote_ident(table)}"

    @staticmethod
    def _exists(conn, schema: str, table: str) -> bool:
        """Whether the catalog lists the table.

        Read with ``fetchall`` on purpose: on this DuckDB build a result read with
        ``fetchone`` stays open on the connection, and nothing else may be registered or
        run on it until the result is drained.
        """
        rows = conn.execute(
            f"""
            SELECT 1
            FROM __ducklake_metadata_{SOURCE}.ducklake_table AS t
            JOIN __ducklake_metadata_{SOURCE}.ducklake_schema AS s
              ON s.schema_id = t.schema_id
            WHERE t.end_snapshot IS NULL
              AND s.schema_name = ?
              AND t.table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchall()
        return bool(rows)

    @staticmethod
    def _max_watermark(conn, target: str, column: str):
        """The highest watermark already stored, asked of the table itself."""
        try:
            rows = conn.execute(
                f"SELECT max({quote_ident(column)}) FROM {target}"
            ).fetchall()
        except Exception:
            return None
        if not rows or rows[0][0] is None:
            return None
        return rows[0][0]

    @staticmethod
    def _catalog_row_count(conn, target: str) -> Optional[int]:
        """Rows in a lakehouse table, from DuckLake's own file statistics.

        ``count(*)`` after a load scans the table a second time: on a 100 M-row load that is
        a full extra pass paid only to print a number, and the append path paid it twice.
        DuckLake records each data file's ``record_count``, so summing the live files
        answers the same question without reading a row.

        Returns ``None`` — meaning "count it properly" — when the table has delete files
        (whose removed rows are still in the statistics), when the sum is zero (an empty
        table is indistinguishable from rows DuckLake kept inline in the catalog), and for
        anything that is not a lakehouse table at all.
        """
        parts = target.split(".")
        if len(parts) != 3:
            return None
        _, schema, table = (part.strip('"') for part in parts)

        meta = f"__ducklake_metadata_{SOURCE}"
        try:
            deleted = conn.execute(
                f"""
                SELECT 1
                FROM {meta}.ducklake_delete_file AS del
                JOIN {meta}.ducklake_data_file AS df ON df.data_file_id = del.data_file_id
                JOIN {meta}.ducklake_table AS t ON t.table_id = df.table_id
                JOIN {meta}.ducklake_schema AS s ON s.schema_id = t.schema_id
                WHERE del.end_snapshot IS NULL
                  AND s.schema_name = ? AND t.table_name = ?
                LIMIT 1
                """,
                [schema, table],
            ).fetchall()
            if deleted:
                return None

            rows = conn.execute(
                f"""
                SELECT coalesce(sum(df.record_count), 0)
                FROM {meta}.ducklake_data_file AS df
                JOIN {meta}.ducklake_table AS t ON t.table_id = df.table_id
                JOIN {meta}.ducklake_schema AS s ON s.schema_id = t.schema_id
                WHERE df.end_snapshot IS NULL
                  AND t.end_snapshot IS NULL
                  AND s.schema_name = ?
                  AND t.table_name = ?
                """,
                [schema, table],
            ).fetchall()
        except Exception:
            return None

        if not rows or rows[0][0] is None:
            return None
        total = int(rows[0][0])
        return total if total > 0 else None

    @staticmethod
    def _row_count(conn, target: str) -> int:
        """Rows in the table: from the catalog when it knows, otherwise counted."""
        known = IngestEngine._catalog_row_count(conn, target)
        if known is not None:
            return known
        return int(conn.execute(f"SELECT count(*) FROM {target}").fetchall()[0][0])

    @staticmethod
    def _columns(conn, table: str) -> list[str]:
        return [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]

    # -- staging -----------------------------------------------------------

    @staticmethod
    def _stage_batches(
        conn,
        batches: Iterable[pa.Table],
        sink: str,
        on_batch: Optional[Callable[[int], None]] = None,
        create: bool = False,
        commit_rows: int = STAGE_COMMIT_ROWS,
        should_cancel: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Writes Arrow batches into ``sink``; returns the rows read.

        The rows are committed every ``commit_rows`` rows, so they become Parquet on the
        machine's disk instead of rows waiting in the DuckDB temp directory — which is what
        turns the size of a load into a disk question rather than a
        ``DUCKDB_TEMP_SIZE_LIMIT`` question.

        ``create`` makes the first batch define the sink, which is how a load table is
        started; re-creating it also discards whatever a crashed earlier run left behind.
        Without it the sink must already exist and rows are appended to it.

        ``on_batch`` receives the running total after each batch. One large table is the
        case where a status line with no counter looks wedged, and a streamed load has no
        other signal until its swap.
        """
        total = 0
        pending: list[pa.Table] = []
        pending_rows = 0
        created = False

        def execute_with(chunk: pa.Table, statement: str) -> None:
            conn.register("_obelisk_batch", chunk)
            try:
                # The result is drained on purpose: DuckDB refuses to register the next
                # chunk while a previous statement's result is still open on the
                # connection, and a commit returns one row ("Count") whatever it wrote.
                conn.execute(statement).fetchall()
            finally:
                conn.unregister("_obelisk_batch")

        def flush() -> None:
            nonlocal pending, pending_rows
            if not pending:
                return
            chunk = pending[0] if len(pending) == 1 else pa.concat_tables(pending)
            pending, pending_rows = [], 0
            execute_with(chunk, f"INSERT INTO {sink} SELECT * FROM _obelisk_batch")

        for batch in batches:
            # Checked per batch, not per table: one table can be hundreds of batches, so a
            # cancel that only landed between tables could take hours to be noticed.
            if should_cancel is not None and should_cancel():
                raise IngestCancelled(
                    "Ingestion cancelled: stopped staging before the table was committed."
                )
            if batch.num_rows == 0:
                continue
            if create and not created:
                execute_with(
                    batch,
                    f"CREATE OR REPLACE TABLE {sink} AS SELECT * FROM _obelisk_batch",
                )
                created = True
            else:
                pending.append(batch)
                pending_rows += batch.num_rows
                if pending_rows >= commit_rows:
                    flush()
            total += batch.num_rows
            if on_batch is not None:
                on_batch(total)
        flush()
        return total

    def _batch_sink(
        self,
        *,
        schema: str,
        table: str,
        target: str,
        mode: str,
        exists: bool,
        write_strategy: str,
    ) -> tuple[str, bool, Optional[str]]:
        """Where streamed batches land: ``(sink, create, load_table)``.

        A snapshot and an upsert both stream into a load table beside the target — a
        snapshot because it replaces the target when it is complete, an upsert because
        deduplication needs the incoming rows as a set, and a set read from Parquet in the
        lake is not bounded by ``DUCKDB_TEMP_SIZE_LIMIT`` the way a temp table is.

        An incremental append has nothing to swap and nothing to deduplicate: the rows
        belong in the target, so they go straight there and the load is resumable because
        the watermark is read back from the table.
        """
        if mode == "snapshot" or not exists or write_strategy == "upsert":
            load = f"{table}{LOAD_TABLE_SUFFIX}"
            return self._target_sql(schema, load), True, load
        return target, False, None

    def _prune_load_directory(self, schema: str, directory: str) -> None:
        """Removes the directory a load table leaves behind once it is dropped.

        DuckLake derives a table's data directory from the schema, and a drop does not take
        the directory with it — nor does renaming the load table into place move its
        Parquet, which is why a table that outgrew DuckLake's inlining keeps its files under
        the load table's name. A directory that still holds a file therefore belongs to the
        live table and stays; only an empty one — an inlined load, or a drop whose files
        ``ducklake_cleanup_old_files`` already reclaimed — is scaffolding, and an empty
        directory is what made a table appear in the tree as a folder.

        ``directory`` is the load table's own name, suffix included, because the catalog
        has already forgotten the table by the time this runs — the directory is found on
        disk instead. A nested folder's schema directory is named by DuckLake itself, so
        maintenance's sweep of the whole warehouse is what catches anything this misses.
        """
        root = settings.lakehouse_root_path
        prune_empty_dirs(root, table_directory(root, schema, directory))

    def _swap_into_place(self, conn, *, schema: str, staged: str, target: str, table: str) -> None:
        """Makes a finished load table the live one, atomically.

        ``DROP`` and ``RENAME`` are catalog operations inside one DuckLake transaction: a
        reader sees the whole previous table or the whole new one, never a half-written
        one — and not a single row is read back through Python to do it.
        """
        try:
            conn.execute("BEGIN TRANSACTION;")
            conn.execute(f"DROP TABLE IF EXISTS {target};")
            conn.execute(f"ALTER TABLE {staged} RENAME TO {quote_ident(table)};")
            conn.execute("COMMIT;")
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except Exception as rollback_error:
                logger.warning(
                    "Rolling back the swap of %s failed as well: %s", target, rollback_error
                )
            raise
        # The load table's name is gone. Its directory may be empty — an inlined load — or
        # it may hold the Parquet the rename did not move, which is the live table's.
        self._prune_load_directory(schema, f"{table}{LOAD_TABLE_SUFFIX}")

    def _drop_load_table(self, conn, *, schema: str, table: str) -> None:
        """Removes a load table an earlier run left behind, before this run takes the name.

        A ``finally`` cannot run when the process is killed outright — SIGKILL, an OOM
        kill, a machine losing power — so a retry can find the previous attempt's load
        table still in the catalog. Dropping it first is what makes the retry start from
        nothing instead of writing today's rows into yesterday's half-written load.
        """
        if self._exists(conn, schema, table):
            logger.warning(
                "Dropping %s, left behind by an interrupted load.",
                self._target_sql(schema, table),
            )
        conn.execute(f"DROP TABLE IF EXISTS {self._target_sql(schema, table)};")
        # The drop takes the table out of the catalog; the directory it used stays on disk.
        self._prune_load_directory(schema, table)

    @staticmethod
    def _require_primary_key(write_strategy: str, primary_key: Optional[str]) -> None:
        """Refuses an upsert that cannot be deduplicated, before any rows are read."""
        if write_strategy == "upsert" and not primary_key:
            raise ValueError(
                "write_strategy='upsert' requires a primary_key; refusing to write rows "
                "that cannot be deduplicated."
            )

    @staticmethod
    def _raise_if_cancelled(should_cancel: Optional[Callable[[], bool]]) -> None:
        """The one check every stage of a load shares, so the message stays consistent."""
        if should_cancel is not None and should_cancel():
            raise IngestCancelled(
                "Ingestion cancelled: stopped before the table was committed."
            )

    def _stage(
        self,
        conn,
        *,
        source_type: str,
        connection_params: Dict[str, Any],
        entity: str,
        mode: str,
        watermark_column: Optional[str],
        watermark_value,
        limit: Optional[int],
        on_batch: Optional[Callable[[int], None]] = None,
        sink: Optional[str] = None,
        create: bool = False,
        should_cancel: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Lets the driver write into ``sink``; returns the rows extracted.

        Postgres is the exception: DuckDB reads it directly, so it only reaches this method
        for an upsert — the one case whose rows have to be deduplicated before they are
        written, and which therefore land in a load table first.
        """
        common = {
            "conn": conn,
            "connection_params": connection_params,
            "entity": entity,
            "mode": mode,
            "watermark_column": watermark_column,
            "watermark_value": watermark_value,
            "limit": limit,
            "on_batch": on_batch,
            "sink": sink,
            "create": create,
        }
        st = source_type.lower()
        if st == "postgres":
            return self._stage_postgres(**common)
        if st in ("mysql", "mariadb"):
            return self._stage_mysql(**common, should_cancel=should_cancel)
        if st in ("mongodb", "mongo"):
            return self._stage_mongodb(**common, should_cancel=should_cancel)
        if st == "oracle":
            return self._stage_oracle(**common, should_cancel=should_cancel)
        if st in ("api", "rest", "rest_api", "erp"):
            return self._stage_api(**common, should_cancel=should_cancel)
        raise ValueError(f"Unsupported source type '{source_type}' for ingestion.")

    # -- writing the staged rows ------------------------------------------

    def _upsert_from_load(
        self,
        conn,
        *,
        schema: str,
        staged: str,
        target: str,
        table: str,
        mode: str,
        exists: bool,
        primary_key: str,
        watermark_column: Optional[str],
    ) -> bool:
        """Turns a load table into the table the upsert asked for.

        Returns True when the load table *became* the target (it was renamed, so there is
        nothing left to clean up) and False when it was merged into a target that already
        existed and still has to be dropped.

        A first load has nothing to merge into: one row per key is selected where the rows
        lie and the result is renamed onto the target, the same atomic swap a snapshot uses.
        A later load is a delta, so it is deduplicated and ``MERGE``d — one statement, so
        the size of the existing table never becomes a Python object.
        """
        deduped = self._deduped(primary_key, watermark_column, relation=staged)

        if mode == "snapshot" or not exists:
            # Deduplicating in place is one statement over one name: the load table is read
            # and rewritten as the clean table, so nothing else has to be tracked to the
            # swap that consumes it.
            conn.execute(f"CREATE OR REPLACE TABLE {staged} AS {deduped}")
            self._swap_into_place(
                conn, schema=schema, staged=staged, target=target, table=table
            )
            return True

        columns = [c for c in self._columns(conn, target) if c != "_obelisk_rank"]
        key = quote_ident(primary_key)
        assignments = ", ".join(
            f"{quote_ident(c)} = s.{quote_ident(c)}" for c in columns if c != primary_key
        )
        insert_cols = ", ".join(quote_ident(c) for c in columns)
        source_cols = ", ".join(f"s.{quote_ident(c)}" for c in columns)
        # Deduplicating the inbound rows is not enough: the *existing* row has a watermark
        # too, and an incoming row with no watermark must never beat it. That is the failure
        # the old Arrow path shipped (audit F-02), so the condition lives in the statement
        # where it cannot be forgotten.
        guard = ""
        if watermark_column:
            wm = quote_ident(watermark_column)
            guard = (
                f"AND (t.{wm} IS NULL "
                f"OR (s.{wm} IS NOT NULL "
                f"AND s.{wm} >= t.{wm})) "
            )
        # A table whose only column is its key has nothing to set; MERGE would be a syntax
        # error rather than the no-op that row already is.
        matched = (
            f"WHEN MATCHED {guard}THEN UPDATE SET {assignments}" if assignments else ""
        )
        conn.execute(
            f"""
            MERGE INTO {target} AS t
            USING ({deduped}) AS s
            ON t.{key} = s.{key}
            {matched}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})
            """
        )
        return False

    @staticmethod
    def _deduped(primary_key: str, watermark_column: Optional[str], relation: str) -> str:
        """One row per key from ``relation``, newest first.

        ``NULLS LAST`` matters: a row whose watermark is missing must never outrank a row
        that has one — that is exactly how the old Arrow path lost updates (audit F-02).
        """
        order = f"{quote_ident(watermark_column)} DESC NULLS LAST" if watermark_column else "1"
        return f"""
            SELECT * EXCLUDE (_obelisk_rank) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {quote_ident(primary_key)} ORDER BY {order}
                ) AS _obelisk_rank
                FROM {relation}
            ) WHERE _obelisk_rank = 1
        """

    # -- entry point -------------------------------------------------------

    def ingest_entity(
        self,
        source_type: str,
        connection_params: Dict[str, Any],
        entity: str,
        mode: str = "snapshot",  # "snapshot" or "incremental"
        watermark_column: Optional[str] = None,
        target_name: Optional[str] = None,
        target_folder: Optional[str] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",  # "append" or "upsert"
        primary_key: Optional[str] = None,
        on_batch: Optional[Callable[[int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
        on_connection: Optional[Callable[[Any], None]] = None,
    ) -> Dict[str, Any]:
        """Ingests one entity into the lakehouse.

        The path depends on what the source can do. Postgres is transferred by one
        statement that DuckDB executes end to end. Every other driver streams batches into
        the lake as they arrive: a snapshot into a load table that replaces the target when
        it is complete, an incremental append straight into the target, an upsert into a
        load table that is deduplicated and then either swapped in or merged from.

        ``on_batch`` is called with the running row count as rows land, so a caller can
        report progress on a table large enough to look stuck.

        ``should_cancel`` is polled between batches: a load of one large table had no
        cancellation point at all, so "cancel" only took effect once the table it was
        writing finished. ``on_connection`` hands the live DuckDB connection to the caller
        once, which is how a single-statement transfer — the one with no batches to poll —
        is interrupted instead.
        """
        schema, table_stem = self._address(target_folder, entity, target_name)
        target = self._target_sql(schema, table_stem)

        # Both are column identifiers this code writes into the MERGE and the source
        # filter, so they follow the column rule rather than the table rule (no dots).
        if watermark_column:
            watermark_column = validate_column(watermark_column, what="watermark column")
        if primary_key:
            primary_key = validate_column(primary_key, what="primary key")
        self._require_primary_key(write_strategy, primary_key)

        conn = duckdb_engine.get_connection()
        if on_connection is not None:
            on_connection(conn)
        load_table: Optional[str] = None
        try:
            self._raise_if_cancelled(should_cancel)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema)};")
            exists = self._exists(conn, schema, table_stem)

            watermark_value = None
            if mode == "incremental" and watermark_column and exists:
                watermark_value = self._max_watermark(conn, target, watermark_column)

            if source_type.lower() == "postgres" and write_strategy != "upsert":
                # DuckDB reads Postgres itself, so the lakehouse table is written by the
                # same statement: no load table, no Arrow, no second copy of 150 GB.
                extracted = self._transfer_postgres(
                    conn,
                    connection_params=connection_params,
                    entity=entity,
                    mode=mode,
                    watermark_column=watermark_column,
                    watermark_value=watermark_value,
                    limit=limit,
                    target=target,
                    exists=exists,
                    on_batch=on_batch,
                )
            else:
                sink, create, load_table = self._batch_sink(
                    schema=schema,
                    table=table_stem,
                    target=target,
                    mode=mode,
                    exists=exists,
                    write_strategy=write_strategy,
                )
                if load_table is not None:
                    # A run killed outright could not clean up after itself, and this run
                    # is about to take the same name.
                    self._drop_load_table(conn, schema=schema, table=load_table)
                extracted = self._stage(
                    conn,
                    source_type=source_type,
                    connection_params=connection_params,
                    entity=entity,
                    mode=mode,
                    watermark_column=watermark_column,
                    watermark_value=watermark_value,
                    limit=limit,
                    on_batch=on_batch,
                    sink=sink,
                    create=create,
                    should_cancel=should_cancel,
                )
                # Nothing published after a cancel: the load table is dropped in `finally`,
                # and the target keeps exactly the rows it had before the run started.
                self._raise_if_cancelled(should_cancel)
                if extracted and load_table is not None:
                    if write_strategy == "upsert":
                        if self._upsert_from_load(
                            conn,
                            schema=schema,
                            staged=sink,
                            target=target,
                            table=table_stem,
                            mode=mode,
                            exists=exists,
                            primary_key=primary_key,
                            watermark_column=watermark_column,
                        ):
                            # Renamed onto the target: nothing left under the loaded name.
                            load_table = None
                    else:
                        # The whole snapshot is on disk; two catalog operations make it the
                        # table everyone else already knows.
                        self._swap_into_place(
                            conn, schema=schema, staged=sink, target=target, table=table_stem
                        )
                        load_table = None

            if extracted == 0:
                # Nothing arrived. An existing table is left alone on purpose: replacing
                # a table with an empty one because a source was briefly empty loses data
                # that a rerun will never bring back.
                if mode == "snapshot" and exists:
                    logger.warning(
                        "Snapshot of %s returned no rows; keeping the existing table.",
                        target,
                    )
                total = self._row_count(conn, target) if exists else 0
            else:
                total = self._row_count(conn, target)
        finally:
            abandoned = load_table
            if load_table is not None:
                # Either the rows were merged into a target that already holds them, so
                # there is nothing left to swap, or the load failed (or was cancelled)
                # part-way and the rows it committed are worthless on their own. Either
                # way the load table goes: no run leaves one behind for a reader, a backup
                # or the next retry to trip over.
                try:
                    conn.execute(
                        f"DROP TABLE IF EXISTS {self._target_sql(schema, load_table)};"
                    )
                except Exception as cleanup_error:
                    logger.warning(
                        "Could not drop the abandoned load table %s: %s",
                        load_table,
                        cleanup_error,
                    )
                    abandoned = None
            conn.close()
            if abandoned is not None:
                # After the close, so the files the connection had open are released and
                # the directory the dropped table used can actually be removed.
                self._prune_load_directory(schema, abandoned)

        return {
            "status": "success",
            "entity": entity,
            "target": table_stem,
            "target_folder": "" if schema == "main" else schema,
            "address": target,
            "num_rows": total,
            "new_rows_extracted": extracted,
            "file_path": target,
            "mode": mode,
            "write_strategy": write_strategy,
        }

    # -- drivers -----------------------------------------------------------

    def _postgres_source(
        self, conn, connection_params, entity, mode, watermark_column, watermark_value, limit
    ) -> tuple[str, str, str, str]:
        """Attaches the Postgres database: ``(alias, relation, where, limit_clause)``.

        The alias is random per run so two loads of one source cannot collide on a shared
        connection, and the caller owns detaching it.
        """
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        dsn = build_pg_dsn(
            host=host,
            port=int(connection_params["port"]),
            dbname=connection_params["database"],
            user=connection_params.get("username") or connection_params.get("user"),
            password=connection_params.get("password"),
            sslmode=connection_params.get("sslmode", "disable"),
            connect_timeout=8,
        )
        alias = validate_alias(f"pg_src_{uuid.uuid4().hex[:6]}")
        conn.execute(f"ATTACH {sql_string(dsn)} AS {alias} (TYPE POSTGRES);")
        if "." in entity:
            schema_name, table_name = entity.split(".", 1)
        else:
            schema_name, table_name = "public", entity
            try:
                found = conn.execute(
                    f"SELECT table_schema FROM information_schema.tables "
                    f"WHERE table_catalog = {sql_string(alias)} AND table_name = {sql_string(table_name)} "
                    f"ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END LIMIT 1"
                ).fetchone()
                if found and found[0]:
                    schema_name = found[0]
            except Exception:
                pass
        where = ""
        if mode == "incremental" and watermark_column and watermark_value is not None:
            where = f"WHERE {sql_ident(watermark_column)} > {_literal(watermark_value)}"
        limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
        relation = f"{alias}.{sql_ident(schema_name)}.{sql_ident(table_name)}"
        return alias, relation, where, limit_clause

    def _transfer_from(
        self,
        conn=None,
        *,
        source_ref: str,
        target: str,
        mode: str,
        exists: bool,
        where: str = "",
        limit_clause: str = "",
        on_batch: Optional[Callable[[int], None]] = None,
        **kwargs,
    ) -> int:
        """Moves rows from a relation DuckDB can scan into the lakehouse, in one statement.

        Nothing passes through Python: DuckDB reads the source and writes DuckLake's
        Parquet files directly, so the cost of the load is one statement rather than one
        Python object per row.
        """
        c = conn if conn is not None else kwargs.get("conn")
        if c is None:
            raise ValueError("conn is required for _transfer_from")
        if mode == "snapshot" or not exists:
            # An empty source must not replace a table that has rows: a source that is
            # briefly empty would otherwise destroy data a rerun cannot bring back.
            probe = c.execute(
                f"SELECT 1 FROM {source_ref} {where} LIMIT 1"
            ).fetchall()
            if not probe:
                return 0
            c.execute(
                f"CREATE OR REPLACE TABLE {target} AS "
                f"SELECT * FROM {source_ref} {where}{limit_clause};"
            )
            written = self._row_count(c, target)
        else:
            before = self._row_count(c, target)
            c.execute(
                f"INSERT INTO {target} SELECT * FROM {source_ref} {where}{limit_clause};"
            )
            written = max(0, self._row_count(c, target) - before)
        if on_batch is not None:
            on_batch(written)
        return written

    def _transfer_postgres(
        self, conn=None, *, connection_params, entity, mode, watermark_column, watermark_value,
        limit, target, exists, on_batch=None, **kwargs,
    ) -> int:
        """Copies a Postgres table straight into the lakehouse.

        DuckDB's Postgres scanner feeds DuckLake's Parquet writer, so each row is written
        once, to disk, and never becomes a Python object or a second copy in the temp
        directory.
        """
        c = conn if conn is not None else kwargs.get("conn")
        if c is None:
            raise ValueError("conn is required for _transfer_postgres")
        alias, relation, where, limit_clause = self._postgres_source(
            c, connection_params, entity, mode, watermark_column, watermark_value, limit
        )
        try:
            return self._transfer_from(
                c,
                source_ref=relation,
                target=target,
                mode=mode,
                exists=exists,
                where=where,
                limit_clause=limit_clause,
                on_batch=on_batch,
            )
        finally:
            c.execute(f"DETACH {alias};")

    def _stage_postgres(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False,
    ) -> int:
        """Postgres into ``sink``, in one statement DuckDB executes end to end.

        Only an upsert reaches this method, because its rows land in a load table before
        they are deduplicated; every other Postgres load goes through
        :meth:`_transfer_postgres`, straight to its target. Either way DuckDB reads the
        source and writes the lakehouse, and no row becomes a Python object. The sink never
        exists yet, so ``create`` is implied by the transfer itself.
        """
        if sink is None:
            raise ValueError("A Postgres load needs a sink: DuckDB writes it in one statement.")
        alias, relation, where, limit_clause = self._postgres_source(
            conn, connection_params, entity, mode, watermark_column, watermark_value, limit
        )
        try:
            return self._transfer_from(
                conn,
                source_ref=relation,
                target=sink,
                mode="snapshot",
                exists=False,
                where=where,
                limit_clause=limit_clause,
                on_batch=on_batch,
            )
        finally:
            conn.execute(f"DETACH {alias};")

    def _stage_mysql(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
    ) -> int:
        """MySQL/MariaDB: an unbuffered cursor, streamed into the sink batch by batch.

        ``DictCursor`` is the wrong cursor here: PyMySQL reads the *entire* result set into
        client memory inside ``execute()``, so ``fetchmany`` never bounded anything. The
        server-side cursor reads one batch off the socket at a time and hands back tuples,
        which is also one less dict per row to build.
        """
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        connection = pymysql.connect(
            host=host,
            port=int(connection_params["port"]),
            user=connection_params.get("username"),
            password=connection_params.get("password"),
            database=connection_params["database"],
            cursorclass=pymysql.cursors.SSCursor,
            charset="utf8mb4",
        )
        try:
            if "." in entity:
                db_name, table_name = entity.split(".", 1)
                table_ref = f"{_bt(db_name)}.{_bt(table_name)}"
            else:
                table_ref = _bt(entity)

            limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
            with connection.cursor() as cur:
                if mode == "incremental" and watermark_column and watermark_value is not None:
                    cur.execute(
                        f"SELECT * FROM {table_ref} "
                        f"WHERE {_bt(watermark_column)} > %s "
                        f"ORDER BY {_bt(watermark_column)} ASC{limit_clause}",
                        (watermark_value,),
                    )
                else:
                    cur.execute(f"SELECT * FROM {table_ref}{limit_clause}")
                columns = [desc[0] for desc in cur.description] if cur.description else []

                def batches() -> Iterator[pa.Table]:
                    while True:
                        rows = cur.fetchmany(BATCH_ROWS)
                        if not rows:
                            return
                        yield _arrow_batch(columns, [_cells(row) for row in rows])

                return self._stage_batches(
                    conn, batches(), on_batch=on_batch, sink=sink, create=create,
                    should_cancel=should_cancel,
                )
        finally:
            connection.close()

    def _stage_mongodb(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
    ) -> int:
        """MongoDB: documents in batches, BSON types kept (dates, ints, floats)."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        client = MongoClient(
            host=host,
            port=int(connection_params["port"]),
            username=connection_params.get("username") or None,
            password=connection_params.get("password") or None,
            authSource=(
                connection_params.get("auth_db", "admin")
                if (connection_params.get("username") and connection_params.get("password"))
                else None
            ),
            serverSelectionTimeoutMS=5000,
        )
        try:
            if "." in entity:
                db_override, coll_name = entity.split(".", 1)
                collection = client[db_override][coll_name]
            else:
                collection = client[connection_params["database"]][entity]

            query_filter = {}
            if mode == "incremental" and watermark_column and watermark_value is not None:
                query_filter = {watermark_column: {"$gt": watermark_value}}

            cursor = collection.find(query_filter)
            if limit and limit > 0:
                cursor = cursor.limit(int(limit))

            def batches() -> Iterator[pa.Table]:
                while True:
                    docs = list(itertools.islice(cursor, BATCH_ROWS))
                    if not docs:
                        return
                    keys: list[str] = sorted({key for doc in docs for key in doc})
                    rows = []
                    for doc in docs:
                        values = []
                        for key in keys:
                            value = doc.get(key)
                            if str(type(value)).find("ObjectId") != -1:
                                value = str(value)
                            elif isinstance(value, (dict, list)):
                                value = json.dumps(value, default=str)
                            values.append(value)
                        rows.append(tuple(values))
                    yield _arrow_batch(keys, rows)

            return self._stage_batches(
                conn, batches(), on_batch=on_batch, sink=sink, create=create,
                should_cancel=should_cancel,
            )
        finally:
            client.close()

    def _stage_oracle(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
    ) -> int:
        """Oracle: cursor batches, column names lowercased as the rest of the platform is."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        dsn = f"{host}:{int(connection_params['port'])}/{connection_params['database']}"
        connection = oracledb.connect(
            user=connection_params.get("username"),
            password=connection_params.get("password"),
            dsn=dsn,
            tcp_connect_timeout=5.0,
        )
        try:
            if "." in entity:
                schema_name, table_name = entity.split(".", 1)
                table_ref = f"{sql_ident(schema_name)}.{sql_ident(table_name)}"
            else:
                table_ref = sql_ident(entity)

            limit_clause = f" FETCH FIRST {int(limit)} ROWS ONLY" if limit and limit > 0 else ""
            cur = connection.cursor()
            if mode == "incremental" and watermark_column and watermark_value is not None:
                cur.execute(
                    f"SELECT * FROM {table_ref} WHERE {sql_ident(watermark_column)} > :val "
                    f"ORDER BY {sql_ident(watermark_column)} ASC{limit_clause}",
                    [watermark_value],
                )
            else:
                cur.execute(f"SELECT * FROM {table_ref}{limit_clause}")

            columns = [desc[0].lower() for desc in cur.description] if cur.description else []

            def batches() -> Iterator[pa.Table]:
                while True:
                    rows = cur.fetchmany(BATCH_ROWS)
                    if not rows:
                        return
                    yield _arrow_batch(columns, [_cells(row) for row in rows])

            return self._stage_batches(
                conn, batches(), on_batch=on_batch, sink=sink, create=create,
                should_cancel=should_cancel,
            )
        finally:
            connection.close()

    def _stage_api(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
    ) -> int:
        """A REST endpoint returns one document, so it is staged in one batch."""
        from app.engines.introspector import Introspector

        base_url = (
            connection_params.get("host") or connection_params.get("url") or "http://localhost"
        ).strip()
        if not base_url.startswith(("http://", "https://")):
            port = connection_params.get("port")
            base_url = (
                f"http://{base_url}:{port}"
                if port and port not in (80, 443)
                else f"http://{base_url}"
            )

        endpoint = entity if entity != "api_records" else ""
        full_url = (
            f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else base_url
        )

        options = connection_params.get("options") or {}
        token = connection_params.get("password") or options.get("token") or options.get("api_key")
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if options.get("headers"):
            headers.update(options["headers"])

        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            payload = response.json()

        records = Introspector._extract_api_records(payload, options.get("data_key"))

        if mode == "incremental" and watermark_column and watermark_value is not None:
            records = [
                record
                for record in records
                if record.get(watermark_column) is not None
                and _watermark_is_newer(record[watermark_column], watermark_value)
            ]
        if limit and limit > 0:
            records = records[: int(limit)]
        if not records:
            return 0

        columns: list[str] = sorted({key for record in records for key in record})
        rows = []
        for record in records:
            values = []
            for column in columns:
                value = record.get(column)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, default=str)
                values.append(value)
            rows.append(tuple(values))
        return self._stage_batches(
            conn, [_arrow_batch(columns, rows)], on_batch=on_batch, sink=sink, create=create,
            should_cancel=should_cancel,
        )


ingest_engine = IngestEngine()




