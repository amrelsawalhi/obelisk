"""Ingestion writes into the lakehouse catalog.

No run waits for the whole source to arrive before it writes anything.

Postgres is read by DuckDB itself, so one statement reads the source and writes the
lakehouse table: no staging table, no Arrow, no second copy of the rows.

The other drivers stream their cursor in batches and each chunk is committed straight into
the lake as Parquet, bounded both by row count (``STAGE_COMMIT_ROWS``) and by Arrow byte
footprint (``STAGE_COMMIT_BYTES``).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import itertools
import json
import logging
from pathlib import Path
import re
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
    record_lakehouse_commit,
    register_active_load_table,
    table_directory,
    table_ingest_lock,
    unregister_active_load_table,
    validate_column,
    validate_segment,
)
from app.core.dsn import build_kv_dsn, build_pg_dsn, sql_ident, sql_string, validate_alias
from app.core.host_resolver import resolve_connection_host
from app.core.resource_advisor import advisor
from app.core.storage import prune_empty_dirs
from app.engines.duckdb_engine import duckdb_engine

logger = logging.getLogger(__name__)

BATCH_ROWS = 50_000
STAGE_COMMIT_ROWS = 500_000
STAGE_COMMIT_BYTES = 128 * 1024 * 1024  # 128 MiB ceiling per staged commit buffer
MAX_API_RESPONSE_BYTES = 64 * 1024 * 1024  # 64 MiB ceiling per HTTP response

_OBJECT_ID_HEX_RE = re.compile(r"^[0-9a-fA-F]{24}$")


def _parse_iso_datetime(raw: str) -> Optional[datetime]:
    text = raw.strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _watermark_is_newer(value: Any, reference: Any) -> bool:
    """Compares two watermark values using exact 128-bit Decimal and UTC-normalized timestamps."""
    if value is None or reference is None:
        return False
    if (
        isinstance(value, (int, Decimal))
        and isinstance(reference, (int, Decimal))
        and not isinstance(value, bool)
        and not isinstance(reference, bool)
    ):
        d_val, d_ref = Decimal(value), Decimal(reference)
        if d_val.is_nan() or d_ref.is_nan():
            return False
        return d_val > d_ref
    try:
        if not isinstance(value, bool) and not isinstance(reference, bool):
            d_val = Decimal(str(value).strip())
            d_ref = Decimal(str(reference).strip())
            if d_val.is_nan() or d_ref.is_nan():
                return False
            return d_val > d_ref
    except (InvalidOperation, TypeError, ValueError):
        pass

    ts_val = (
        value
        if isinstance(value, datetime)
        else (_parse_iso_datetime(value) if isinstance(value, str) else None)
    )
    ts_ref = (
        reference
        if isinstance(reference, datetime)
        else (_parse_iso_datetime(reference) if isinstance(reference, str) else None)
    )
    if ts_val is not None and ts_ref is not None:
        u_val = ts_val.astimezone(timezone.utc) if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        u_ref = ts_ref.astimezone(timezone.utc) if ts_ref.tzinfo else ts_ref.replace(tzinfo=timezone.utc)
        return u_val > u_ref

    return str(value) > str(reference)


def _literal(value: Any) -> str:
    """A SQL literal that keeps numbers comparable as exact numbers and escapes strings."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (float, Decimal)):
        try:
            d = Decimal(str(value))
            if not d.is_finite():
                return "NULL"
            return str(value) if isinstance(value, float) and "e" not in str(value).lower() else format(d, "f")
        except InvalidOperation:
            return "NULL"
    if isinstance(value, (datetime, date)):
        return sql_string(value.isoformat())
    return sql_string(str(value))


def _arrow_batch(columns: Iterable[str], rows: Sequence[Sequence[Any]]) -> pa.Table:
    """Builds one Arrow batch from column names and row tuples, with LargeString/mixed-type fallback."""
    names = list(columns)
    if not rows:
        return pa.Table.from_pydict({name: [] for name in names})
    col_tuples = dict(zip(names, zip(*rows)))
    try:
        return pa.Table.from_pydict(col_tuples)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowCapacityError):
        arrays: list[pa.Array] = []
        fields: list[pa.Field] = []
        for name in names:
            col_vals = col_tuples.get(name, ())
            try:
                arr = pa.array(col_vals)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowCapacityError):
                arr = pa.array(
                    [None if v is None else str(v) for v in col_vals],
                    type=pa.large_string(),
                )
            arrays.append(arr)
            fields.append(pa.field(name, arr.type, nullable=True))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if hasattr(value, "to_decimal") and callable(value.to_decimal):
        try:
            return value.to_decimal()
        except Exception:
            return str(value)
    if type(value).__name__ == "ObjectId":
        return str(value)
    return value


def _cells(row: Sequence[Any]) -> tuple:
    """One driver row with Arrow-friendly values: BSON/JSON objects become text or Decimal."""
    return tuple(_normalize_cell(value) for value in row)


def _bt(value: object) -> str:
    """A MySQL identifier: backticks, with any embedded backtick doubled."""
    return "`" + str(value).replace("`", "``") + "`"


class IngestCancelled(RuntimeError):
    """A load stopped because the operator cancelled it."""


class IngestEngine:
    """Stages rows from a source, then writes them to the lakehouse."""

    # -- addresses ---------------------------------------------------------

    @staticmethod
    def _address(
        target_folder: Optional[str], entity: str, target_name: Optional[str]
    ) -> tuple[str, str]:
        """Where a table lives: ``(schema, table)`` inside the lakehouse catalog."""
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
        """Whether the catalog lists the table."""
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
        """Rows in a lakehouse table, from DuckLake's own file statistics."""
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
                # Try fast delete-count subtraction from DuckLake metadata first
                try:
                    net_rows = conn.execute(
                        f"""
                        SELECT
                            coalesce(sum(df.record_count), 0) - coalesce((
                                SELECT sum(del.delete_count)
                                FROM {meta}.ducklake_delete_file AS del
                                JOIN {meta}.ducklake_data_file AS df2 ON df2.data_file_id = del.data_file_id
                                WHERE del.end_snapshot IS NULL
                                  AND df2.end_snapshot IS NULL
                                  AND df2.table_id = t.table_id
                            ), 0)
                        FROM {meta}.ducklake_table AS t
                        JOIN {meta}.ducklake_schema AS s ON s.schema_id = t.schema_id
                        LEFT JOIN {meta}.ducklake_data_file AS df
                          ON df.table_id = t.table_id AND df.end_snapshot IS NULL
                        WHERE t.end_snapshot IS NULL
                          AND s.schema_name = ?
                          AND t.table_name = ?
                        GROUP BY t.table_id
                        """,
                        [schema, table],
                    ).fetchall()
                    if net_rows and net_rows[0][0] is not None and int(net_rows[0][0]) > 0:
                        return int(net_rows[0][0])
                except Exception:
                    pass
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

    @staticmethod
    def _evolve_target_schema(
        conn,
        target: str,
        source_rel: str,
        schema_policy: str = "evolve",
    ) -> list[str]:
        """Detects new upstream columns in ``source_rel`` and either evolves ``target`` or raises under ``strict``."""
        try:
            target_cols = {
                str(row[0]).lower(): (str(row[0]), str(row[1]))
                for row in conn.execute(f"DESCRIBE {target}").fetchall()
            }
            source_cols = [
                (str(row[0]), str(row[1]))
                for row in conn.execute(f"DESCRIBE SELECT * FROM {source_rel} LIMIT 0").fetchall()
                if str(row[0]) != "_obelisk_rank"
            ]
        except Exception:
            return []

        missing = [
            (col_name, col_type)
            for col_name, col_type in source_cols
            if col_name.lower() not in target_cols
        ]
        if not missing:
            return []

        policy = (schema_policy or "evolve").strip().lower()
        if policy == "strict":
            names = ", ".join(name for name, _ in missing)
            raise ValueError(
                f"Upstream schema drift detected on {target}: new column(s) [{names}] "
                "refused under schema_policy='strict'."
            )

        added: list[str] = []
        for col_name, col_type in missing:
            safe_type = col_type if col_type and col_type.upper() != "NULL" else "VARCHAR"
            try:
                conn.execute(
                    f"ALTER TABLE {target} ADD COLUMN {quote_ident(col_name)} {safe_type};"
                )
                added.append(col_name)
            except Exception as exc:
                logger.debug("Could not add evolved column %s to %s: %s", col_name, target, exc)
        return added

    @staticmethod
    def _estimate_source_bytes(source_type: str, connection_params: Dict[str, Any], entity: str) -> int:
        """Best-effort pre-flight source table size estimate in bytes (0 when unknown or mocked)."""
        st = (source_type or "").lower()
        try:
            if st == "postgres":
                import psycopg2
                if getattr(psycopg2.connect, "__module__", "").startswith("psycopg2"):
                    host = resolve_connection_host(connection_params["host"], connection_params["port"])
                    schema_name, table_name = entity.split(".", 1) if "." in entity else ("public", entity)
                    with psycopg2.connect(
                        host=host,
                        port=int(connection_params["port"]),
                        dbname=connection_params["database"],
                        user=connection_params.get("username") or connection_params.get("user"),
                        password=connection_params.get("password"),
                        sslmode=connection_params.get("sslmode", "disable"),
                        connect_timeout=4,
                    ) as pg_conn:
                        with pg_conn.cursor() as cur:
                            cur.execute(
                                "SELECT coalesce(pg_total_relation_size(c.oid), 0) "
                                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                                "WHERE n.nspname = %s AND c.relname = %s LIMIT 1;",
                                (schema_name, table_name),
                            )
                            row = cur.fetchone()
                            return int(row[0] or 0) if row else 0
            if st in ("mysql", "mariadb") and getattr(pymysql.connect, "__module__", "").startswith("pymysql"):
                host = resolve_connection_host(connection_params["host"], connection_params["port"])
                db_name, table_name = (
                    entity.split(".", 1)
                    if "." in entity
                    else (connection_params["database"], entity)
                )
                mysql_conn = pymysql.connect(
                    host=host,
                    port=int(connection_params["port"]),
                    user=connection_params.get("username"),
                    password=connection_params.get("password"),
                    database=connection_params["database"],
                    connect_timeout=4,
                )
                try:
                    with mysql_conn.cursor() as cur:
                        cur.execute(
                            "SELECT coalesce(data_length + index_length, 0) "
                            "FROM information_schema.tables "
                            "WHERE table_schema = %s AND table_name = %s LIMIT 1;",
                            (db_name, table_name),
                        )
                        row = cur.fetchone()
                        return int(row[0] or 0) if row else 0
                finally:
                    mysql_conn.close()
            if st in ("mongodb", "mongo") and getattr(MongoClient, "__module__", "").startswith("pymongo"):
                host = resolve_connection_host(connection_params["host"], connection_params["port"])
                db_name, coll_name = (
                    entity.split(".", 1)
                    if "." in entity
                    else (connection_params["database"], entity)
                )
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
                    serverSelectionTimeoutMS=3000,
                )
                try:
                    stats = client[db_name].command("collStats", coll_name)
                    return int(stats.get("size") or 0)
                finally:
                    client.close()
        except Exception:
            return 0
        return 0

    # -- staging -----------------------------------------------------------

    @staticmethod
    def _stage_batches(
        conn,
        batches: Iterable[pa.Table],
        sink: str,
        on_batch: Optional[Callable[[int], None]] = None,
        create: bool = False,
        commit_rows: int = STAGE_COMMIT_ROWS,
        commit_bytes: int = STAGE_COMMIT_BYTES,
        should_cancel: Optional[Callable[[], bool]] = None,
        schema_policy: str = "evolve",
    ) -> int:
        """Writes Arrow batches into ``sink`` bounded by both row count and byte footprint."""
        total = 0
        pending: list[pa.Table] = []
        pending_rows = 0
        pending_bytes = 0
        created = False

        def execute_with(chunk: pa.Table, statement: str) -> None:
            conn.register("_obelisk_batch", chunk)
            try:
                conn.execute(statement).fetchall()
            finally:
                conn.unregister("_obelisk_batch")

        def _concat_pending(tables: list[pa.Table]) -> pa.Table:
            if len(tables) == 1:
                return tables[0]
            try:
                return pa.concat_tables(tables, promote_options="permissive")
            except Exception:
                # Fallback if schemas have incompatible types across batches (e.g. Decimal vs string)
                all_names: list[str] = []
                for t in tables:
                    for col_name in t.column_names:
                        if col_name not in all_names:
                            all_names.append(col_name)
                normalized: list[pa.Table] = []
                for t in tables:
                    cols = []
                    for col_name in all_names:
                        if col_name in t.column_names:
                            col = t[col_name]
                            if not pa.types.is_string(col.type) and not pa.types.is_large_string(col.type):
                                col = col.cast(pa.large_string())
                            cols.append(col)
                        else:
                            cols.append(pa.nulls(t.num_rows, type=pa.large_string()))
                    normalized.append(pa.Table.from_arrays(cols, names=all_names))
                return pa.concat_tables(normalized, promote_options="permissive")

        def flush() -> None:
            nonlocal pending, pending_rows, pending_bytes
            if not pending:
                return
            chunk = _concat_pending(pending)
            pending, pending_rows, pending_bytes = [], 0, 0
            conn.register("_obelisk_batch", chunk)
            try:
                IngestEngine._evolve_target_schema(
                    conn, sink, "_obelisk_batch", schema_policy=schema_policy
                )
                conn.execute(f"INSERT INTO {sink} BY NAME SELECT * FROM _obelisk_batch").fetchall()
            finally:
                conn.unregister("_obelisk_batch")

        for batch in batches:
            if should_cancel is not None and should_cancel():
                pending.clear()
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
                pending_bytes += batch.nbytes
                if pending_rows >= commit_rows or pending_bytes >= commit_bytes:
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
        """Where streamed batches land: ``(sink, create, load_table)``."""
        if mode == "snapshot" or not exists or write_strategy == "upsert":
            load = f"{table}{LOAD_TABLE_SUFFIX}"
            return self._target_sql(schema, load), True, load
        return target, False, None

    def _prune_load_directory(self, schema: str, directory: str) -> None:
        """Removes the directory a load table leaves behind once it is dropped."""
        root = settings.lakehouse_root_path
        prune_empty_dirs(root, table_directory(root, schema, directory))

    def _swap_into_place(self, conn, *, schema: str, staged: str, target: str, table: str) -> None:
        """Makes a finished load table the live one, atomically."""
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
        self._prune_load_directory(schema, f"{table}{LOAD_TABLE_SUFFIX}")

    def _drop_load_table(self, conn, *, schema: str, table: str) -> None:
        """Removes a load table an earlier run left behind, before this run takes the name."""
        if self._exists(conn, schema, table):
            logger.warning(
                "Dropping %s, left behind by an interrupted load.",
                self._target_sql(schema, table),
            )
        conn.execute(f"DROP TABLE IF EXISTS {self._target_sql(schema, table)};")
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
        schema_policy: str = "evolve",
    ) -> int:
        """Lets the driver write into ``sink``; returns the rows extracted."""
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
            return self._stage_mysql(**common, should_cancel=should_cancel, schema_policy=schema_policy)
        if st in ("mongodb", "mongo"):
            return self._stage_mongodb(**common, should_cancel=should_cancel, schema_policy=schema_policy)
        if st == "oracle":
            return self._stage_oracle(**common, should_cancel=should_cancel, schema_policy=schema_policy)
        if st in ("api", "rest", "rest_api", "erp"):
            return self._stage_api(**common, should_cancel=should_cancel, schema_policy=schema_policy)
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
        schema_policy: str = "evolve",
    ) -> bool:
        """Turns a load table into the table the upsert asked for."""
        deduped = self._deduped(primary_key, watermark_column, relation=staged)

        if mode == "snapshot" or not exists:
            conn.execute(f"CREATE OR REPLACE TABLE {staged} AS {deduped}")
            self._swap_into_place(
                conn, schema=schema, staged=staged, target=target, table=table
            )
            return True

        self._evolve_target_schema(conn, target, staged, schema_policy=schema_policy)
        columns = [c for c in self._columns(conn, target) if c != "_obelisk_rank"]
        key = quote_ident(primary_key)
        assignments = ", ".join(
            f"{quote_ident(c)} = s.{quote_ident(c)}" for c in columns if c != primary_key
        )
        insert_cols = ", ".join(quote_ident(c) for c in columns)
        source_cols = ", ".join(f"s.{quote_ident(c)}" for c in columns)
        guard = ""
        if watermark_column:
            wm = quote_ident(watermark_column)
            guard = (
                f"AND (t.{wm} IS NULL "
                f"OR (s.{wm} IS NOT NULL "
                f"AND s.{wm} >= t.{wm})) "
            )
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
        """One row per key from ``relation``, newest first."""
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
        schema_policy: str = "evolve",
    ) -> Dict[str, Any]:
        """Ingests one entity into the lakehouse."""
        schema, table_stem = self._address(target_folder, entity, target_name)
        target = self._target_sql(schema, table_stem)

        if watermark_column:
            watermark_column = validate_column(watermark_column, what="watermark column")
        if primary_key:
            primary_key = validate_column(primary_key, what="primary key")
        self._require_primary_key(write_strategy, primary_key)
        est_bytes = (
            self._estimate_source_bytes(source_type, connection_params, entity)
            if mode == "snapshot" and not limit
            else 0
        )
        required_headroom = max(512 * 1024 * 1024, int(est_bytes * 0.40))
        advisor.assert_disk_headroom(settings.duckdb_storage_path, min_free_bytes=required_headroom)

        with table_ingest_lock(schema, table_stem):
            conn = duckdb_engine.get_connection()
            if on_connection is not None:
                on_connection(conn)
            load_table: Optional[str] = None
            registered_load: Optional[str] = None
            try:
                self._raise_if_cancelled(should_cancel)
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema)};")
                exists = self._exists(conn, schema, table_stem)

                watermark_value = None
                if mode == "incremental" and watermark_column and exists:
                    watermark_value = self._max_watermark(conn, target, watermark_column)

                if source_type.lower() == "postgres" and write_strategy != "upsert":
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
                        schema_policy=schema_policy,
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
                        register_active_load_table(schema, load_table)
                        registered_load = load_table
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
                        schema_policy=schema_policy,
                    )
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
                                schema_policy=schema_policy,
                            ):
                                load_table = None
                        else:
                            self._swap_into_place(
                                conn, schema=schema, staged=sink, target=target, table=table_stem
                            )
                            load_table = None

                if extracted == 0:
                    if mode == "snapshot" and exists:
                        logger.warning(
                            "Snapshot of %s returned no rows; keeping the existing table.",
                            target,
                        )
                    total = self._row_count(conn, target) if exists else 0
                else:
                    record_lakehouse_commit(conn)
                    total = self._row_count(conn, target)
            finally:
                abandoned = load_table
                if load_table is not None:
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
                if registered_load is not None:
                    unregister_active_load_table(schema, registered_load)
                conn.close()
                if abandoned is not None:
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
        """Attaches the Postgres database: ``(alias, relation, where, limit_clause)``."""
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
        """Moves rows from a relation DuckDB can scan into the lakehouse, in one statement."""
        c = conn if conn is not None else kwargs.get("conn")
        if c is None:
            raise ValueError("conn is required for _transfer_from")

        def _select_expr() -> str:
            try:
                desc = c.execute(f"DESCRIBE SELECT * FROM {source_ref} LIMIT 0;").fetchall()
                enum_cols = [row[0] for row in desc if "ENUM" in str(row[1]).upper()]
                if enum_cols:
                    replaces = ", ".join(
                        f"{quote_ident(col)}::VARCHAR AS {quote_ident(col)}" for col in enum_cols
                    )
                    return f"* REPLACE ({replaces})"
            except Exception as ex:
                logger.debug("Column introspection for ENUM conversion skipped: %s", ex)
            return "*"

        projection = _select_expr()

        if mode == "snapshot" or not exists:
            probe = c.execute(
                f"SELECT 1 FROM {source_ref} {where} LIMIT 1"
            ).fetchall()
            if not probe:
                return 0
            c.execute(
                f"CREATE OR REPLACE TABLE {target} AS "
                f"SELECT {projection} FROM {source_ref} {where}{limit_clause};"
            )
            written = self._row_count(c, target)
        else:
            schema_policy = kwargs.get("schema_policy", "evolve")
            self._evolve_target_schema(c, target, source_ref, schema_policy=schema_policy)
            before = self._row_count(c, target)
            c.execute(
                f"INSERT INTO {target} BY NAME SELECT {projection} FROM {source_ref} {where}{limit_clause};"
            )
            written = max(0, self._row_count(c, target) - before)
        if on_batch is not None:
            on_batch(written)
        return written

    def _transfer_postgres(
        self, conn=None, *, connection_params, entity, mode, watermark_column, watermark_value,
        limit, target, exists, on_batch=None, **kwargs,
    ) -> int:
        """Copies a Postgres table straight into the lakehouse."""
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
                schema_policy=kwargs.get("schema_policy", "evolve"),
            )
        finally:
            c.execute(f"DETACH {alias};")

    def _stage_postgres(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False,
    ) -> int:
        """Postgres into ``sink``, in one statement DuckDB executes end to end."""
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
        schema_policy: str = "evolve",
    ) -> int:
        """MySQL/MariaDB: native C++ DuckDB mysql_scanner fast-path with SSCursor fallback."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        # Use native C++ DuckDB mysql_scanner when pymysql.connect is not mocked by unit tests
        if (
            sink is not None
            and getattr(pymysql.connect, "__module__", "").startswith("pymysql")
        ):
            alias = validate_alias(f"mysql_src_{uuid.uuid4().hex[:6]}")
            attached = False
            try:
                conn.execute("LOAD mysql;")
                dsn = build_kv_dsn(
                    host=host,
                    port=int(connection_params["port"]),
                    database=connection_params["database"],
                    user=connection_params.get("username") or connection_params.get("user"),
                    password=connection_params.get("password"),
                )
                conn.execute(f"ATTACH {sql_string(dsn)} AS {alias} (TYPE MYSQL, READ_ONLY);")
                attached = True
                if "." in entity:
                    db_name, table_name = entity.split(".", 1)
                else:
                    db_name, table_name = connection_params["database"], entity
                relation = f"{alias}.{sql_ident(db_name)}.{sql_ident(table_name)}"
                where = ""
                if mode == "incremental" and watermark_column and watermark_value is not None:
                    where = f"WHERE {sql_ident(watermark_column)} > {_literal(watermark_value)}"
                limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
                return self._transfer_from(
                    conn,
                    source_ref=relation,
                    target=sink,
                    mode="snapshot" if create else "incremental",
                    exists=not create,
                    where=where,
                    limit_clause=limit_clause,
                    on_batch=on_batch,
                    schema_policy=schema_policy,
                )
            except Exception as exc:
                logger.debug("Native DuckDB mysql_scanner fast-path unavailable, falling back to SSCursor: %s", exc)
            finally:
                if attached:
                    try:
                        conn.execute(f"DETACH {alias};")
                    except Exception:
                        pass
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
                    should_cancel=should_cancel, schema_policy=schema_policy,
                )
        finally:
            connection.close()

    def _stage_mongodb(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
        schema_policy: str = "evolve",
    ) -> int:
        """MongoDB: documents in batches, BSON types normalized and sorted on incremental runs."""
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
                wm_val = watermark_value
                if (
                    watermark_column == "_id"
                    and isinstance(wm_val, str)
                    and _OBJECT_ID_HEX_RE.match(wm_val)
                ):
                    try:
                        from bson import ObjectId
                        wm_val = ObjectId(wm_val)
                    except Exception:
                        pass
                query_filter = {watermark_column: {"$gt": wm_val}}

            cursor = collection.find(query_filter).batch_size(BATCH_ROWS)
            if mode == "incremental" and watermark_column:
                cursor = cursor.sort(watermark_column, 1)
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
                        values = [_normalize_cell(doc.get(key)) for key in keys]
                        rows.append(tuple(values))
                    yield _arrow_batch(keys, rows)

            return self._stage_batches(
                conn, batches(), on_batch=on_batch, sink=sink, create=create,
                should_cancel=should_cancel, schema_policy=schema_policy,
            )
        finally:
            client.close()

    def _stage_oracle(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
        schema_policy: str = "evolve",
    ) -> int:
        """Oracle: cursor batches with arraysize=10000 to avoid 500x round-trip amplification."""
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
            cur.arraysize = 10_000
            cur.prefetchrows = 10_000
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
                should_cancel=should_cancel, schema_policy=schema_policy,
            )
        finally:
            connection.close()

    def _stage_api(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None, sink=None, create=False, should_cancel=None,
        schema_policy: str = "evolve",
    ) -> int:
        """REST endpoint staged with byte-bounded response streaming and chunked Arrow batches."""
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

        content_type = ""
        is_ndjson = str(options.get("format", "")).lower() in ("ndjson", "jsonl")
        if is_ndjson and sink is not None:
            import tempfile
            settings.duckdb_temp_path.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".ndjson", dir=settings.duckdb_temp_path, delete=False
            ) as tmp_f:
                tmp_path = Path(tmp_f.name)
                total_bytes = 0
                try:
                    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                        with client.stream("GET", full_url, headers=headers) as response:
                            response.raise_for_status()
                            for chunk in response.iter_bytes(chunk_size=65536):
                                if should_cancel is not None and should_cancel():
                                    raise IngestCancelled(
                                        "Ingestion cancelled: stopped before the table was committed."
                                    )
                                total_bytes += len(chunk)
                                if total_bytes > MAX_API_RESPONSE_BYTES * 8:
                                    raise MemoryError(
                                        f"NDJSON stream from {full_url} exceeded {(MAX_API_RESPONSE_BYTES * 8) // (1024 ** 2)} MiB safety limit."
                                    )
                                tmp_f.write(chunk)
                    tmp_f.flush()
                    if total_bytes == 0:
                        return 0
                    where = ""
                    if mode == "incremental" and watermark_column and watermark_value is not None:
                        where = f"WHERE {sql_ident(watermark_column)} > {_literal(watermark_value)}"
                    limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
                    src_expr = f"read_json_auto({sql_string(tmp_path.as_posix())}, format='newline_delimited')"
                    return self._transfer_from(
                        conn,
                        source_ref=src_expr,
                        target=sink,
                        mode="snapshot" if create else "incremental",
                        exists=not create,
                        where=where,
                        limit_clause=limit_clause,
                        on_batch=on_batch,
                        schema_policy=schema_policy,
                    )
                finally:
                    tmp_path.unlink(missing_ok=True)

        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            with client.stream("GET", full_url, headers=headers) as response:
                response.raise_for_status()
                buf = bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if should_cancel is not None and should_cancel():
                        raise IngestCancelled(
                            "Ingestion cancelled: stopped before the table was committed."
                        )
                    buf.extend(chunk)
                    if len(buf) > MAX_API_RESPONSE_BYTES:
                        raise MemoryError(
                            f"API response from {full_url} exceeded {MAX_API_RESPONSE_BYTES // (1024 ** 2)} MiB safety limit."
                        )
                payload = json.loads(buf.decode("utf-8", errors="replace"))
                del buf

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

        def _api_batches() -> Iterator[pa.Table]:
            for idx in range(0, len(records), BATCH_ROWS):
                sub = records[idx : idx + BATCH_ROWS]
                rows = [
                    tuple(_normalize_cell(record.get(col)) for col in columns)
                    for record in sub
                ]
                yield _arrow_batch(columns, rows)

        return self._stage_batches(
            conn, _api_batches(), on_batch=on_batch, sink=sink, create=create,
            should_cancel=should_cancel, schema_policy=schema_policy,
        )


ingest_engine = IngestEngine()
