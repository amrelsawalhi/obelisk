"""Ingestion writes into the lakehouse catalog.

Every source is staged in a temporary DuckDB table and then written with one statement:
``CREATE OR REPLACE`` for a snapshot, ``INSERT`` for an incremental append, ``MERGE INTO``
for an upsert. Nothing is concatenated in Python, so the size of the existing table never
becomes a Python object — the old path materialised the whole history to merge 40 MB —
and a failed run leaves the previous table exactly as it was.

Postgres is read by DuckDB itself (one statement, no copies). The other drivers stream
their cursor in batches, so their memory footprint is one batch rather than one table.
"""
from __future__ import annotations

import itertools
import json
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Iterator, Optional

import httpx
import oracledb
import pyarrow as pa
import pymysql
from pymongo import MongoClient

from app.core.catalog import SOURCE, quote_ident, validate_column, validate_segment
from app.core.dsn import build_pg_dsn, sql_ident, sql_string, validate_alias
from app.core.host_resolver import resolve_connection_host
from app.engines.duckdb_engine import duckdb_engine

logger = logging.getLogger(__name__)

BATCH_ROWS = 50_000


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


def _arrow_batch(columns: Iterable[str], rows: Iterable[Iterable[Any]]) -> pa.Table:
    """Builds one Arrow batch from column names and row tuples, preserving types."""
    names = list(columns)
    data: Dict[str, list] = {name: [] for name in names}
    for row in rows:
        for name, value in zip(names, row):
            data[name].append(value)
    return pa.Table.from_pydict(data)


def _bt(value: object) -> str:
    """A MySQL identifier: backticks, with any embedded backtick doubled."""
    return "`" + str(value).replace("`", "``") + "`"


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
        row = conn.execute(
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
        ).fetchone()
        return row is not None

    @staticmethod
    def _max_watermark(conn, target: str, column: str):
        """The highest watermark already stored, asked of the table itself."""
        try:
            value = conn.execute(
                f"SELECT max({quote_ident(column)}) FROM {target}"
            ).fetchone()
        except Exception:
            return None
        if not value or value[0] is None:
            return None
        return value[0]

    @staticmethod
    def _row_count(conn, target: str) -> int:
        return int(conn.execute(f"SELECT count(*) FROM {target}").fetchone()[0])

    @staticmethod
    def _columns(conn, table: str) -> list[str]:
        return [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]

    # -- staging -----------------------------------------------------------

    @staticmethod
    def _stage_batches(
        conn, batches: Iterable[pa.Table], on_batch: Optional[Callable[[int], None]] = None
    ) -> int:
        """Fills a temp table named ``staging`` from Arrow batches; returns rows read.

        ``on_batch`` receives the running total after each batch. One large table is the
        case where a status line with no counter looks wedged, and this is the only
        progress signal it produces before the write statement runs.
        """
        total = 0
        for batch in batches:
            if batch.num_rows == 0:
                continue
            conn.register("_obelisk_batch", batch)
            try:
                if total == 0:
                    conn.execute(
                        "CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM _obelisk_batch"
                    )
                else:
                    conn.execute("INSERT INTO staging SELECT * FROM _obelisk_batch")
            finally:
                conn.unregister("_obelisk_batch")
            total += batch.num_rows
            if on_batch is not None:
                on_batch(total)
        return total

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
    ) -> int:
        """Lets the driver fill ``staging``; returns the rows extracted."""
        common = {
            "conn": conn,
            "connection_params": connection_params,
            "entity": entity,
            "mode": mode,
            "watermark_column": watermark_column,
            "watermark_value": watermark_value,
            "limit": limit,
            "on_batch": on_batch,
        }
        st = source_type.lower()
        if st == "postgres":
            return self._stage_postgres(**common)
        if st in ("mysql", "mariadb"):
            return self._stage_mysql(**common)
        if st in ("mongodb", "mongo"):
            return self._stage_mongodb(**common)
        if st == "oracle":
            return self._stage_oracle(**common)
        if st in ("api", "rest", "rest_api", "erp"):
            return self._stage_api(**common)
        raise ValueError(f"Unsupported source type '{source_type}' for ingestion.")

    # -- writing the staged rows ------------------------------------------

    def _write_staged(
        self,
        conn,
        *,
        target: str,
        mode: str,
        exists: bool,
        write_strategy: str,
        primary_key: Optional[str],
        watermark_column: Optional[str],
    ) -> None:
        """Moves ``staging`` into the lake: replace, append, or merge.

        Every branch is one statement DuckDB executes, so the size of the existing table
        never becomes a Python object and a crash cannot leave a half-written table.
        """
        if write_strategy == "upsert" and not primary_key:
            raise ValueError(
                "write_strategy='upsert' requires a primary_key; refusing to write rows "
                "that cannot be deduplicated."
            )

        if mode == "snapshot" or not exists:
            if write_strategy == "upsert":
                conn.execute(f"CREATE OR REPLACE TABLE {target} AS {self._deduped(primary_key, watermark_column)}")
            else:
                conn.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM staging")
            return

        if write_strategy == "upsert":
            columns = [c for c in self._columns(conn, target) if c != "_obelisk_rank"]
            key = quote_ident(primary_key)
            assignments = ", ".join(
                f"{quote_ident(c)} = s.{quote_ident(c)}" for c in columns if c != primary_key
            )
            insert_cols = ", ".join(quote_ident(c) for c in columns)
            source_cols = ", ".join(f"s.{quote_ident(c)}" for c in columns)
            # Deduplicating the batch is not enough: the *existing* row has a watermark
            # too, and a staged row with no watermark must never beat it. That is the
            # failure the old Arrow path shipped (audit F-02), so the condition is in the
            # statement where it cannot be forgotten.
            guard = ""
            if watermark_column:
                wm = quote_ident(watermark_column)
                guard = (
                    f"AND (t.{wm} IS NULL "
                    f"OR (s.{wm} IS NOT NULL "
                    f"AND s.{wm} >= t.{wm})) "
                )
            conn.execute(
                f"""
                MERGE INTO {target} AS t
                USING ({self._deduped(primary_key, watermark_column)}) AS s
                ON t.{key} = s.{key}
                WHEN MATCHED {guard}THEN UPDATE SET {assignments}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})
                """
            )
            return

        conn.execute(f"INSERT INTO {target} SELECT * FROM staging")

    @staticmethod
    def _deduped(primary_key: str, watermark_column: Optional[str]) -> str:
        """One row per key, newest first.

        ``NULLS LAST`` matters: a row whose watermark is missing must never outrank a row
        that has one — that is exactly how the old Arrow path lost updates (audit F-02).
        """
        order = f"{quote_ident(watermark_column)} DESC NULLS LAST" if watermark_column else "1"
        return f"""
            SELECT * EXCLUDE (_obelisk_rank) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {quote_ident(primary_key)} ORDER BY {order}
                ) AS _obelisk_rank
                FROM staging
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
    ) -> Dict[str, Any]:
        """Ingests one entity into the lakehouse.

        The driver stages the rows in a temporary table; one statement then replaces,
        appends or merges them into the target. A failed run leaves the previous table
        exactly as it was, because nothing is written until that statement runs.

        ``on_batch`` is called with the running row count as rows are staged, so a caller
        can report progress on a table large enough to look stuck.
        """
        schema, table_stem = self._address(target_folder, entity, target_name)
        target = self._target_sql(schema, table_stem)

        # Both are column identifiers this code writes into the MERGE and the source
        # filter, so they follow the column rule rather than the table rule (no dots).
        if watermark_column:
            watermark_column = validate_column(watermark_column, what="watermark column")
        if primary_key:
            primary_key = validate_column(primary_key, what="primary key")

        conn = duckdb_engine.get_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema)};")
            exists = self._exists(conn, schema, table_stem)

            watermark_value = None
            if mode == "incremental" and watermark_column and exists:
                watermark_value = self._max_watermark(conn, target, watermark_column)

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
            )

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
                self._write_staged(
                    conn,
                    target=target,
                    mode=mode,
                    exists=exists,
                    write_strategy=write_strategy,
                    primary_key=primary_key,
                    watermark_column=watermark_column,
                )
                total = self._row_count(conn, target)
        finally:
            conn.close()

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

    def _stage_postgres(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None,
    ) -> int:
        """Postgres is read by DuckDB itself: one statement, no copies, no Python rows."""
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
        try:
            if "." in entity:
                schema_name, table_name = entity.split(".", 1)
            else:
                schema_name, table_name = "public", entity
            where = ""
            if mode == "incremental" and watermark_column and watermark_value is not None:
                where = f"WHERE {sql_ident(watermark_column)} > {_literal(watermark_value)}"
            limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
            conn.execute(
                f'CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM '
                f'{alias}.{sql_ident(schema_name)}.{sql_ident(table_name)} {where}{limit_clause}'
            )
            staged = self._row_count(conn, "staging")
            # DuckDB stages Postgres in one statement, so there is nothing to report while
            # it runs. The count still lands before the write, which is the slower half.
            if on_batch is not None:
                on_batch(staged)
            return staged
        finally:
            conn.execute(f"DETACH {alias};")

    def _stage_mysql(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None,
    ) -> int:
        """MySQL/MariaDB: cursor batches in, one Arrow batch at a time."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        connection = pymysql.connect(
            host=host,
            port=int(connection_params["port"]),
            user=connection_params.get("username"),
            password=connection_params.get("password"),
            database=connection_params["database"],
            cursorclass=pymysql.cursors.DictCursor,
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
                        clean = []
                        for row in rows:
                            clean.append(
                                tuple(
                                    json.dumps(row.get(col), default=str)
                                    if isinstance(row.get(col), (dict, list))
                                    else row.get(col)
                                    for col in columns
                                )
                            )
                        yield _arrow_batch(columns, clean)

                return self._stage_batches(conn, batches(), on_batch=on_batch)
        finally:
            connection.close()

    def _stage_mongodb(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None,
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

            return self._stage_batches(conn, batches(), on_batch=on_batch)
        finally:
            client.close()

    def _stage_oracle(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None,
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
                    clean = []
                    for row in rows:
                        clean.append(
                            tuple(
                                json.dumps(value, default=str)
                                if isinstance(value, (dict, list))
                                else value
                                for value in row
                            )
                        )
                    yield _arrow_batch(columns, clean)

            return self._stage_batches(conn, batches(), on_batch=on_batch)
        finally:
            connection.close()

    def _stage_api(
        self, *, conn, connection_params, entity, mode, watermark_column, watermark_value,
        limit, on_batch=None,
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
        return self._stage_batches(conn, [_arrow_batch(columns, rows)], on_batch=on_batch)


ingest_engine = IngestEngine()




