import os
import uuid
import json
import sqlite3
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pymongo import MongoClient
import pymysql
import oracledb
import httpx

from app.config import settings
from app.engines.duckdb_engine import duckdb_engine
from app.core.host_resolver import resolve_connection_host
from app.core.storage import warehouse_storage
from app.core.dsn import build_pg_dsn, sql_string, validate_alias

logger = logging.getLogger(__name__)


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


class IngestEngine:
    """
    Universal Ingestion Engine:
    - Track A: High-performance DuckDB direct C++ streaming extraction (PostgreSQL)
      using ATTACH (TYPE POSTGRES) + COPY ... TO ... (FORMAT PARQUET, COMPRESSION SNAPPY).
    - Track B: MongoDB extraction preserving native BSON types (datetimes, ints, floats)
      without string degradation, supporting incremental watermark queries on Date/Timestamp fields.
    - Zero-copy PyArrow Compute (pc.max) for watermark detection.
    - Safe incremental schema merging via pa.concat_tables(promote_options="default").
    - Atomic file swapping via temporary files and os.replace() to prevent Windows file locks / partial writes.
    - Automatic Lakehouse asset registration in data/obelisk.db metadata catalog.
    """

    def _ensure_storage_dir(self, target_folder: Optional[str] = None) -> Path:
        """Resolves a lakehouse sub-directory, creating it when needed.

        Scheme handling lives in Settings.lakehouse_root_path: remote URIs raise
        there instead of being silently treated as local paths (audit B1/B2).
        """
        base_path = settings.lakehouse_root_path
        if target_folder:
            resolved = base_path / target_folder.strip("/\\")
        else:
            resolved = base_path
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    def _record_asset_in_db(
        self,
        name: str,
        layer: str,
        storage_uri: str,
        record_count: int,
        schema_metadata: Dict[str, Any],
    ) -> None:
        """No-op kept for call sites: the catalog is a projection, not a ledger.

        A written Parquet file is the record. Registering it here would mean two
        writers for one fact, which is exactly the drift the projection removes — the
        next catalog read finds the file by itself.
        """
        return

    def _apply_write_strategy(
        self,
        table: pa.Table,
        write_strategy: str,
        primary_key: Optional[str],
        watermark_column: Optional[str],
    ) -> pa.Table:
        """
        Deduplicates records on primary_key if write_strategy == 'upsert',
        preserving the newest record ordered by watermark_column.
        """
        if write_strategy != "upsert":
            return table
        if not primary_key:
            raise ValueError(
                "write_strategy='upsert' requires a primary_key; refusing to write rows "
                "that cannot be deduplicated."
            )
        if primary_key not in table.column_names:
            raise ValueError(
                f"primary_key {primary_key!r} is not one of the extracted columns "
                f"({', '.join(table.column_names)}); cannot deduplicate."
            )

        dconn = duckdb.connect()
        try:
            dconn.register("raw_batch", table)
            order_sql = f'ORDER BY "{watermark_column}" DESC' if watermark_column and watermark_column in table.column_names else ""
            dedup_sql = f'''
                SELECT * FROM raw_batch
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "{primary_key}" {order_sql}) = 1
            '''
            # fetch_arrow_table() exists across the whole duckdb range this project
            # declares; DuckDBPyConnection.arrow() only appeared in later releases, so
            # using it made a clean install fail where the dev box passed.
            return dconn.execute(dedup_sql).fetch_arrow_table()
        finally:
            # Always close: a lingering DuckDB connection retains OS-level file
            # locks on Parquet files and triggers [WinError 32] on Windows when
            # ingestion later swaps files via os.replace().
            dconn.close()

    def _combine_for_write(
        self,
        new_table: pa.Table,
        existing_table: Optional[pa.Table],
        mode: str,
        write_strategy: str,
        primary_key: Optional[str],
        watermark_column: Optional[str],
    ) -> pa.Table:
        """Combines a freshly extracted batch with the existing table.

        Incremental runs append and then deduplicate; snapshot runs replace the
        table but still honour an explicit upsert, so a requested deduplication
        is never silently skipped (audit M2).
        """
        if mode == "incremental" and existing_table is not None:
            if new_table.num_rows == 0:
                return existing_table
            combined = pa.concat_tables([existing_table, new_table], promote_options="default")
            return self._apply_write_strategy(
                combined, write_strategy, primary_key, watermark_column
            )
        return self._apply_write_strategy(
            new_table, write_strategy, primary_key, watermark_column
        )

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
    ) -> Dict[str, Any]:
        """
        Executes ingestion and writes a Snappy Parquet file atomically to lakehouse storage.
        Returns execution statistics and registers asset in Obelisk catalog.
        """
        # Everything lands in a folder. An empty target means the warehouse root, so
        # what is on disk is exactly what the catalog will report.
        requested = target_folder.strip("/\\").replace("..", "") if target_folder else ""
        clean_folder = "/".join(part for part in requested.split("/") if part)
        store = warehouse_storage()
        target_dir = store.root / clean_folder if clean_folder else store.root
        target_dir.mkdir(parents=True, exist_ok=True)

        # Default clean table stem without database/schema prefixes
        if target_name and target_name.strip():
            table_stem = target_name.strip()
        elif "." in entity:
            table_stem = entity.split(".", 1)[-1]
        else:
            table_stem = entity
        table_stem = table_stem.replace("/", "_").replace("\\", "_")

        target_file = target_dir / f"{table_stem}.parquet"

        existing_max_watermark = None
        existing_table: Optional[pa.Table] = None
        existing_file: Optional[Path] = None

        # 1. Inspect the existing Parquet for its incremental watermark. Only the
        # watermark column is read: materialising the whole file here would pull
        # the entire history into RAM just to compute a max.
        if mode == "incremental" and watermark_column and target_file.exists():
            existing_file = target_file
            try:
                watermark_only = pq.read_table(str(target_file), columns=[watermark_column])
                if watermark_only.num_rows > 0:
                    # High-performance C++ max calculation via PyArrow compute
                    max_scalar = pc.max(watermark_only[watermark_column])
                    if max_scalar.is_valid:
                        existing_max_watermark = max_scalar.as_py()
            except Exception:
                existing_max_watermark = None

        st = source_type.lower()

        # Postgres merges incrementally inside DuckDB (out-of-core) and must not be
        # handed an in-memory copy of the existing table. The remaining sources
        # still concatenate in RAM, so they need it materialised.
        if st != "postgres" and existing_file is not None:
            try:
                existing_table = pq.read_table(str(existing_file))
            except Exception:
                existing_table = None

        if st == "postgres":
            res = self._ingest_postgres(
                connection_params=connection_params,
                entity=entity,
                target_file=target_file,
                mode=mode,
                watermark_column=watermark_column,
                watermark_value=existing_max_watermark,
                existing_file=existing_file,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
            )
        elif st == "mongodb":
            res = self._ingest_mongodb(
                connection_params=connection_params,
                entity=entity,
                target_file=target_file,
                mode=mode,
                watermark_column=watermark_column,
                watermark_value=existing_max_watermark,
                existing_table=existing_table,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
            )
        elif st in ("mysql", "mariadb"):
            res = self._ingest_mysql(
                connection_params=connection_params,
                entity=entity,
                target_file=target_file,
                mode=mode,
                watermark_column=watermark_column,
                watermark_value=existing_max_watermark,
                existing_table=existing_table,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
            )
        elif st == "oracle":
            res = self._ingest_oracle(
                connection_params=connection_params,
                entity=entity,
                target_file=target_file,
                mode=mode,
                watermark_column=watermark_column,
                watermark_value=existing_max_watermark,
                existing_table=existing_table,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
            )
        elif st in ("api", "rest", "rest_api", "erp"):
            res = self._ingest_api(
                connection_params=connection_params,
                entity=entity,
                target_file=target_file,
                mode=mode,
                watermark_column=watermark_column,
                watermark_value=existing_max_watermark,
                existing_table=existing_table,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
            )
        else:
            raise ValueError(f"Unsupported source type '{source_type}' for ingestion.")

        # 2. Extract column schema metadata from final Parquet file
        meta = pq.read_metadata(str(target_file))
        schema_meta = {
            "num_rows": meta.num_rows,
            "columns": [
                {
                    "name": meta.schema.column(i).name,
                    "type": meta.schema.column(i).physical_type,
                }
                for i in range(meta.num_columns)
            ],
        }

        # 3. Register asset in metadata DB
        storage_uri_clean = settings.STORAGE_URI.rstrip("/")
        if clean_folder:
            asset_storage_uri = f"{storage_uri_clean}/{clean_folder}/{table_stem}.parquet"
        else:
            asset_storage_uri = f"{storage_uri_clean}/{table_stem}.parquet"

        self._record_asset_in_db(
            name=table_stem,
            layer=clean_folder,
            storage_uri=asset_storage_uri,
            record_count=res["num_rows"],
            schema_metadata=schema_meta,
        )

        return {
            "status": "success",
            "entity": entity,
            "target": table_stem,
            "target_folder": clean_folder,
            "num_rows": res["num_rows"],
            "new_rows_extracted": res["new_rows_extracted"],
            "file_path": target_file.as_posix(),
            "mode": mode,
            "write_strategy": write_strategy,
        }

    def _merge_incremental_parquet(
        self,
        existing_file: Path,
        delta_file: Path,
        output_file: Path,
        write_strategy: str,
        primary_key: Optional[str],
        watermark_column: Optional[str],
    ) -> int:
        """Merges an existing Bronze Parquet file with a delta, out-of-core.

        DuckDB reads both files lazily via read_parquet() and only the merged
        result is written, so memory stays flat no matter how large the existing
        history is (the old code concatenated both tables in Python RAM).
        Returns the merged row count.
        """
        existing_posix = existing_file.as_posix()
        delta_posix = delta_file.as_posix()
        output_posix = output_file.as_posix()

        if write_strategy == "upsert" and not primary_key:
            raise ValueError(
                "write_strategy='upsert' requires a primary_key; refusing to merge the "
                "delta without deduplication."
            )

        union_sql = (
            f"SELECT * FROM read_parquet('{existing_posix}') "
            f"UNION ALL BY NAME SELECT * FROM read_parquet('{delta_posix}')"
        )
        dedup_sql = None
        if write_strategy == "upsert" and primary_key:
            order_sql = f'ORDER BY "{watermark_column}" DESC' if watermark_column else ""
            dedup_sql = (
                f'SELECT * FROM ({union_sql}) '
                f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "{primary_key}" {order_sql}) = 1'
            )

        copy_target = f"'{output_posix}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
        dconn = duckdb.connect()
        try:
            if dedup_sql is not None:
                try:
                    dconn.execute(f"COPY ({dedup_sql}) TO {copy_target}")
                except Exception as e:
                    # Never fall back to the raw union: that silently writes
                    # duplicate primary keys into the live file (audit H1).
                    raise RuntimeError(
                        f"Upsert deduplication failed for {output_file.name} on primary key "
                        f"{primary_key!r}; the existing file was left untouched. Cause: {e}"
                    ) from e
            else:
                dconn.execute(f"COPY ({union_sql}) TO {copy_target}")
            return dconn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_posix}')").fetchone()[0]
        finally:
            dconn.close()

    def _ingest_postgres(
        self,
        connection_params: Dict[str, Any],
        entity: str,
        target_file: Path,
        mode: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[Any] = None,
        existing_file: Optional[Path] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Track A: DuckDB native C++ direct streaming extraction from PostgreSQL.
        Attaches the PostgreSQL database and streams directly to Snappy Parquet.
        Incremental merges stay inside DuckDB so large histories never load into RAM.
        """
        if write_strategy == "upsert" and not primary_key:
            raise ValueError(
                "write_strategy='upsert' requires a primary_key; refusing to write rows "
                "that cannot be deduplicated."
            )

        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        port = connection_params["port"]
        database = connection_params["database"]
        user = connection_params["username"]
        password = connection_params["password"]
        sslmode = connection_params.get("sslmode", "disable")

        if "." in entity:
            schema_name, table_name = entity.split(".", 1)
        else:
            schema_name = connection_params.get("schema", "public")
            table_name = entity

        alias = f"pg_extract_{uuid.uuid4().hex[:6]}"
        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")
        temp_file_posix = temp_file.as_posix()
        limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""

        conn = duckdb_engine.get_connection()
        try:
            conn.execute("LOAD postgres;")
            dsn = build_pg_dsn(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
                sslmode=sslmode,
            )
            conn.execute(
                f"ATTACH {sql_string(dsn)} AS {validate_alias(alias)} "
                f"(TYPE POSTGRES, READ_ONLY);"
            )

            # A full refresh runs for a snapshot, for a first run, or when the
            # existing file has no usable incremental cursor. That last case used
            # to fall through to `WHERE col > 'None'`, which crashed on timestamp
            # columns or filtered string columns against the literal "None"
            # (audit H2).
            full_refresh = (
                mode == "snapshot"
                or existing_file is None
                or not watermark_column
                or watermark_value is None
            )
            if full_refresh:
                if mode == "incremental" and existing_file is not None:
                    logger.warning(
                        "Incremental ingest of %s has no usable watermark "
                        "(column=%r, value=%r); falling back to a full refresh.",
                        entity,
                        watermark_column,
                        watermark_value,
                    )

                select_sql = f'SELECT * FROM {alias}."{schema_name}"."{table_name}"{limit_clause}'
                if write_strategy == "upsert" and primary_key:
                    order_sql = f'ORDER BY "{watermark_column}" DESC' if watermark_column else ""
                    select_sql = (
                        f'SELECT * FROM ({select_sql}) '
                        f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "{primary_key}" {order_sql}) = 1'
                    )
                # Direct C++ streaming straight to Snappy Parquet
                query = f"COPY ({select_sql}) TO '{temp_file_posix}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
                conn.execute(query)

                meta = pq.read_metadata(str(temp_file))
                num_rows = meta.num_rows

                # Atomic swap
                os.replace(temp_file, target_file)
                return {"num_rows": num_rows, "new_rows_extracted": num_rows}

            # Incremental delta extraction via DuckDB
            delta_file = target_file.with_name(f"{target_file.stem}_delta_{uuid.uuid4().hex[:8]}.tmp.parquet")
            delta_file_posix = delta_file.as_posix()

            if isinstance(watermark_value, (int, float)):
                where_clause = f'WHERE "{watermark_column}" > {watermark_value}'
            else:
                where_clause = f"WHERE \"{watermark_column}\" > '{watermark_value}'"

            query = f'COPY (SELECT * FROM {alias}."{schema_name}"."{table_name}" {where_clause} ORDER BY "{watermark_column}" ASC{limit_clause}) TO \'{delta_file_posix}\' (FORMAT PARQUET, COMPRESSION SNAPPY);'
            conn.execute(query)

            try:
                new_rows = pq.read_metadata(str(delta_file)).num_rows if delta_file.exists() else 0
                if new_rows == 0:
                    # Nothing new — leave the existing Parquet file untouched.
                    return {
                        "num_rows": pq.read_metadata(str(existing_file)).num_rows,
                        "new_rows_extracted": 0,
                    }

                # Merge + upsert dedup stream out-of-core inside DuckDB, then the
                # result is swapped in atomically.
                num_rows = self._merge_incremental_parquet(
                    existing_file=existing_file,
                    delta_file=delta_file,
                    output_file=temp_file,
                    write_strategy=write_strategy,
                    primary_key=primary_key,
                    watermark_column=watermark_column,
                )
                os.replace(temp_file, target_file)
                return {"num_rows": num_rows, "new_rows_extracted": new_rows}
            finally:
                if delta_file.exists():
                    delta_file.unlink(missing_ok=True)

        finally:
            try:
                conn.execute(f"DETACH {alias};")
            except Exception:
                pass
            conn.close()
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _ingest_mongodb(
        self,
        connection_params: Dict[str, Any],
        entity: str,
        target_file: Path,
        mode: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[Any] = None,
        existing_table: Optional[pa.Table] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Track B: MongoDB extraction preserving native BSON types (datetimes, floats, ints)
        so timestamp/date watermarks compare natively against BSON Date fields.
        """
        user = connection_params.get("username")
        pw = connection_params.get("password")
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        port = connection_params["port"]
        auth_db = connection_params.get("auth_db", "admin")
        db_name = connection_params["database"]

        # Kwargs avoid URI escaping problems in passwords (audit H5).
        client = MongoClient(
            host=host,
            port=port,
            username=user or None,
            password=pw or None,
            authSource=auth_db if (user and pw) else None,
            serverSelectionTimeoutMS=5000,
        )
        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")

        try:
            if "." in entity:
                db_override, coll_name = entity.split(".", 1)
                coll = client[db_override][coll_name]
            else:
                db = client[db_name]
                coll = db[entity]

            query_filter = {}
            if mode == "incremental" and watermark_column and watermark_value is not None:
                # Direct comparison using native python datetime or integer/float value
                query_filter = {watermark_column: {"$gt": watermark_value}}

            cursor = coll.find(query_filter)
            if limit and limit > 0:
                cursor = cursor.limit(int(limit))
            docs = list(cursor)

            if not docs and existing_table is not None:
                return {"num_rows": existing_table.num_rows, "new_rows_extracted": 0}

            # Normalize documents preserving native datetimes
            all_keys = set()
            for d in docs:
                all_keys.update(d.keys())
            ordered_keys = sorted(list(all_keys))

            col_data = {k: [] for k in ordered_keys}
            for d in docs:
                for k in ordered_keys:
                    val = d.get(k)
                    # Convert MongoDB ObjectId to string
                    if str(type(val)).find("ObjectId") != -1:
                        val = str(val)
                    elif isinstance(val, (dict, list)):
                        val = json.dumps(val, default=str)
                    # Keep datetime, numbers, booleans, strings intact
                    col_data[k].append(val)

            new_table = pa.Table.from_pydict(col_data) if docs else pa.Table.from_pydict({})

            final_table = self._combine_for_write(
                new_table, existing_table, mode, write_strategy, primary_key, watermark_column
            )

            # Atomic write
            pq.write_table(final_table, str(temp_file), compression="SNAPPY")
            os.replace(temp_file, target_file)

            return {"num_rows": final_table.num_rows, "new_rows_extracted": new_table.num_rows}

        finally:
            client.close()
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _ingest_mysql(
        self,
        connection_params: Dict[str, Any],
        entity: str,
        target_file: Path,
        mode: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[Any] = None,
        existing_table: Optional[pa.Table] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ) -> Dict[str, int]:
        """Ingests rows from MySQL/MariaDB into Bronze Snappy Parquet."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        port = connection_params["port"]
        database = connection_params["database"]
        user = connection_params.get("username")
        password = connection_params.get("password")

        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor,
            charset="utf8mb4",
        )
        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")
        limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""

        if "." in entity:
            db_name, table_name = entity.split(".", 1)
            table_ref = f"`{db_name}`.`{table_name}`"
        else:
            table_ref = f"`{entity}`"

        try:
            with conn.cursor() as cur:
                if mode == "incremental" and watermark_column and watermark_value is not None:
                    query = f"SELECT * FROM {table_ref} WHERE `{watermark_column}` > %s ORDER BY `{watermark_column}` ASC{limit_clause}"
                    cur.execute(query, (watermark_value,))
                else:
                    cur.execute(f"SELECT * FROM {table_ref}{limit_clause}")
                rows = cur.fetchall()
                # Keep the projection's columns so an empty snapshot still writes a
                # readable Parquet schema instead of a 0-column file.
                columns = [desc[0] for desc in cur.description] if cur.description else []

            if not rows and existing_table is not None:
                return {"num_rows": existing_table.num_rows, "new_rows_extracted": 0}

            all_keys = list(rows[0].keys()) if rows else columns
            col_data = {k: [] for k in all_keys}
            for r in rows:
                for k in all_keys:
                    v = r.get(k)
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, default=str)
                    col_data[k].append(v)

            new_table = pa.Table.from_pydict(col_data)

            final_table = self._combine_for_write(
                new_table, existing_table, mode, write_strategy, primary_key, watermark_column
            )

            pq.write_table(final_table, str(temp_file), compression="SNAPPY")
            os.replace(temp_file, target_file)

            return {"num_rows": final_table.num_rows, "new_rows_extracted": new_table.num_rows}
        finally:
            conn.close()
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _ingest_oracle(
        self,
        connection_params: Dict[str, Any],
        entity: str,
        target_file: Path,
        mode: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[Any] = None,
        existing_table: Optional[pa.Table] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ) -> Dict[str, int]:
        """Ingests rows from Oracle DB into Bronze Snappy Parquet."""
        host = resolve_connection_host(connection_params["host"], connection_params["port"])
        port = connection_params["port"]
        database = connection_params["database"]
        user = connection_params.get("username")
        password = connection_params.get("password")
        dsn = f"{host}:{port}/{database}"

        conn = oracledb.connect(user=user, password=password, dsn=dsn, tcp_connect_timeout=5.0)
        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")
        limit_clause = f" FETCH FIRST {int(limit)} ROWS ONLY" if limit and limit > 0 else ""

        if "." in entity:
            schema_name, table_name = entity.split(".", 1)
            table_ref = f'"{schema_name}"."{table_name}"'
        else:
            table_ref = f'"{entity}"'

        try:
            cur = conn.cursor()
            if mode == "incremental" and watermark_column and watermark_value is not None:
                query = f'SELECT * FROM {table_ref} WHERE "{watermark_column}" > :val ORDER BY "{watermark_column}" ASC{limit_clause}'
                cur.execute(query, [watermark_value])
            else:
                cur.execute(f'SELECT * FROM {table_ref}{limit_clause}')

            columns = [desc[0].lower() for desc in cur.description] if cur.description else []
            raw_rows = cur.fetchall()

            if not raw_rows and existing_table is not None:
                return {"num_rows": existing_table.num_rows, "new_rows_extracted": 0}

            col_data = {c: [] for c in columns}
            for row in raw_rows:
                for c, val in zip(columns, row):
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val, default=str)
                    col_data[c].append(val)

            # col_data is keyed by the projection's columns, so an empty snapshot
            # still produces a readable (0-row) Parquet schema.
            new_table = pa.Table.from_pydict(col_data)

            final_table = self._combine_for_write(
                new_table, existing_table, mode, write_strategy, primary_key, watermark_column
            )

            pq.write_table(final_table, str(temp_file), compression="SNAPPY")
            os.replace(temp_file, target_file)

            return {"num_rows": final_table.num_rows, "new_rows_extracted": new_table.num_rows}
        finally:
            conn.close()
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _ingest_api(
        self,
        connection_params: Dict[str, Any],
        entity: str,
        target_file: Path,
        mode: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[Any] = None,
        existing_table: Optional[pa.Table] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ) -> Dict[str, int]:
        """Ingests JSON records from a REST API / ERP endpoint into Bronze Snappy Parquet."""
        base_url = (connection_params.get("host") or connection_params.get("url") or "http://localhost").strip()
        if not base_url.startswith(("http://", "https://")):
            port = connection_params.get("port")
            if port and port not in (80, 443):
                base_url = f"http://{base_url}:{port}"
            else:
                base_url = f"http://{base_url}"

        endpoint = entity if entity != "api_records" else ""
        if endpoint:
            full_url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        else:
            full_url = base_url

        options = connection_params.get("options") or {}
        token = connection_params.get("password") or options.get("token") or options.get("api_key")
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if options.get("headers"):
            headers.update(options["headers"])

        data_key = options.get("data_key")

        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            resp = client.get(full_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        from app.engines.introspector import Introspector
        records = Introspector._extract_api_records(data, data_key)

        if mode == "incremental" and watermark_column and watermark_value is not None:
            records = [
                r for r in records
                if r.get(watermark_column) is not None
                and _watermark_is_newer(r[watermark_column], watermark_value)
            ]

        if limit and limit > 0:
            records = records[:int(limit)]

        if not records and existing_table is not None:
            return {"num_rows": existing_table.num_rows, "new_rows_extracted": 0}

        all_keys = set()
        for r in records:
            all_keys.update(r.keys())
        ordered_keys = sorted(list(all_keys))

        col_data = {k: [] for k in ordered_keys}
        for r in records:
            for k in ordered_keys:
                val = r.get(k)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, default=str)
                col_data[k].append(val)

        new_table = pa.Table.from_pydict(col_data) if records else pa.Table.from_pydict({})

        final_table = self._combine_for_write(
            new_table, existing_table, mode, write_strategy, primary_key, watermark_column
        )

        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")
        try:
            pq.write_table(final_table, str(temp_file), compression="SNAPPY")
            os.replace(temp_file, target_file)
            return {"num_rows": final_table.num_rows, "new_rows_extracted": new_table.num_rows}
        finally:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

ingest_engine = IngestEngine()

