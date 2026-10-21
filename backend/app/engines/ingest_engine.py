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

logger = logging.getLogger(__name__)

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
        base_path = Path(settings.duckdb_storage_path)
        if target_folder:
            resolved = base_path / target_folder.strip("/\\")
        else:
            resolved = base_path
        # Guard local directory creation against remote cloud URIs (s3://, gs://, abfss://)
        if not settings.STORAGE_URI.startswith(("s3://", "gs://", "abfss://")):
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
        """Records or updates the lakehouse asset metadata in SQLite brain."""
        db_path = settings.DATABASE_URL.replace("sqlite+aiosqlite:///", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        metadata_json = json.dumps(schema_metadata)
        asset_id = str(uuid.uuid4())

        try:
            conn = sqlite3.connect(db_path, timeout=10.0)
            with conn:
                cur = conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL;")
                cur.execute(
                    """
                    INSERT INTO assets (id, name, layer, storage_uri, format, record_count, schema_metadata, created_at, last_materialized_at)
                    VALUES (?, ?, ?, ?, 'parquet', ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        storage_uri=excluded.storage_uri,
                        record_count=excluded.record_count,
                        schema_metadata=excluded.schema_metadata,
                        last_materialized_at=excluded.last_materialized_at;
                    """,
                    (asset_id, name, layer, storage_uri, record_count, metadata_json, now_iso, now_iso),
                )
            conn.close()
        except Exception:
            pass

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
        if write_strategy != "upsert" or not primary_key or primary_key not in table.column_names:
            return table
        try:
            dconn = duckdb.connect()
        except Exception:
            return table
        try:
            dconn.register("raw_batch", table)
            order_sql = f'ORDER BY "{watermark_column}" DESC' if watermark_column and watermark_column in table.column_names else ""
            dedup_sql = f'''
                SELECT * FROM raw_batch
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "{primary_key}" {order_sql}) = 1
            '''
            deduped = dconn.execute(dedup_sql).arrow().read_all()
            return deduped
        except Exception as e:
            logger.warning("Upsert deduplication failed, falling back to raw batch: %s", e)
            return table
        finally:
            # Always close: a lingering DuckDB connection retains OS-level file
            # locks on Parquet files and triggers [WinError 32] on Windows when
            # ingestion later swaps files via os.replace().
            dconn.close()

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
        clean_folder = target_folder.strip("/\\").replace("..", "") if target_folder else ""
        if clean_folder:
            target_dir = settings.lakehouse_root_path / clean_folder
        else:
            target_dir = settings.lakehouse_root_path

        if not settings.STORAGE_URI.startswith(("s3://", "gs://", "abfss://")):
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

        # 1. Inspect existing table for incremental watermark
        if mode == "incremental" and watermark_column and target_file.exists():
            try:
                existing_table = pq.read_table(str(target_file))
                if existing_table.num_rows > 0 and watermark_column in existing_table.column_names:
                    # High-performance C++ max calculation via PyArrow compute
                    max_scalar = pc.max(existing_table[watermark_column])
                    if max_scalar.is_valid:
                        existing_max_watermark = max_scalar.as_py()
            except Exception:
                existing_table = None
                existing_max_watermark = None

        st = source_type.lower()
        if st == "postgres":
            res = self._ingest_postgres(
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
            layer=clean_folder or "main",
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

    def _ingest_postgres(
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
        Track A: DuckDB native C++ direct streaming extraction from PostgreSQL.
        Attaches the PostgreSQL database and streams directly to Snappy Parquet.
        """
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
            dsn = f"host={host} port={port} dbname={database} user={user} password={password} sslmode={sslmode}"
            conn.execute(f"ATTACH '{dsn}' AS {alias} (TYPE POSTGRES, READ_ONLY);")

            if mode == "snapshot" or existing_table is None:
                # Direct C++ streaming snapshot straight to Snappy Parquet
                query = f'COPY (SELECT * FROM {alias}."{schema_name}"."{table_name}"{limit_clause}) TO \'{temp_file_posix}\' (FORMAT PARQUET, COMPRESSION SNAPPY);'
                conn.execute(query)

                meta = pq.read_metadata(str(temp_file))
                num_rows = meta.num_rows

                # Atomic swap
                os.replace(temp_file, target_file)
                return {"num_rows": num_rows, "new_rows_extracted": num_rows}

            else:
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
                    delta_table = pq.read_table(str(delta_file))
                    new_rows = delta_table.num_rows

                    if new_rows > 0:
                        # Schema-safe concatenation with null-type promotion
                        final_table = pa.concat_tables([existing_table, delta_table], promote_options="default")
                        final_table = self._apply_write_strategy(final_table, write_strategy, primary_key, watermark_column)
                    else:
                        final_table = existing_table

                    pq.write_table(final_table, str(temp_file), compression="SNAPPY")
                    os.replace(temp_file, target_file)
                    return {"num_rows": final_table.num_rows, "new_rows_extracted": new_rows}
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

        if user and pw:
            uri = f"mongodb://{user}:{pw}@{host}:{port}/?authSource={auth_db}"
        else:
            uri = f"mongodb://{host}:{port}/"

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
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

            if mode == "incremental" and existing_table is not None:
                if new_table.num_rows > 0:
                    final_table = pa.concat_tables([existing_table, new_table], promote_options="default")
                    final_table = self._apply_write_strategy(final_table, write_strategy, primary_key, watermark_column)
                else:
                    final_table = existing_table
            else:
                final_table = new_table

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

            if not rows and existing_table is not None:
                return {"num_rows": existing_table.num_rows, "new_rows_extracted": 0}

            all_keys = list(rows[0].keys()) if rows else []
            col_data = {k: [] for k in all_keys}
            for r in rows:
                for k in all_keys:
                    v = r.get(k)
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, default=str)
                    col_data[k].append(v)

            new_table = pa.Table.from_pydict(col_data) if rows else pa.Table.from_pydict({})

            if mode == "incremental" and existing_table is not None:
                if new_table.num_rows > 0:
                    final_table = pa.concat_tables([existing_table, new_table], promote_options="default")
                    final_table = self._apply_write_strategy(final_table, write_strategy, primary_key, watermark_column)
                else:
                    final_table = existing_table
            else:
                final_table = new_table

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

            new_table = pa.Table.from_pydict(col_data) if raw_rows else pa.Table.from_pydict({})

            if mode == "incremental" and existing_table is not None:
                if new_table.num_rows > 0:
                    final_table = pa.concat_tables([existing_table, new_table], promote_options="default")
                    final_table = self._apply_write_strategy(final_table, write_strategy, primary_key, watermark_column)
                else:
                    final_table = existing_table
            else:
                final_table = new_table

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
            filtered_records = []
            for r in records:
                val = r.get(watermark_column)
                if val is not None and str(val) > str(watermark_value):
                    filtered_records.append(r)
            records = filtered_records

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

        if mode == "incremental" and existing_table is not None:
            if new_table.num_rows > 0:
                final_table = pa.concat_tables([existing_table, new_table], promote_options="default")
                final_table = self._apply_write_strategy(final_table, write_strategy, primary_key, watermark_column)
            else:
                final_table = existing_table
        else:
            final_table = new_table

        temp_file = target_file.with_name(f"{target_file.stem}_{uuid.uuid4().hex[:8]}.tmp.parquet")
        try:
            pq.write_table(final_table, str(temp_file), compression="SNAPPY")
            os.replace(temp_file, target_file)
            return {"num_rows": final_table.num_rows, "new_rows_extracted": new_table.num_rows}
        finally:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)

ingest_engine = IngestEngine()

