import os
import uuid
import datetime
from pathlib import Path
import pytest
import psycopg2
from pymongo import MongoClient
import pyarrow.parquet as pq
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.database import AsyncSessionLocal
from app.models.connection import Connection
from app.core.vault import vault
from app.engines.dagster_client import dagster_client

def test_dagster_ingest_postgres_snapshot(source_postgres_params):
    """
    Physical Parquet verification for Postgres snapshot ingestion:
    1. Populates 3 rows in live Postgres.
    2. Runs Dagster asset materialization.
    3. Asserts physical file on disk exists, has num_rows == 3, and compression == 'SNAPPY'.
    """
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"pg_orders_{rand_suffix}"
    target_name = f"bronze_test_orders_{rand_suffix}"

    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE "{table_name}" (
                    id INT PRIMARY KEY,
                    sku VARCHAR(50),
                    amount NUMERIC(8, 2),
                    created_at TIMESTAMP WITHOUT TIME ZONE
                );
            """)
            cur.execute(f"""
                INSERT INTO "{table_name}" VALUES
                    (1, 'SKU-001', 19.99, '2026-01-01 10:00:00'),
                    (2, 'SKU-002', 49.50, '2026-01-02 11:00:00'),
                    (3, 'SKU-003', 99.00, '2026-01-03 12:00:00');
            """)

        conn_params = {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "username": p["user"],
            "password": p["password"],
            "sslmode": p["sslmode"],
        }

        run_res = dagster_client.trigger_ingest(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="snapshot",
            target_name=target_name,
        )

        assert run_res["success"] is True, f"Dagster run failed: {run_res['logs']}"
        assert run_res["run_id"]

        # Physical file verification
        storage_dir = Path(settings.duckdb_storage_path)
        parquet_path = storage_dir / f"{target_name}.parquet"
        assert parquet_path.exists(), f"Physical Parquet file {parquet_path} not found on disk!"

        meta = pq.read_metadata(str(parquet_path))
        assert meta.num_rows == 3
        # Assert compression is SNAPPY across all column chunks in the row group
        row_group = meta.row_group(0)
        for i in range(row_group.num_columns):
            assert row_group.column(i).compression == "SNAPPY"

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()


def test_dagster_ingest_postgres_incremental(source_postgres_params):
    """
    Physical Parquet verification for incremental watermark ingestion:
    1. Ingests initial 3 rows (ids 1, 2, 3).
    2. Inserts 2 new rows (ids 4, 5).
    3. Triggers incremental ingest with watermark_column='id'.
    4. Asserts final file has num_rows == 5 and SNAPPY compression.
    """
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"pg_incremental_{rand_suffix}"
    target_name = f"bronze_test_incremental_{rand_suffix}"

    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE "{table_name}" (
                    id INT PRIMARY KEY,
                    device_id VARCHAR(50),
                    reading FLOAT,
                    recorded_at TIMESTAMP WITHOUT TIME ZONE
                );
            """)
            cur.execute(f"""
                INSERT INTO "{table_name}" VALUES
                    (1, 'sensor-A', 22.5, '2026-02-01 08:00:00'),
                    (2, 'sensor-B', 19.8, '2026-02-01 08:05:00'),
                    (3, 'sensor-A', 23.1, '2026-02-01 08:10:00');
            """)

        conn_params = {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "username": p["user"],
            "password": p["password"],
            "sslmode": p["sslmode"],
        }

        # Step 1: Initial snapshot ingest
        res1 = dagster_client.trigger_ingest(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="snapshot",
            target_name=target_name,
        )
        assert res1["success"] is True

        parquet_path = Path(settings.duckdb_storage_path) / f"{target_name}.parquet"
        assert parquet_path.exists()
        assert pq.read_metadata(str(parquet_path)).num_rows == 3

        # Step 2: Insert 2 newer rows
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO "{table_name}" VALUES
                    (4, 'sensor-C', 31.0, '2026-02-01 08:15:00'),
                    (5, 'sensor-B', 20.4, '2026-02-01 08:20:00');
            """)

        # Step 3: Trigger incremental ingest on watermark column 'id'
        res2 = dagster_client.trigger_ingest(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="incremental",
            watermark_column="id",
            target_name=target_name,
        )
        assert res2["success"] is True

        # Step 4: Verify combined Parquet metadata and table contents
        meta2 = pq.read_metadata(str(parquet_path))
        assert meta2.num_rows == 5

        row_group = meta2.row_group(0)
        for i in range(row_group.num_columns):
            assert row_group.column(i).compression == "SNAPPY"

        table_data = pq.read_table(str(parquet_path))
        ids = table_data["id"].to_pylist()
        assert sorted(ids) == [1, 2, 3, 4, 5]

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()


def test_dagster_ingest_mongodb(source_mongo_params):
    """
    Physical Parquet verification for MongoDB ingestion to Bronze Snappy Parquet.
    """
    p = source_mongo_params
    rand_suffix = uuid.uuid4().hex[:6]
    coll_name = f"mongo_coll_{rand_suffix}"
    target_name = f"bronze_test_mongo_{rand_suffix}"

    uri = f"mongodb://{p['user']}:{p['password']}@{p['host']}:{p['port']}/?authSource={p['auth_source']}"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[p["database"]]
    coll = db[coll_name]

    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        coll.insert_many([
            {"log_id": 101, "level": "INFO", "msg": "System booted", "ts": now},
            {"log_id": 102, "level": "WARN", "msg": "Memory pressure high", "ts": now},
            {"log_id": 103, "level": "INFO", "msg": "Job scheduled", "ts": now},
            {"log_id": 104, "level": "ERROR", "msg": "Connection dropped", "ts": now},
        ])

        conn_params = {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "username": p["user"],
            "password": p["password"],
            "auth_db": p["auth_source"],
        }

        run_res = dagster_client.trigger_ingest(
            source_type="mongodb",
            connection_params=conn_params,
            entity=coll_name,
            mode="snapshot",
            target_name=target_name,
        )
        assert run_res["success"] is True

        parquet_path = Path(settings.duckdb_storage_path) / f"{target_name}.parquet"
        assert parquet_path.exists()
        meta = pq.read_metadata(str(parquet_path))
        assert meta.num_rows == 4
        for i in range(meta.row_group(0).num_columns):
            assert meta.row_group(0).column(i).compression == "SNAPPY"

    finally:
        coll.drop()
        client.close()


@pytest.mark.asyncio
async def test_ingest_api_trigger(source_postgres_params):
    """Tests the POST /api/ingest/trigger endpoint with registered Connection."""
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"api_orders_{rand_suffix}"
    target_name = f"bronze_api_orders_{rand_suffix}"

    # Setup Postgres table
    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{table_name}" (id INT PRIMARY KEY, name TEXT);')
            cur.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'alpha'), (2, 'beta');")

        # Register Connection in SQLite
        async with AsyncSessionLocal() as session:
            conn_record = Connection(
                name=f"test_ingest_pg_{rand_suffix}",
                source_type="postgres",
                host=p["host"],
                port=p["port"],
                database=p["database"],
                username=p["user"],
                encrypted_credentials=vault.encrypt(p["password"]),
                options={"sslmode": p["sslmode"]},
            )
            session.add(conn_record)
            await session.commit()
            await session.refresh(conn_record)
            connection_id = conn_record.id

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            res = await client.post(
                "/api/ingest/trigger",
                json={
                    "connection_id": connection_id,
                    "entity": table_name,
                    "mode": "snapshot",
                    "target_name": target_name,
                },
            )
            assert res.status_code == 200
            data = res.json()
            assert data["success"] is True
            assert data["run_id"]
            assert data["metadata"]["num_rows"] == 2

            parquet_path = Path(settings.duckdb_storage_path) / f"{target_name}.parquet"
            assert parquet_path.exists()
            assert pq.read_metadata(str(parquet_path)).num_rows == 2

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()


def test_sqlite_asset_catalog_registration(source_postgres_params):
    """
    Verifies that ingest_entity physically registers the Bronze asset in data/obelisk.db
    under the assets table.
    """
    import sqlite3
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"pg_asset_{rand_suffix}"
    target_name = f"bronze_asset_{rand_suffix}"

    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{table_name}" (id INT PRIMARY KEY, val TEXT);')
            cur.execute(f"INSERT INTO \"{table_name}\" VALUES (1, 'foo'), (2, 'bar');")

        from app.engines.ingest_engine import ingest_engine
        res = ingest_engine.ingest_entity(
            source_type="postgres",
            connection_params={
                "host": p["host"],
                "port": p["port"],
                "database": p["database"],
                "username": p["user"],
                "password": p["password"],
                "sslmode": p["sslmode"],
            },
            entity=table_name,
            mode="snapshot",
            target_name=target_name,
        )
        assert res["num_rows"] == 2

        # Verify physical registration in SQLite
        db_path = settings.DATABASE_URL.replace("sqlite+aiosqlite:///", "")
        sql_conn = sqlite3.connect(db_path)
        cur = sql_conn.cursor()
        cur.execute("SELECT name, layer, storage_uri, record_count, schema_metadata FROM assets WHERE name = ?;", (target_name,))
        row = cur.fetchone()
        sql_conn.close()

        assert row is not None, f"Asset '{target_name}' was not recorded in SQLite assets table!"
        name, layer, storage_uri, count, schema_json = row
        assert name == target_name
        # Layer is inherited from the target folder (medallion decoupled):
        # no target_folder means the file lands in the lakehouse root ('main').
        assert layer == "main"
        assert count == 2
        assert "parquet" in storage_uri

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()


def test_mongodb_incremental_date_watermark(source_mongo_params):
    """
    Proves that MongoDB native BSON Date fields compare natively during incremental ingestion,
    preventing the BSON Date vs String comparison bug.
    """
    p = source_mongo_params
    rand_suffix = uuid.uuid4().hex[:6]
    coll_name = f"mongo_dates_{rand_suffix}"
    target_name = f"bronze_dates_{rand_suffix}"

    uri = f"mongodb://{p['user']}:{p['password']}@{p['host']}:{p['port']}/?authSource={p['auth_source']}"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[p["database"]]
    coll = db[coll_name]

    try:
        t0 = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        t1 = datetime.datetime(2026, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)

        coll.insert_many([
            {"seq": 1, "ts": t0, "label": "event_one"},
            {"seq": 2, "ts": t1, "label": "event_two"},
        ])

        conn_params = {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "username": p["user"],
            "password": p["password"],
            "auth_db": p["auth_source"],
        }

        # Step 1: Snapshot
        from app.engines.ingest_engine import ingest_engine
        res1 = ingest_engine.ingest_entity(
            source_type="mongodb",
            connection_params=conn_params,
            entity=coll_name,
            mode="snapshot",
            target_name=target_name,
        )
        assert res1["num_rows"] == 2

        # Step 2: Insert newer documents with later BSON Dates
        t2 = datetime.datetime(2026, 1, 3, 12, 0, 0, tzinfo=datetime.timezone.utc)
        t3 = datetime.datetime(2026, 1, 4, 12, 0, 0, tzinfo=datetime.timezone.utc)
        coll.insert_many([
            {"seq": 3, "ts": t2, "label": "event_three"},
            {"seq": 4, "ts": t3, "label": "event_four"},
        ])

        # Step 3: Incremental ingest with Date watermark
        res2 = ingest_engine.ingest_entity(
            source_type="mongodb",
            connection_params=conn_params,
            entity=coll_name,
            mode="incremental",
            watermark_column="ts",
            target_name=target_name,
        )
        assert res2["new_rows_extracted"] == 2
        assert res2["num_rows"] == 4

        parquet_path = Path(settings.duckdb_storage_path) / f"{target_name}.parquet"
        assert pq.read_metadata(str(parquet_path)).num_rows == 4

    finally:
        coll.drop()
        client.close()


def test_incremental_empty_and_null_promotion(source_postgres_params):
    """
    Proves that incremental ingestion handles 0 new rows (empty delta) and nullable
    column type promotion without crashing.
    """
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"pg_nulls_{rand_suffix}"
    target_name = f"bronze_nulls_{rand_suffix}"

    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE "{table_name}" (
                    id INT PRIMARY KEY,
                    item VARCHAR(50),
                    discount FLOAT
                );
            """)
            cur.execute(f"""
                INSERT INTO "{table_name}" VALUES
                    (1, 'Alpha', 15.5),
                    (2, 'Beta', 25.0);
            """)

        conn_params = {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "username": p["user"],
            "password": p["password"],
            "sslmode": p["sslmode"],
        }

        from app.engines.ingest_engine import ingest_engine

        # Step 1: Initial snapshot
        res1 = ingest_engine.ingest_entity(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="snapshot",
            target_name=target_name,
        )
        assert res1["num_rows"] == 2

        # Step 2: Incremental run with 0 new rows
        res2 = ingest_engine.ingest_entity(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="incremental",
            watermark_column="id",
            target_name=target_name,
        )
        assert res2["new_rows_extracted"] == 0
        assert res2["num_rows"] == 2

        # Step 3: Insert new row where discount is NULL
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table_name}\" VALUES (3, 'Gamma', NULL);")

        res3 = ingest_engine.ingest_entity(
            source_type="postgres",
            connection_params=conn_params,
            entity=table_name,
            mode="incremental",
            watermark_column="id",
            target_name=target_name,
        )
        assert res3["new_rows_extracted"] == 1
        assert res3["num_rows"] == 3

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()

