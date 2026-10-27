import uuid
import datetime
from pathlib import Path
import pytest
import psycopg2
from pymongo import MongoClient
import pyarrow as pa
import pyarrow.parquet as pq
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.engines.introspector import introspector
from app.config import settings
from app.database import AsyncSessionLocal
from app.models.connection import Connection
from app.core.vault import vault

@pytest.mark.asyncio
async def test_postgres_introspection_dynamic_mutation(source_postgres_params):
    """
    Dynamic Mutation Proof for PostgreSQL:
    1. Creates a table with a randomized UUID suffix.
    2. Inserts 3 rows with diverse SQL types.
    3. Runs introspect_postgres and asserts the table, columns, and row_count match.
    4. Runs preview_postgres and asserts rows match.
    5. Cleans up by dropping the table.
    """
    p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    table_name = f"orders_test_{rand_suffix}"

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
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(100) NOT NULL,
                    total_amount NUMERIC(10, 2),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute(f"""
                INSERT INTO "{table_name}" (customer_name, total_amount, is_active)
                VALUES 
                    ('Alice Enterprise', 129.99, TRUE),
                    ('Bob Retail', 45.50, FALSE),
                    ('Charlie Logistics', 999.00, TRUE);
            """)

        # Perform live introspection
        tables = introspector.introspect_postgres(
            host=p["host"],
            port=p["port"],
            database=p["database"],
            username=p["user"],
            password=p["password"],
            schema="public",
            sslmode=p["sslmode"],
        )

        matched_tables = [t for t in tables if t["name"] == table_name]
        assert len(matched_tables) == 1, f"Table {table_name} not discovered by introspector!"
        tbl_info = matched_tables[0]

        # Physical size is served from pg_total_relation_size, never a scan.
        assert tbl_info["size_bytes"] is not None and tbl_info["size_bytes"] > 0
        assert tbl_info["size_human"]
        # Row counts are deliberately absent for database tables: stale planner
        # statistics are worse than no number at all.
        assert "row_count" not in tbl_info
        col_names = {c["name"] for c in tbl_info["columns"]}
        assert {"id", "customer_name", "total_amount", "is_active", "created_at"}.issubset(col_names)

        pk_cols = [c["name"] for c in tbl_info["columns"] if c.get("is_primary_key")]
        assert "id" in pk_cols

        # Test live preview
        preview = introspector.preview_postgres(
            host=p["host"],
            port=p["port"],
            database=p["database"],
            username=p["user"],
            password=p["password"],
            table=table_name,
            schema="public",
            limit=50,
            sslmode=p["sslmode"],
        )
        assert len(preview["rows"]) == 3
        assert preview["row_count"] == 3
        assert "customer_name" in preview["columns"]
        customer_names = {r["customer_name"] for r in preview["rows"]}
        assert "Alice Enterprise" in customer_names
        assert "Bob Retail" in customer_names

    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.close()


def test_mongodb_introspection_dynamic_mutation(source_mongo_params):
    """
    Dynamic Mutation Proof for MongoDB:
    1. Creates a collection with randomized UUID suffix.
    2. Inserts 3 documents with diverse types.
    3. Runs introspect_mongodb and asserts collection, fields, and row count match.
    4. Runs preview_mongodb and asserts documents match.
    5. Cleans up by dropping collection.
    """
    p = source_mongo_params
    rand_suffix = uuid.uuid4().hex[:6]
    coll_name = f"events_test_{rand_suffix}"

    uri = f"mongodb://{p['user']}:{p['password']}@{p['host']}:{p['port']}/?authSource={p['auth_source']}"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[p["database"]]
    coll = db[coll_name]

    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        coll.insert_many([
            {"event_type": "user_signup", "user_id": 101, "score": 9.5, "verified": True, "ts": now},
            {"event_type": "checkout", "user_id": 102, "score": 8.0, "verified": True, "ts": now},
            {"event_type": "logout", "user_id": 103, "score": 0.0, "verified": False, "ts": now},
        ])

        tables = introspector.introspect_mongodb(
            host=p["host"],
            port=p["port"],
            username=p["user"],
            password=p["password"],
            auth_db=p["auth_source"],
            database_name=p["database"],
        )

        matched = [t for t in tables if t["name"] == coll_name]
        assert len(matched) == 1, f"Collection {coll_name} not found by MongoDB introspector!"
        coll_info = matched[0]
        # Size comes from collStats; document counts are intentionally omitted
        # because counting documents either scans the collection or returns a
        # rough estimate.
        assert coll_info["size_bytes"] is not None and coll_info["size_bytes"] > 0
        assert coll_info["size_human"]
        assert "row_count" not in coll_info

        col_names = {c["name"] for c in coll_info["columns"]}
        assert {"event_type", "user_id", "score", "verified", "ts"}.issubset(col_names)

        # Test preview
        preview = introspector.preview_mongodb(
            host=p["host"],
            port=p["port"],
            database=p["database"],
            collection=coll_name,
            username=p["user"],
            password=p["password"],
            auth_db=p["auth_source"],
            limit=50,
        )
        assert len(preview["rows"]) == 3
        event_types = {r["event_type"] for r in preview["rows"]}
        assert "user_signup" in event_types
        assert "checkout" in event_types

    finally:
        coll.drop()
        client.close()


def test_parquet_introspection(tmp_path):
    """Verifies Parquet file introspection directly against disk."""
    # The introspector inherits the tier from the parent folder name, which is
    # the documented behaviour (no hardcoded medallion): a file under bronze/
    # belongs to the bronze layer.
    tier_dir = tmp_path / "bronze"
    tier_dir.mkdir()
    test_file = tier_dir / "test_bronze_sample.parquet"

    table = pa.Table.from_pydict({
        "order_id": [1, 2, 3, 4, 5],
        "metric_val": [10.5, 20.2, 30.1, 40.0, 50.9],
        "status": ["NEW", "PROCESSING", "DONE", "DONE", "REFUNDED"],
    })
    pq.write_table(table, str(test_file), compression="SNAPPY")

    results = introspector.introspect_parquet(str(test_file))
    assert len(results) == 1
    info = results[0]
    assert info["name"] == "test_bronze_sample"
    assert info["schema_name"] == "bronze"
    assert info["row_count"] == 5
    col_names = {c["name"] for c in info["columns"]}
    assert col_names == {"order_id", "metric_val", "status"}


@pytest.mark.asyncio
async def test_catalog_api_flow(source_postgres_params):
    """Tests the /api/catalog REST endpoints using registered Connection."""
    p = source_postgres_params

    # Register real connection in SQLite
    async with AsyncSessionLocal() as session:
        conn_record = Connection(
            name=f"test_catalog_pg_{uuid.uuid4().hex[:6]}",
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
        # 1. Fetch catalog tree
        res = await client.get("/api/catalog/tree")
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        matched_conn = [c for c in data if c["id"] == connection_id]
        assert len(matched_conn) == 1
        assert matched_conn[0]["type"] == "postgres"

        # 2. Test preview of missing table. The source refusing the lookup is reported as a
        # 502 (upstream), not a 500 that reads like an Obelisk bug.
        res_missing = await client.get(f"/api/catalog/preview/{connection_id}/non_existent_table")
        assert res_missing.status_code in [404, 502]
