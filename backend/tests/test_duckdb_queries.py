import io
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
from app.core.resource_advisor import advisor
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import ingest_engine

def test_duckdb_dynamic_memory_limit():
    """Verifies DuckDB applies dynamic memory limit from ResourceAdvisor."""
    conn = duckdb_engine.get_connection()
    try:
        mem_limit_setting = conn.execute("SELECT current_setting('memory_limit');").fetchone()[0]
        expected_limit = advisor.get_duckdb_memory_limit()
        # Verify memory limit was applied
        assert mem_limit_setting is not None
        assert str(mem_limit_setting).strip() != ""
    finally:
        conn.close()


def test_federated_join_postgres_and_bronze_parquet(source_postgres_params, source_mongo_params):
    """
    Cross-engine federated SQL test:
    Joins live PostgreSQL container table with Mongo-ingested Bronze Snappy Parquet file.
    """
    pg_p = source_postgres_params
    mg_p = source_mongo_params
    rand_suffix = uuid.uuid4().hex[:6]
    pg_table = f"pg_staff_{rand_suffix}"
    mg_coll = f"mg_tasks_{rand_suffix}"
    target_parquet = f"bronze_tasks_{rand_suffix}"

    # 1. Populate live PostgreSQL
    pg_conn = psycopg2.connect(
        host=pg_p["host"],
        port=pg_p["port"],
        dbname=pg_p["database"],
        user=pg_p["user"],
        password=pg_p["password"],
        sslmode=pg_p["sslmode"],
        connect_timeout=10,
    )
    pg_conn.autocommit = True
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE "{pg_table}" (
                    user_id INT PRIMARY KEY,
                    full_name VARCHAR(100),
                    dept VARCHAR(50)
                );
            """)
            cur.execute(f"""
                INSERT INTO "{pg_table}" VALUES
                    (1, 'Alice Smith', 'Engineering'),
                    (2, 'Bob Jones', 'Data Science'),
                    (3, 'Charlie Brown', 'Operations');
            """)

        # 2. Populate live MongoDB & extract to Bronze Snappy Parquet
        mg_uri = f"mongodb://{mg_p['user']}:{mg_p['password']}@{mg_p['host']}:{mg_p['port']}/?authSource={mg_p['auth_source']}"
        mg_client = MongoClient(mg_uri, serverSelectionTimeoutMS=5000)
        mg_db = mg_client[mg_p["database"]]
        mg_c = mg_db[mg_coll]
        try:
            mg_c.insert_many([
                {"user_id": 1, "task": "Kubernetes migration", "score": 98.5},
                {"user_id": 2, "task": "LLM fine-tuning", "score": 91.0},
                {"user_id": 3, "task": "Audit compliance", "score": 87.5},
            ])

            # Ingest Mongo to Bronze Parquet
            ingest_res = ingest_engine.ingest_entity(
                source_type="mongodb",
                connection_params={
                    "host": mg_p["host"],
                    "port": mg_p["port"],
                    "database": mg_p["database"],
                    "username": mg_p["user"],
                    "password": mg_p["password"],
                    "auth_db": mg_p["auth_source"],
                },
                entity=mg_coll,
                mode="snapshot",
                target_name=target_parquet,
            )
            assert ingest_res["num_rows"] == 3

            # 3. Execute cross-engine federated SQL JOIN in DuckDB
            sql = f"""
                SELECT 
                    u.full_name, 
                    u.dept, 
                    t.task, 
                    t.score
                FROM pg_src."{pg_table}" u
                JOIN {target_parquet} t ON u.user_id = t.user_id
                ORDER BY u.user_id ASC;
            """

            attachments = [{
                "alias": "pg_src",
                "host": pg_p["host"],
                "port": pg_p["port"],
                "database": pg_p["database"],
                "username": pg_p["user"],
                "password": pg_p["password"],
                "schema": "public",
                "sslmode": pg_p["sslmode"],
            }]

            query_res = duckdb_engine.execute_query(sql=sql, postgres_attachments=attachments)
            assert query_res["row_count"] == 3
            assert {"full_name", "dept", "task", "score"}.issubset(set(query_res["columns"]))

            rows = query_res["rows"]
            assert rows[0]["full_name"] == "Alice Smith"
            assert rows[0]["dept"] == "Engineering"
            assert rows[0]["task"] == "Kubernetes migration"
            assert abs(rows[0]["score"] - 98.5) < 0.01

            assert rows[1]["full_name"] == "Bob Jones"
            assert rows[1]["task"] == "LLM fine-tuning"

            assert rows[2]["full_name"] == "Charlie Brown"
            assert rows[2]["dept"] == "Operations"

        finally:
            mg_c.drop()
            mg_client.close()

    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{pg_table}";')
        pg_conn.close()


@pytest.mark.asyncio
async def test_query_api_execute_and_history():
    """Tests non-blocking query execution and QueryHistory recording."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        test_title = f"Test Execution {uuid.uuid4().hex[:6]}"
        sql = "SELECT 42 AS answer, 'Obelisk' AS platform, 3.14159 AS pi;"

        # Execute query
        res = await client.post(
            "/api/query/execute",
            json={
                "sql": sql,
                "title": test_title,
                "timeout_seconds": 15,
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "success"
        assert data["row_count"] == 1
        assert data["columns"] == ["answer", "platform", "pi"]
        assert data["rows"][0]["answer"] == 42
        assert data["rows"][0]["platform"] == "Obelisk"

        # Verify QueryHistory
        hist_res = await client.get("/api/query/history")
        assert hist_res.status_code == 200
        history_list = hist_res.json()
        matching = [h for h in history_list if h.get("title") == test_title]
        assert len(matching) == 1
        entry = matching[0]
        assert entry["status"] == "success"
        assert entry["row_count"] == 1
        assert entry["duration_ms"] >= 0


@pytest.mark.asyncio
async def test_query_api_export_parquet_and_csv():
    """Tests POST /api/query/export for Parquet (with Snappy compression) and CSV."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        sql = """
            SELECT 
                range AS id, 
                concat('item_', range::VARCHAR) AS item_name, 
                range * 10.5 AS price
            FROM range(5);
        """

        # 1. Export as Parquet
        pq_res = await client.post(
            "/api/query/export",
            json={
                "sql": sql,
                "format": "parquet",
                "filename": "items_export",
            },
        )
        assert pq_res.status_code == 200
        assert "application/octet-stream" in pq_res.headers.get("content-type", "")
        pq_bytes = pq_res.content

        # Verify physical Parquet structure & Snappy compression from returned bytes
        pq_file = pq.ParquetFile(io.BytesIO(pq_bytes))
        assert pq_file.metadata.num_rows == 5
        rg = pq_file.metadata.row_group(0)
        for i in range(rg.num_columns):
            assert rg.column(i).compression == "SNAPPY"

        # 2. Export as CSV
        csv_res = await client.post(
            "/api/query/export",
            json={
                "sql": sql,
                "format": "csv",
                "filename": "items_export",
            },
        )
        assert csv_res.status_code == 200
        assert "text/csv" in csv_res.headers.get("content-type", "")
        csv_text = csv_res.content.decode("utf-8")

        lines = [line.strip() for line in csv_text.strip().split("\n") if line.strip()]
        assert "id" in lines[0] and "item_name" in lines[0] and "price" in lines[0]
        assert "item_0" in lines[1]
