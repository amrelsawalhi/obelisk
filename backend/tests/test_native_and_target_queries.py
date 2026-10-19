import uuid
import pytest
from httpx import AsyncClient, ASGITransport
import psycopg2

from app.main import app
from app.models.connection import Connection
from app.core.vault import encrypt
from app.database import get_db

@pytest.mark.asyncio
async def test_query_targets_discovery():
    """Verifies GET /api/query/targets returns available targets including DuckDB default."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.get("/api/query/targets")
        assert res.status_code == 200
        targets = res.json()
        assert len(targets) >= 1
        duckdb_target = next((t for t in targets if t["id"] == "duckdb"), None)
        assert duckdb_target is not None
        assert duckdb_target["engine"] == "duckdb"
        assert duckdb_target["is_default"] is True
        assert "Cross-Source Federation" in duckdb_target["capabilities"]

@pytest.mark.asyncio
async def test_direct_postgres_execution_and_stored_procedure(source_postgres_params):
    """
    Real Integration Test (Zero Mocks):
    Registers a real PostgreSQL connection in the vault, executes direct queries,
    creates a native PL/pgSQL stored procedure, calls it, and verifies execution feedback.
    """
    pg_p = source_postgres_params
    rand_suffix = uuid.uuid4().hex[:6]
    conn_name = f"TestPG_{rand_suffix}"

    # 1. Register connection via Catalog API
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        reg_res = await ac.post(
            "/api/catalog/connections",
            json={
                "name": conn_name,
                "source_type": "postgres",
                "host": pg_p["host"],
                "port": pg_p["port"],
                "database": pg_p["database"],
                "username": pg_p["user"],
                "password": pg_p["password"],
                "options": {"sslmode": pg_p["sslmode"]},
            },
        )
        assert reg_res.status_code == 200
        conn_data = reg_res.json()
        conn_id = conn_data["id"]
        target_id = f"connection:{conn_id}"

        # 2. Verify connection appears in /api/query/targets
        targets_res = await ac.get("/api/query/targets")
        assert targets_res.status_code == 200
        target_entry = next((t for t in targets_res.json() if t["id"] == target_id), None)
        assert target_entry is not None
        assert target_entry["engine"] == "postgres"
        assert "Stored Procedures" in target_entry["capabilities"]

        # 3. Create a test table using direct native DDL
        table_name = f"sp_ledger_{rand_suffix}"
        ddl_sql = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            account_name VARCHAR(100),
            balance NUMERIC(12, 2)
        );
        INSERT INTO {table_name} (account_name, balance) VALUES ('Alpha Corp', 1500.00);
        """
        create_res = await ac.post(
            "/api/query/execute",
            json={
                "sql": ddl_sql,
                "target": target_id,
                "title": "Create Ledger Table",
            },
        )
        assert create_res.status_code == 200
        create_data = create_res.json()
        assert create_data["status"] == "success"
        assert create_data["target_engine"] == "postgres"

        # 4. Create a native Stored Procedure (PL/pgSQL)
        sp_name = f"proc_deposit_{rand_suffix}"
        sp_sql = f"""
        CREATE OR REPLACE PROCEDURE {sp_name}(acc_id INT, amount NUMERIC)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE {table_name}
            SET balance = balance + amount
            WHERE id = acc_id;
        END;
        $$;
        """
        sp_res = await ac.post(
            "/api/query/execute",
            json={
                "sql": sp_sql,
                "target": target_id,
                "title": "Create Deposit SP",
            },
        )
        assert sp_res.status_code == 200
        sp_data = sp_res.json()
        assert sp_data["status"] == "success"
        assert sp_data["command_tag"] == "CREATE PROCEDURE"

        # 5. Call the stored procedure using CALL proc(...)
        call_sql = f"CALL {sp_name}(1, 500.00);"
        call_res = await ac.post(
            "/api/query/execute",
            json={
                "sql": call_sql,
                "target": target_id,
                "title": "Call Deposit SP",
            },
        )
        assert call_res.status_code == 200
        call_data = call_res.json()
        assert call_data["status"] == "success"
        assert call_data["command_tag"] == "CALL"

        # 6. Verify updated balance via SELECT
        select_sql = f"SELECT id, account_name, balance FROM {table_name} WHERE id = 1;"
        select_res = await ac.post(
            "/api/query/execute",
            json={
                "sql": select_sql,
                "target": target_id,
                "title": "Verify Balance",
            },
        )
        assert select_res.status_code == 200
        select_data = select_res.json()
        assert select_data["row_count"] == 1
        assert len(select_data["rows"]) == 1
        assert float(select_data["rows"][0]["balance"]) == 2000.00
