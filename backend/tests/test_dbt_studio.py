import pytest
from httpx import AsyncClient, ASGITransport
from pathlib import Path

from app.main import app
from app.engines.dbt_runner import dbt_runner


@pytest.mark.asyncio
async def test_dbt_studio_project_tree():
    """Tests GET /api/dbt/tree against real git_repo/dbt_project/."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/dbt/tree")
        assert res.status_code == 200
        data = res.json()
        assert data["name"] == "dbt_project"
        assert data["type"] == "directory"
        child_names = [c["name"] for c in data.get("children", [])]
        assert "models" in child_names
        assert "dbt_project.yml" in child_names


@pytest.mark.asyncio
async def test_dbt_studio_file_lifecycle_and_compile():
    """
    Tests creating a real model file models/staging/stg_test_unit.sql,
    reading it, compiling it with real dbt compile, and verifying output.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        rel_path = "models/staging/stg_test_unit.sql"
        content = (
            "{{ config(materialized='view', schema=var('layer_staging')) }}\n\n"
            "select\n"
            "    order_id,\n"
            "    customer_id,\n"
            "    amount\n"
            "from {{ source('landing', 'orders') }}\n"
            "where amount > 0\n"
        )

        # 1. Create / Write File
        save_res = await client.post("/api/dbt/file", json={"path": rel_path, "content": content})
        assert save_res.status_code == 200
        assert save_res.json()["status"] == "success"

        # 2. Read File Back
        read_res = await client.get(f"/api/dbt/file?path={rel_path}")
        assert read_res.status_code == 200
        file_data = read_res.json()
        assert file_data["path"] == rel_path
        assert "amount > 0" in file_data["content"]

        # 3. Real dbt compile
        comp_res = await client.post("/api/dbt/compile", json={"model_name": "stg_test_unit", "target": "postgres"})
        assert comp_res.status_code == 200
        comp_data = comp_res.json()
        assert comp_data["success"] is True
        assert "compiled_sql" in comp_data
        assert "amount > 0" in comp_data["compiled_sql"]
        assert comp_data["duration_ms"] >= 0

        # 4. Lineage DAG for stg_test_unit
        lineage_res = await client.get("/api/dbt/lineage?model=stg_test_unit")
        assert lineage_res.status_code == 200
        lin_data = lineage_res.json()
        assert lin_data["model_name"] == "stg_test_unit"
        assert len(lin_data["nodes"]) >= 1

        # 5. Clean up created file
        del_res = await client.delete(f"/api/dbt/file?path={rel_path}")
        assert del_res.status_code == 200


@pytest.mark.asyncio
async def test_dbt_studio_preview_data(source_postgres_params):
    """
    Tests previewing data from a compiled model against the real PostgreSQL container.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # stg_orders is an existing model
        preview_res = await client.post(
            "/api/dbt/preview",
            json={"model_name": "stg_orders", "target": "postgres", "limit": 10},
        )
        assert preview_res.status_code == 200
        data = preview_res.json()
        assert data["status"] == "success"
        assert "columns" in data
        assert "rows" in data
        assert "order_id" in data["columns"]
        assert len(data["rows"]) > 0


@pytest.mark.asyncio
async def test_dbt_studio_run_and_test(source_postgres_params):
    """
    Tests executing real dbt run and dbt test against schema validations.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Run scoped to stg_orders
        run_res = await client.post(
            "/api/dbt/run",
            json={"selector": "stg_orders", "target": "postgres"},
        )
        assert run_res.status_code == 200
        run_data = run_res.json()
        assert run_data["success"] is True
        assert "stg_orders" in run_data["output"]

        # Test scoped to stg_orders
        test_res = await client.post(
            "/api/dbt/test",
            json={"selector": "stg_orders", "target": "postgres"},
        )
        assert test_res.status_code == 200
        test_data = test_res.json()
        assert test_data["success"] is True
        assert "PASS" in test_data["output"]
