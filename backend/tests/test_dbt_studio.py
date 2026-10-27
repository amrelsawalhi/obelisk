import pytest
from httpx import AsyncClient, ASGITransport
from pathlib import Path
import uuid

from app.core.warehouse_resolver import get_warehouse_connection_id, set_warehouse_connection_id
from app.main import app
from app.engines.dbt_runner import dbt_runner


@pytest.fixture()
async def bound_warehouse(source_postgres_params):
    """Binds the live Postgres these dbt tests build against as the warehouse.

    Publishing is a decision now — there is no "newest Postgres row" fallback — so the
    test makes the same explicit choice an operator makes: register the database, then
    select it in Settings. Without this a Postgres target is refused with a 409.
    """
    transport = ASGITransport(app=app)
    previous = get_warehouse_connection_id()
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/api/catalog/connections",
            json={
                "name": f"zzdbt{uuid.uuid4().hex[:6]}",
                "source_type": "postgres",
                "host": source_postgres_params["host"],
                "port": int(source_postgres_params["port"]),
                "database": source_postgres_params["database"],
                "username": source_postgres_params["user"],
                "password": source_postgres_params["password"],
                "read_only": False,
            },
        )
        assert created.status_code == 200, created.text
        connection = created.json()
        bound = await client.put(
            "/api/dbt/warehouse", json={"connection_id": connection["id"]}
        )
        assert bound.status_code == 200, bound.text
    try:
        yield connection["id"]
    finally:
        set_warehouse_connection_id(previous)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.delete(f"/api/catalog/connections/{connection['id']}")


async def _seed_sample_data(client: AsyncClient) -> dict:
    """Creates the shipped sample tables on purpose.

    A build no longer fills an empty layer behind the operator's back, so the tests
    that read ``orders`` have to ask for it the same way an operator would.
    """
    response = await client.post("/api/dbt/sample-data", json={})
    assert response.status_code == 200, response.text
    return response.json()


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
async def test_dbt_studio_file_lifecycle_and_compile(source_postgres_params, bound_warehouse):
    """
    Tests creating a real model file models/staging/stg_test_unit.sql,
    reading it, compiling it with real dbt compile, and verifying output.

    ``dbt compile`` opens a connection to the target to resolve relations, so this
    needs a live warehouse -- the same fixture the preview and run tests use. Without
    it the test failed on any machine that had no Postgres on 5434 instead of saying
    what it actually required.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        rel_path = "models/staging/stg_test_unit.sql"
        content = (
            "{{ config(materialized='view') }}\n\n"
            "select\n"
            "    order_id,\n"
            "    customer_id,\n"
            "    amount\n"
            "from {{ source('landing', 'orders') }}\n"
            "where amount > 0\n"
        )

        # 1. Create / Write File
        created = True
        try:
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
            created = False
        finally:
            # A model file left behind makes every later `dbt parse` of the real
            # project fail, so cleanup must not depend on the happy path.
            if created:
                await client.delete(f"/api/dbt/file?path={rel_path}")


@pytest.mark.asyncio
async def test_dbt_studio_preview_data(source_postgres_params, bound_warehouse):
    """
    Tests previewing data from a compiled model against the real PostgreSQL container.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Preview reads the warehouse, which a build fills from the staging layer:
        # the sample tables have to exist, and only this call creates them.
        seeded = await _seed_sample_data(client)
        assert "orders" in seeded["tables"]
        run_res = await client.post(
            "/api/dbt/run", json={"selector": "stg_orders", "target": "postgres"}
        )
        assert run_res.status_code == 200
        assert run_res.json()["success"] is True, run_res.json().get("output", "")

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
async def test_dbt_studio_run_and_test(source_postgres_params, bound_warehouse):
    """
    Tests executing real dbt run and dbt test against schema validations.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Without data a build reports that plainly; with it, dbt runs for real.
        await _seed_sample_data(client)
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
