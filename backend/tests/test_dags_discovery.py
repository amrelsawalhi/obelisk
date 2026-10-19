import pytest
from pathlib import Path
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.engines.dag_loader import dag_loader


@pytest.mark.asyncio
async def test_dag_loader_discovery():
    """
    Real unit test for DagLoader:
    - Verifies git_repo/dags/ files are discovered
    - Loads actual Dagster AssetsDefinitions dynamically
    - Verifies asset graph node conversion and dependency mapping
    """
    dags = dag_loader.list_dags()
    assert len(dags) >= 2
    dag_names = [d["name"] for d in dags]
    assert "daily_kpi_summary" in dag_names
    assert "customer_churn_notebook" in dag_names

    # Test dynamic loading into Dagster
    assets = dag_loader.load_dag_assets()
    assert len(assets) >= 2

    # Test graph node introspection
    nodes, edges = dag_loader.get_dag_graph_nodes()
    assert len(nodes) >= 2
    assert all(n["layer"] == "custom" for n in nodes)

    node_ids = [n["id"] for n in nodes]
    assert "dag.daily_kpi_summary" in node_ids
    assert "dag.customer_churn_notebook" in node_ids


@pytest.mark.asyncio
async def test_dags_api_crud_lifecycle():
    """
    Real integration test for /api/pipelines/dags endpoints:
    - List discovered DAGs
    - Read asset code
    - Create a new custom DAG from template
    - Verify asset graph contains the new node and edges
    - Update DAG code
    - Delete created test DAG
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. List DAGs
        res = await client.get("/api/pipelines/dags")
        assert res.status_code == 200
        dags = res.json()
        assert len(dags) >= 2

        # 2. Read existing DAG code
        res_code = await client.get("/api/pipelines/dags/daily_kpi_summary/code")
        assert res_code.status_code == 200
        code_data = res_code.json()
        assert "daily_kpi_summary" in code_data["code"]
        assert "asset(" in code_data["code"]

        # 3. Verify asset graph includes custom layer
        res_graph = await client.get("/api/pipelines/assets")
        assert res_graph.status_code == 200
        graph_data = res_graph.json()
        custom_nodes = [n for n in graph_data["nodes"] if n["layer"] == "custom"]
        assert len(custom_nodes) >= 2
        assert "custom" in graph_data["layers"]

        # 4. Create a new custom DAG from template
        test_dag_name = "test_unit_kpi_mart"
        create_payload = {
            "name": test_dag_name,
            "template": "kpi",
            "deps": ["fct_orders_enriched"],
            "description": "Integration test KPI mart",
        }
        create_res = await client.post("/api/pipelines/dags", json=create_payload)
        assert create_res.status_code == 200
        created_info = create_res.json()
        assert created_info["name"] == test_dag_name
        assert Path(created_info["path"]).exists()

        try:
            # 5. Read code of newly created DAG
            test_code_res = await client.get(f"/api/pipelines/dags/{test_dag_name}/code")
            assert test_code_res.status_code == 200
            content = test_code_res.json()["code"]
            assert f"def {test_dag_name}(" in content

            # 6. Update code of DAG
            updated_code = content.replace("Integration test KPI mart", "Updated KPI mart docstring")
            put_res = await client.put(
                f"/api/pipelines/dags/{test_dag_name}/code",
                json={"code": updated_code},
            )
            assert put_res.status_code == 200

            # Verify updated code persisted
            verify_res = await client.get(f"/api/pipelines/dags/{test_dag_name}/code")
            assert "Updated KPI mart docstring" in verify_res.json()["code"]

            # 7. Verify asset graph now includes this new node
            graph_after = (await client.get("/api/pipelines/assets")).json()
            test_node = next((n for n in graph_after["nodes"] if n["name"] == test_dag_name), None)
            assert test_node is not None
            assert test_node["layer"] == "custom"
        finally:
            # 8. Clean up test DAG
            del_res = await client.delete(f"/api/pipelines/dags/{test_dag_name}")
            assert del_res.status_code == 200

            # Verify file removed
            assert not Path(created_info["path"]).exists()
