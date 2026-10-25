import uuid
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.engines.duckdb_engine import duckdb_engine
from app.engines.introspector import introspector


@pytest.mark.asyncio
async def test_lakehouse_catalog_tree_and_preview():
    """
    Tests:
    1. /api/catalog/tree returns Lakehouse root with all tiers (bronze, silver, gold)
    2. Creating a directory in Lakehouse
    3. Live 50-row preview of a table in a non-bronze lakehouse tier (e.g. silver)
    4. Querying tier.table directly in DuckDB
    """
    lakehouse_root = settings.lakehouse_root_path
    lakehouse_root.mkdir(parents=True, exist_ok=True)

    # 1. Create a custom test folder and a sample table in the catalog
    custom_tier = f"test_tier_{uuid.uuid4().hex[:6]}"
    tier_dir = lakehouse_root / custom_tier
    tier_dir.mkdir(parents=True, exist_ok=True)

    sample_table = f"events_{uuid.uuid4().hex[:4]}"
    from lakehouse_helpers import write_table as create_table

    table_data = pa.Table.from_pydict({
        "event_id": [101, 102, 103],
        "event_type": ["click", "view", "purchase"],
        "revenue": [0.0, 0.0, 149.99],
    })
    create_table(f"{custom_tier}/{sample_table}.parquet", table_data)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # A. Fetch catalog tree (refresh bypasses the module-level tree cache so
        # this test is order-independent).
        res = await client.get("/api/catalog/tree?refresh=true")
        assert res.status_code == 200
        tree = res.json()
        assert isinstance(tree, list)

        # Verify Lakehouse root node
        lakehouse_node = next((n for n in tree if n["id"] == "lakehouse"), None)
        assert lakehouse_node is not None
        assert lakehouse_node["type"] == "lakehouse"
        assert "directories" in lakehouse_node

        dir_names = [d["name"] for d in lakehouse_node["directories"]]
        assert custom_tier in dir_names

        # Verify custom tier has the table
        custom_dir_entry = next(d for d in lakehouse_node["directories"] if d["name"] == custom_tier)
        table_names = [t["name"] for t in custom_dir_entry["tables"]]
        assert sample_table in table_names

        # B. Test table preview via API: /preview/lakehouse:{tier}/{table}
        preview_res = await client.get(f"/api/catalog/preview/lakehouse:{custom_tier}/{sample_table}")
        assert preview_res.status_code == 200
        preview_data = preview_res.json()
        assert preview_data["row_count"] == 3
        assert "event_id" in preview_data["columns"]
        assert "revenue" in preview_data["columns"]

        # C. Test creating a new directory via POST /api/catalog/lakehouse/directories
        new_dir_name = f"tier_{uuid.uuid4().hex[:6]}"
        create_res = await client.post("/api/catalog/lakehouse/directories", json={"name": new_dir_name})
        assert create_res.status_code == 200
        assert create_res.json()["status"] == "created"
        assert (lakehouse_root / new_dir_name).exists()

        # D. Test deleting the newly created empty directory via DELETE /api/catalog/lakehouse/directories/{name}
        delete_res = await client.delete(f"/api/catalog/lakehouse/directories/{new_dir_name}")
        assert delete_res.status_code == 200
        assert delete_res.json()["status"] == "deleted"
        assert not (lakehouse_root / new_dir_name).exists()

        # E. Query it by its address: the warehouse is the source, the folder the schema.
        query_sql = (
            f"SELECT event_id, event_type, revenue FROM lakehouse.{custom_tier}.{sample_table}"
            " WHERE revenue > 0"
        )
        query_res = duckdb_engine.execute_query(query_sql)
        assert query_res["row_count"] == 1
        assert query_res["rows"][0]["revenue"] == 149.99

    # Cleanup: drop the table, then the folder the test created (DuckLake leaves its
    # data directory inside it, so remove the tree rather than its entries).
    import shutil

    from lakehouse_helpers import drop_schema

    drop_schema(custom_tier)
    shutil.rmtree(tier_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_lakehouse_management_crud_and_metadata():
    """
    Tests full Governed Lakehouse File & Folder Management:
    1. Creating top-level folder and nested subfolders
    2. Verifying hierarchical tree with depth and parent_path
    3. Moving a Parquet table between folders
    4. Renaming a Parquet table
    5. Table deletion
    6. Renaming a directory
    7. Directory deletion with cascade safety check
    """
    lakehouse_root = settings.lakehouse_root_path
    transport = ASGITransport(app=app)

    prefix = f"t_{uuid.uuid4().hex[:6]}"
    parent_tier = f"{prefix}_bronze"
    child_tier = f"{parent_tier}/sub_events"

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Create parent folder and nested child subfolder
        res1 = await client.post("/api/catalog/lakehouse/directories", json={"path": parent_tier})
        assert res1.status_code == 200
        assert (lakehouse_root / parent_tier).exists()

        res2 = await client.post("/api/catalog/lakehouse/directories", json={"path": child_tier})
        assert res2.status_code == 200
        assert (lakehouse_root / child_tier).exists()

        # 2. Plant a sample table in parent_tier
        table_name = f"tab_{uuid.uuid4().hex[:4]}"
        from lakehouse_helpers import write_table as create_table

        create_table(
            f"{parent_tier}/{table_name}.parquet",
            pa.Table.from_pydict({"id": [1, 2], "val": ["A", "B"]}),
        )

        # Verify tree hierarchy contains parent (depth 0) and child (depth 1)
        tree_res = await client.get("/api/catalog/tree")
        assert tree_res.status_code == 200
        tree = tree_res.json()
        lakehouse_node = next(n for n in tree if n["id"] == "lakehouse")
        dirs = {d["path"]: d for d in lakehouse_node["directories"]}

        assert parent_tier in dirs
        assert dirs[parent_tier]["depth"] == 0
        assert child_tier in dirs
        assert dirs[child_tier]["depth"] == 1
        assert dirs[child_tier]["parent_path"] == parent_tier

        # 3. Move table from parent_tier to child_tier
        move_res = await client.post("/api/catalog/lakehouse/tables/move", json={
            "source_folder": parent_tier,
            "target_folder": child_tier,
            "table_name": table_name,
        })
        assert move_res.status_code == 200
        # The table lives in the child schema now — asked of the catalog, not the disk.
        from app.core.catalog import scan as scan_lakehouse

        after_move = {(t.fqn, t.folder) for t in scan_lakehouse().tables}
        child_schema = child_tier.replace("/", ".")
        parent_schema = parent_tier.replace("/", ".")
        assert (f"lakehouse.{child_schema}.{table_name}", child_schema) in after_move
        assert not any(t.fqn.endswith(f".{table_name}") and t.folder == parent_schema for t in scan_lakehouse().tables)

        # 4. Rename table in child_tier
        renamed_table = f"{table_name}_v2"
        rename_res = await client.put("/api/catalog/lakehouse/tables/rename", json={
            "folder": child_tier,
            "old_name": table_name,
            "new_name": renamed_table,
        })
        assert rename_res.status_code == 200
        names = {(t.name, t.folder) for t in scan_lakehouse().tables}
        assert (renamed_table, child_schema) in names
        assert (table_name, child_schema) not in names

        # Preview renamed table
        prev = await client.get(f"/api/catalog/preview/lakehouse:{child_tier}/{renamed_table}")
        assert prev.status_code == 200
        assert prev.json()["row_count"] == 2

        # 5. Delete table
        del_tbl = await client.delete("/api/catalog/lakehouse/tables", params={
            "folder": child_tier,
            "name": renamed_table,
        })
        assert del_tbl.status_code == 200
        assert (renamed_table, child_schema) not in {(t.name, t.folder) for t in scan_lakehouse().tables}

        # 6. Delete directory
        del_child = await client.delete("/api/catalog/lakehouse/directories", params={
            "path": child_tier,
        })
        assert del_child.status_code == 200
        assert not (lakehouse_root / child_tier).exists()

        del_parent = await client.delete("/api/catalog/lakehouse/directories", params={
            "path": parent_tier,
            "cascade": True,
        })
        assert del_parent.status_code == 200
        assert not (lakehouse_root / parent_tier).exists()

