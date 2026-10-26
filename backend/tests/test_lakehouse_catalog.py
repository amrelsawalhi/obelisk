import uuid
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.core.catalog import scan as scan_lakehouse
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


@pytest.mark.asyncio
async def test_a_folder_rename_moves_the_tables_in_the_catalog():
    """DuckLake derives a table's files from its schema, so a rename has to move the tables.

    Renaming the directory alone left every pointer aimed at a folder that no longer held the
    files — the data was still there, and unreachable.
    """
    import shutil

    from lakehouse_helpers import drop_schema, unique_name, write_table

    lakehouse_root = settings.lakehouse_root_path
    old = unique_name("zzren")
    new = f"{old}moved"
    (lakehouse_root / old).mkdir(parents=True, exist_ok=True)
    try:
        write_table(f"{old}/orders.parquet", pa.Table.from_pydict({"id": [1, 2]}))
        write_table(f"{old}/sub/events.parquet", pa.Table.from_pydict({"id": [7]}))

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                "/api/catalog/lakehouse/directories/rename",
                json={"path": old, "new_name": new},
            )

        assert res.status_code == 200, res.text
        assert res.json()["tables_moved"] == 2

        after = {(table.name, table.folder) for table in scan_lakehouse().tables}
        assert ("orders", new) in after, "the table moved with the folder"
        assert ("events", f"{new}.sub") in after, "and so did the nested folder's"
        assert not any(folder == old or folder.startswith(f"{old}.") for _, folder in after), (
            "the old schema must not linger beside the new one"
        )
    finally:
        drop_schema(old)
        shutil.rmtree(lakehouse_root / old, ignore_errors=True)
        shutil.rmtree(lakehouse_root / new, ignore_errors=True)


@pytest.mark.asyncio
async def test_a_rename_refuses_to_overwrite_a_table_of_the_same_name():
    """Nothing moves when the destination is taken: those rows are not the user's to lose."""
    import shutil

    from lakehouse_helpers import drop_schema, unique_name, write_table

    from app.api.routes_catalog import _rename_directory_sync

    lakehouse_root = settings.lakehouse_root_path
    left = unique_name("zzcola")
    right = unique_name("zzcolb")
    (lakehouse_root / left).mkdir(parents=True, exist_ok=True)
    try:
        write_table(f"{left}/orders.parquet", pa.Table.from_pydict({"id": [1]}))
        write_table(f"{right}/orders.parquet", pa.Table.from_pydict({"id": [2]}))

        moved, skipped = _rename_directory_sync(left, right)

        assert (moved, skipped) == (0, 1)
        after = {(table.name, table.folder) for table in scan_lakehouse().tables}
        assert ("orders", left) in after, "the source table is exactly where it was"
        assert ("orders", right) in after
    finally:
        drop_schema(left)
        drop_schema(right)
        shutil.rmtree(lakehouse_root / left, ignore_errors=True)
        shutil.rmtree(lakehouse_root / right, ignore_errors=True)


@pytest.mark.asyncio
async def test_deleting_a_folder_with_tables_needs_confirmation():
    """The count comes from the catalog: a walk of the folder cannot see DuckLake's tables."""
    import shutil

    from lakehouse_helpers import drop_schema, unique_name, write_table

    lakehouse_root = settings.lakehouse_root_path
    folder = unique_name("zzdel")
    (lakehouse_root / folder).mkdir(parents=True, exist_ok=True)
    try:
        write_table(f"{folder}/orders.parquet", pa.Table.from_pydict({"id": [1, 2]}))

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            refused = await client.delete(
                "/api/catalog/lakehouse/directories", params={"path": folder}
            )
            assert refused.status_code == 400
            assert "1 table" in refused.json()["detail"]
            assert ("orders", folder) in {
                (table.name, table.folder) for table in scan_lakehouse().tables
            }, "a refused delete changes nothing"

            confirmed = await client.delete(
                "/api/catalog/lakehouse/directories",
                params={"path": folder, "cascade": True},
            )

        assert confirmed.status_code == 200, confirmed.text
        assert confirmed.json()["tables_dropped"] == 1
        assert not (lakehouse_root / folder).exists(), "the folder is gone from disk"
        assert ("orders", folder) not in {
            (table.name, table.folder) for table in scan_lakehouse().tables
        }
    finally:
        drop_schema(folder)
        shutil.rmtree(lakehouse_root / folder, ignore_errors=True)


@pytest.mark.asyncio
async def test_a_preview_answers_for_the_folder_it_was_asked_about():
    """Two folders may hold a table of the same name; the preview must not pick the first."""
    import shutil

    from lakehouse_helpers import drop_schema, unique_name, write_table

    lakehouse_root = settings.lakehouse_root_path
    left = unique_name("zzpva")
    right = unique_name("zzpvb")
    (lakehouse_root / left).mkdir(parents=True, exist_ok=True)
    (lakehouse_root / right).mkdir(parents=True, exist_ok=True)
    try:
        write_table(f"{left}/orders.parquet", pa.Table.from_pydict({"id": [1, 2, 3]}))
        write_table(f"{right}/orders.parquet", pa.Table.from_pydict({"id": [9]}))

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            left_preview = await client.get(f"/api/catalog/preview/lakehouse:{left}/orders")
            right_preview = await client.get(f"/api/catalog/preview/lakehouse:{right}/orders")

        assert left_preview.status_code == 200, left_preview.text
        assert right_preview.status_code == 200, right_preview.text
        assert left_preview.json()["row_count"] == 3
        assert right_preview.json()["row_count"] == 1, "the other folder's orders, not this one's"
    finally:
        drop_schema(left)
        drop_schema(right)
        shutil.rmtree(lakehouse_root / left, ignore_errors=True)
        shutil.rmtree(lakehouse_root / right, ignore_errors=True)

@pytest.mark.asyncio
async def test_a_folder_is_a_schema_and_outlives_neither_its_tables_nor_the_disk():
    """The tree's folders are schemas, so the folder goes when its last table does.

    A folder a user created keeps its ``.gitkeep`` and stays. A folder that exists only as a
    schema — no directory left, nothing in it — is still deletable: what the tree shows must
    never be something the API refuses to remove.
    """
    import shutil

    from app.core.catalog import quote_ident
    from lakehouse_helpers import drop_schema, unique_name, write_table

    lakehouse_root = settings.lakehouse_root_path
    user_folder = unique_name("zzkeep")
    plain_folder = unique_name("zzdrop")
    schema_only = unique_name("zzschema")
    for folder in (user_folder, plain_folder):
        (lakehouse_root / folder).mkdir(parents=True, exist_ok=True)
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # A folder the user made, with a table in it.
            assert (
                await client.post(
                    "/api/catalog/lakehouse/directories", json={"path": user_folder}
                )
            ).status_code == 200
            write_table(f"{user_folder}/orders.parquet", pa.Table.from_pydict({"id": [1, 2]}))
            # A folder with nothing to mark it as anybody's.
            write_table(f"{plain_folder}/orders.parquet", pa.Table.from_pydict({"id": [3]}))

            for folder in (user_folder, plain_folder):
                dropped = await client.delete(
                    "/api/catalog/lakehouse/tables", params={"folder": folder, "name": "orders"}
                )
                assert dropped.status_code == 200, dropped.text

            tree = (await client.get("/api/catalog/tree?refresh=true")).json()
            folders = {
                directory["path"]
                for node in tree
                if node["id"] == "lakehouse"
                for directory in node["directories"]
            }
            assert user_folder in folders, "the .gitkeep is what makes it the user's folder"
            assert plain_folder not in folders, "an empty schema is not a folder any more"
            assert not (lakehouse_root / user_folder).exists() or (
                lakehouse_root / user_folder / ".gitkeep"
            ).is_file(), "the user's folder is still on disk, marked"
            assert not (lakehouse_root / plain_folder).exists(), (
                "the directory the dropped table left is swept too"
            )

            # A schema nobody gave a directory: still listed, still deletable.
            write_table(f"{schema_only}/temp.parquet", pa.Table.from_pydict({"id": [4]}))
            conn = duckdb_engine.get_connection()
            try:
                conn.execute(f"DROP TABLE lakehouse.{quote_ident(schema_only)}.temp")
            finally:
                conn.close()
            shutil.rmtree(lakehouse_root / schema_only, ignore_errors=True)

            tree = (await client.get("/api/catalog/tree?refresh=true")).json()
            assert schema_only in {
                directory["path"]
                for node in tree
                if node["id"] == "lakehouse"
                for directory in node["directories"]
            }
            removed = await client.delete(
                "/api/catalog/lakehouse/directories", params={"path": schema_only}
            )
            assert removed.status_code == 200, removed.text
    finally:
        for folder in (user_folder, plain_folder, schema_only):
            drop_schema(folder)
            shutil.rmtree(lakehouse_root / folder, ignore_errors=True)

