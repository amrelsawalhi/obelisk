import os
import uuid
import time
import shutil
import urllib.request
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app.config import settings
from app.main import app
from app.engines.marimo_service import marimo_service
import obelisk


@pytest.fixture(scope="module")
def sample_bronze_dataset():
    """Provisions a real table in the bronze schema for SDK verification."""
    from app.engines.duckdb_engine import duckdb_engine

    table_name = f"sdk_orders_{uuid.uuid4().hex[:6]}"
    table_data = pa.Table.from_pydict({
        "order_id": [101, 102, 103],
        "customer": ["Alice", "Bob", "Charlie"],
        "amount": [150.50, 89.20, 240.00],
    })
    obelisk.save(table_data, f"bronze.{table_name}", sync_postgres=False)

    yield table_name, None

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f'DROP TABLE IF EXISTS lakehouse."bronze".{table_name}')
    finally:
        conn.close()


def test_obelisk_table_and_formats(sample_bronze_dataset):
    """Verifies that obelisk.table() resolves datasets and converts seamlessly across engines."""
    table_name, _target_path = sample_bronze_dataset
    tbl = obelisk.table(f"bronze.{table_name}")

    assert tbl.exists() is True

    # 1. PyArrow conversion
    arrow_tbl = tbl.to_arrow()
    assert isinstance(arrow_tbl, pa.Table)
    assert arrow_tbl.num_rows == 3
    assert "customer" in arrow_tbl.column_names

    # 2. Polars conversion
    polars_df = tbl.to_polars()
    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.shape == (3, 3)
    assert polars_df["amount"].sum() == 479.70

    # 3. Pandas DataFrame conversion
    pandas_df = tbl.to_df()
    assert len(pandas_df) == 3

    # 4. DuckDB relation conversion
    duck_rel = tbl.to_duckdb()
    res = duck_rel.filter("amount > 100").fetchall()
    assert len(res) == 2


def test_obelisk_query():
    """Verifies that obelisk.query() executes analytical SQL in DuckDB and returns Polars."""
    df = obelisk.query("SELECT 42 AS answer, 'obelisk_engine' AS engine, 3.14159 AS pi_val")
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 3)
    assert df["answer"][0] == 42
    assert df["engine"][0] == "obelisk_engine"


@pytest.mark.asyncio
async def test_obelisk_save_writes_a_file_the_catalog_can_see():
    """``save`` writes the table; the catalog finds it because the file exists."""
    gold_stem = f"gold_revenue_{uuid.uuid4().hex[:6]}"
    df = pl.DataFrame({
        "region": ["North", "South", "East", "West"],
        "total_revenue": [12500.0, 9400.0, 18300.0, 7200.0],
    })

    res = obelisk.save(df, f"gold.{gold_stem}", sync_postgres=False)
    assert res["status"] == "success"
    assert res["folder"] == "gold"
    assert res["fqn"] == f"lakehouse.gold.{gold_stem}"
    assert res["sql_name"] == f"lakehouse.gold.{gold_stem}"
    assert res["num_rows"] == 4

    # The save landed in the catalog, and nothing was registered by hand.
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            objects = (await client.get("/api/catalog/objects")).json()
        row = next(obj for obj in objects if obj["fqn"] == res["fqn"])
        assert row["row_count"] == 4
        assert row["folder"] == "gold"
    finally:
        from app.engines.duckdb_engine import duckdb_engine

        conn = duckdb_engine.get_connection()
        try:
            conn.execute(f'DROP TABLE IF EXISTS lakehouse."gold".{gold_stem}')
        finally:
            conn.close()
        import sqlite3

        with sqlite3.connect(settings.sqlite_db_path) as conn:
            conn.execute("DELETE FROM catalog_objects WHERE fqn = ?;", (res["fqn"],))
            conn.commit()


def test_obelisk_spark_builder_configuration():
    """Verifies obelisk.spark() builder configures local hardware caps and Spark Connect."""
    # 1. Local builder configuration check
    builder = obelisk.spark(get_or_create=False)
    assert builder._options.get("spark.master") == "local[*]"
    assert builder._options.get("spark.ui.enabled") == "false"
    assert builder._options.get("spark.sql.execution.arrow.pyspark.enabled") == "true"
    assert "g" in builder._options.get("spark.driver.memory", "")

    # 2. Remote Spark Connect builder check
    remote_builder = obelisk.spark(get_or_create=False, remote_url="sc://spark.enterprise.internal:15002")
    assert remote_builder._options.get("spark.remote") == "sc://spark.enterprise.internal:15002"


def test_obelisk_spark_arrow_memory_transfer(sample_bronze_dataset):
    """
    If Java 17+ runtime is available on the machine, verifies that obelisk.table().to_spark()
    transfers data via in-memory Arrow/Pandas, completely avoiding Hadoop's winutils.exe.
    """
    from app.sdk.obelisk import is_java_compatible
    if not is_java_compatible():
        pytest.skip("Java 17+ runtime not installed or configured on host; skipping live Spark JVM execution.")

    table_name, _ = sample_bronze_dataset
    tbl = obelisk.table(f"bronze.{table_name}")

    try:
        spark_sess = obelisk.spark(get_or_create=True)
        spark_df = tbl.to_spark(spark_session=spark_sess)
        assert spark_df.count() == 3
        assert "customer" in spark_df.columns
    finally:
        try:
            from pyspark.sql import SparkSession
            active = SparkSession.getActiveSession()
            if active:
                active.stop()
        except Exception:
            pass


def test_marimo_service_lifecycle_and_http():
    """
    Verifies that Marimo headless server starts on port 2718, returns 200 on /notebooks,
    and terminates process tree cleanly without leaving orphaned port listeners.
    """
    # Verify starter notebook exists
    notebooks = marimo_service.list_notebooks()
    names = [nb["name"] for nb in notebooks]
    assert "Quickstart_Analytics" in names

    # Create a dynamic notebook
    rand_name = f"test_nb_{uuid.uuid4().hex[:6]}"
    create_res = marimo_service.create_notebook(rand_name)
    assert create_res["status"] == "created"
    nb_file = Path(create_res["path"])
    assert nb_file.exists()

    try:
        # Start headless Marimo server
        start_res = marimo_service.start(port=2718, wait_timeout=12.0)
        assert start_res["status"] in ("running", "already_running")
        assert marimo_service.is_running() is True

        # Perform live HTTP probe
        req = urllib.request.Request(marimo_service.url, headers={"User-Agent": "ObeliskTest"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            assert resp.status == 200

        # Verify status metadata
        status_info = marimo_service.get_status()
        assert status_info["is_running"] is True
        assert status_info["port"] == 2718
        assert status_info["notebook_count"] >= 2

    finally:
        # Guarantee zero orphan processes
        stop_res = marimo_service.stop()
        assert stop_res["status"] == "stopped"
        assert marimo_service.is_running() is False
        if nb_file.exists():
            nb_file.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_notebook_api_endpoints():
    """Verifies REST endpoints for Marimo notebooks and server status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. List notebooks
        resp = await client.get("/api/notebooks")
        assert resp.status_code == 200
        data = resp.json()
        assert "notebooks" in data
        assert data["total"] >= 1

        # 2. Server status
        status_resp = await client.get("/api/notebooks/server/status")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert "is_running" in status_data
        assert "port" in status_data

        # 3. Create notebook
        nb_name = f"api_nb_{uuid.uuid4().hex[:6]}"
        create_resp = await client.post("/api/notebooks", json={"name": nb_name})
        assert create_resp.status_code == 200
        assert create_resp.json()["name"] == nb_name

        # Cleanup created file
        target = Path(marimo_service.notebooks_dir) / f"{nb_name}.py"
        if target.exists():
            target.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_analyze_notebook_template_and_overwrite():
    """Verifies that creating an Explore_ notebook generates df = obelisk.table(...) and updates stubs."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        nb_name = f"Explore_test_{uuid.uuid4().hex[:6]}"
        target = Path(marimo_service.notebooks_dir) / f"{nb_name}.py"

        try:
            # 1. Create with Explore_ prefix: should automatically contain obelisk.table
            resp1 = await client.post("/api/notebooks", json={"name": nb_name})
            assert resp1.status_code == 200
            assert target.exists()
            content1 = target.read_text(encoding="utf-8")
            assert "obelisk.table" in content1
            assert "df = obelisk.table" in content1

            # 2. Re-create with custom content & overwrite=True
            custom_code = "# custom test content with obelisk.table('bronze.custom')"
            resp2 = await client.post("/api/notebooks", json={"name": nb_name, "content": custom_code, "overwrite": True})
            assert resp2.status_code == 200
            content2 = target.read_text(encoding="utf-8")
            assert content2 == custom_code

        finally:
            if target.exists():
                target.unlink(missing_ok=True)
