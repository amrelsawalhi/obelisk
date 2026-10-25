"""Sample data exists only when someone asks for it.

The dbt runner used to write six demo Parquet files into the staging folder the first
time a build found it empty, so a green first run could be measuring data the operator
never created — and the catalog showed tables nobody had made. These tests pin the
replacement contract: an empty warehouse is reported as empty, and the sample tables
appear only after ``POST /api/dbt/sample-data``.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.core.catalog import path_for
from app.core.storage import warehouse_storage
from app.engines.dbt_runner import NoSourceData, dbt_runner
from app.main import app

SAMPLE_TABLES = {"orders", "customers", "ecommerce_orders", "ecommerce_customers"}


@pytest.fixture
def empty_warehouse():
    """A warehouse with nothing in it, for the length of one test.

    "Fresh install" is a lakehouse directory with no files, and the session lakehouse
    is shared with every other test, so the only honest way to reproduce one is to
    point the setting at an empty scratch directory and put it back afterwards.
    """
    original = settings.STORAGE_URI
    with tempfile.TemporaryDirectory(prefix="obelisk-empty-warehouse-") as tmp:
        settings.STORAGE_URI = Path(tmp).as_uri()
        try:
            yield Path(tmp)
        finally:
            settings.STORAGE_URI = original


def staging_folder() -> str:
    """The folder the project's staging models read from."""
    return dbt_runner._dbt_folders()["staging"]


def folder_tables(folder: str) -> set[str]:
    """The tables one warehouse schema holds — asked of the catalog, not the disk."""
    from app.core.catalog import scan

    schema = folder.strip("/").replace("/", ".")
    return {table.name for table in scan().tables if (table.folder or "") == schema}


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_a_build_with_no_data_fails_plainly_and_writes_nothing(empty_warehouse):
    """A fresh install gets an explanation, not a warehouse full of demo tables."""
    folder = staging_folder()
    assert folder_tables(folder) == set()

    async with _client() as client:
        response = await client.post(
            "/api/dbt/run", json={"selector": "*", "target": "postgres"}
        )

    assert response.status_code == 200, response.text
    result = response.json()
    assert result["success"] is False
    assert result["reason"] == "no_source_data"
    assert folder in result["output"]
    assert "POST /api/dbt/sample-data" in result["output"]

    # The whole point: the failure did not leave anything behind.
    assert folder_tables(folder) == set()
    assert list(empty_warehouse.rglob("*.parquet")) == []


def test_sync_refuses_an_empty_folder_instead_of_seeding_it(empty_warehouse):
    """The other implicit seeding call is gone too, and says so."""
    params = {
        "host": "127.0.0.1",
        "port": 5434,
        "database": "obelisk",
        "username": "obelisk",
        "password": "obelisk",
    }

    with pytest.raises(NoSourceData) as raised:
        dbt_runner.sync_folder_to_postgres(params, folder=staging_folder())

    assert "POST /api/dbt/sample-data" in str(raised.value)
    assert list(empty_warehouse.rglob("*.parquet")) == []


@pytest.mark.asyncio
async def test_sample_data_is_written_only_when_requested(empty_warehouse):
    """The endpoint creates the shipped tables, and is idempotent."""
    folder = staging_folder()

    async with _client() as client:
        response = await client.post("/api/dbt/sample-data", json={})
        assert response.status_code == 200, response.text
        payload = response.json()

        assert folder_tables(folder) == SAMPLE_TABLES
        assert set(payload["created"]) == SAMPLE_TABLES
        assert set(payload["tables"]) == SAMPLE_TABLES
        assert payload["folder"] == folder

        # A second call must not claim to have created anything.
        again = (await client.post("/api/dbt/sample-data", json={})).json()
        assert again["created"] == []
        assert folder_tables(folder) == SAMPLE_TABLES


@pytest.mark.asyncio
async def test_sample_data_can_target_a_named_folder_and_rejects_an_illegal_one(
    empty_warehouse,
):
    folder = "zz_sample_probe"

    async with _client() as client:
        created = await client.post("/api/dbt/sample-data", json={"folder": folder})
        assert created.status_code == 200, created.text
        assert created.json()["folder"] == folder
        # The sample table exists in the lakehouse — asked of the catalog, not the disk.
        assert f"orders" in folder_tables(folder)

        rejected = await client.post("/api/dbt/sample-data", json={"folder": "../escape"})
        assert rejected.status_code == 400
        assert "illegal" in rejected.json()["detail"].lower() or "may only" in rejected.json()["detail"].lower()


def test_the_guard_lifts_once_the_folder_holds_data(empty_warehouse):
    """The check reads the warehouse, so real data satisfies it without a build."""
    folder = staging_folder()
    assert folder_tables(folder) == set()

    seeded = dbt_runner.seed_sample_data()
    assert set(seeded["created"]) == SAMPLE_TABLES
    assert folder_tables(folder) == SAMPLE_TABLES


def test_sample_tables_land_where_the_catalog_says_they_do(empty_warehouse):
    """The address of a sample table is its path, like any other table."""
    from app.core.catalog import scan

    dbt_runner.seed_sample_data()

    result = scan(warehouse_storage())
    assert {table.name for table in result.tables} == SAMPLE_TABLES
    for table in result.tables:
        # A table's address is its folder and name; the bytes live in whatever files the
        # catalog says belong to it.
        assert table.path == f"{staging_folder()}/{table.name}"

