"""The lakehouse reports its own cost, and can compact and reclaim itself.

Both answers used to exist only in a shell script (``scripts/lakehouse_maintenance.py``), so
the Settings tabs had nothing to show and no way to reclaim bytes without someone opening a
terminal. These tests drive the two endpoints against the real catalog.
"""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.catalog import LOAD_TABLE_SUFFIX, lakehouse_connection
from app.config import settings
from app.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_diagnostics_reports_what_the_lakehouse_costs():
    """Every number the Storage tab shows comes back, and comes back as a number."""
    async with _client() as client:
        res = await client.get("/api/catalog/lakehouse/diagnostics")

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["lakehouse_root"], "the tab shows where the bytes live"
    assert body["duckdb_tmp_limit"] == "50GB" or body["duckdb_tmp_limit"]
    assert body["duckdb_memory_limit_per_slot"].endswith("GB")
    assert body["snapshot_count"] >= 1, "a lakehouse that was written to has a snapshot"
    assert isinstance(body["current_snapshot_id"], int), (
        "the tab pins a snapshot id, not just a count"
    )
    assert body["current_snapshot_id"] >= 0, (
        "DuckLake numbers its first snapshot 0, so a written lakehouse reports one"
    )
    assert body["obelisk_db_journal_mode"] in {
        "WAL",
        "DELETE",
        "TRUNCATE",
        "PERSIST",
        "MEMORY",
        "OFF",
    }, "the tab shows the journal mode SQLite actually runs in"
    for key in (
        "catalog_sqlite_bytes",
        "obelisk_db_bytes",
        "duckdb_tmp_bytes",
        "active_table_count",
        "active_data_files",
        "total_data_bytes",
        "orphaned_load_tables",
    ):
        assert body[key] >= 0, f"{key} must be a count of something"


@pytest.mark.asyncio
async def test_maintenance_drops_an_orphaned_load_table_and_reports_the_difference():
    """A killed ingest leaves a load table; the Settings button is what reclaims it."""
    schema = f"zzmaint{uuid.uuid4().hex[:6]}"
    # A directory the last drop left empty: nothing in the catalog points at it, but a walk
    # of the warehouse would call it a folder.
    emptied = settings.lakehouse_root_path / schema / "orders"
    emptied.mkdir(parents=True, exist_ok=True)
    conn = lakehouse_connection()
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
        conn.execute(
            f"CREATE TABLE lakehouse.{schema}.orders{LOAD_TABLE_SUFFIX} AS SELECT 1 AS id"
        )
    finally:
        conn.close()

    try:
        async with _client() as client:
            before = await client.get("/api/catalog/lakehouse/diagnostics")
            assert before.json()["orphaned_load_tables"] >= 1, (
                "the orphan is visible before maintenance runs"
            )

            res = await client.post(
                "/api/catalog/lakehouse/maintain",
                json={"keep_days": 7, "delete_files": False},
            )
            after = await client.get("/api/catalog/lakehouse/diagnostics")

        assert res.status_code == 200, res.text
        body = res.json()
        assert body["status"] == "ok"
        assert body["orphaned_load_tables_dropped"] >= 1
        assert body["snapshots_expired"] >= 0
        assert body["files_removed"] == 0, "nothing is deleted while delete_files is off"
        assert body["empty_directories_removed"] >= 1, (
            "the sweep reports the directories it removed"
        )
        assert not emptied.exists(), "an empty directory nothing points at is swept"
        assert body["before"]["snapshots"] >= 1
        assert body["after"]["snapshots"] >= 1
        assert after.json()["orphaned_load_tables"] == 0

        conn = lakehouse_connection()
        try:
            rows = conn.execute(
                "SELECT 1 FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
                "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s "
                "  ON s.schema_id = t.schema_id "
                "WHERE t.end_snapshot IS NULL AND s.schema_name = ?",
                [schema],
            ).fetchall()
        finally:
            conn.close()
        assert rows == [], "the load table is gone from the catalog"
    finally:
        conn = lakehouse_connection()
        try:
            conn.execute(f"DROP SCHEMA IF EXISTS lakehouse.{schema} CASCADE;")
        finally:
            conn.close()
