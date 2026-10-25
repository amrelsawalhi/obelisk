"""The catalog is a projection of the warehouse, not a ledger beside it.

The rule these tests pin: nothing registers a table. A row exists because a file
exists, its numbers come from that file, and a row whose file is gone disappears on
the next refresh. A file written by any writer — the SDK, a dbt model, a person with
copy-paste — appears the same way.
"""
from __future__ import annotations

import io
import sqlite3
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.core.storage import LocalWarehouse
from app.database import AsyncSessionLocal
from app.main import app
from app.services.catalog import catalog_service


def write_table(path: Path, rows: int = 3) -> None:
    """Creates the table that ``path`` used to represent, through the catalog.

    The helper keeps its signature so the tests below still read as "a table appears at
    this address"; only the mechanism changed.
    """
    from lakehouse_helpers import write_table as create_table

    rel = path.relative_to(settings.lakehouse_root_path).as_posix()
    create_table(rel, pa.table({"id": list(range(rows))}))


def warehouse_rows() -> list[tuple]:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        return con.execute(
            "SELECT fqn, name, kind, row_count FROM catalog_objects"
            " WHERE connection_id IS NULL ORDER BY fqn"
        ).fetchall()


async def refresh() -> None:
    async with AsyncSessionLocal() as session:
        await catalog_service.refresh(session)


@pytest.fixture
def empty_lakehouse():
    """Points the warehouse at a scratch directory for the length of one test."""
    original = settings.STORAGE_URI
    with tempfile.TemporaryDirectory(prefix="obelisk-projection-") as tmp:
        settings.STORAGE_URI = Path(tmp).as_uri()
        try:
            yield Path(tmp)
        finally:
            settings.STORAGE_URI = original


@pytest.mark.asyncio
async def test_a_file_appears_without_any_registration(empty_lakehouse):
    """Write a Parquet file by hand; the catalog describes it on the next read."""
    write_table(empty_lakehouse / "supabase" / "auth" / "table1.parquet", rows=4)

    await refresh()

    assert warehouse_rows() == [
        ("lakehouse.supabase.auth.table1", "table1", "parquet", 4)
    ]


@pytest.mark.asyncio
async def test_dropping_the_table_deletes_the_row(empty_lakehouse):
    target = empty_lakehouse / "raw" / "orders.parquet"
    write_table(target, rows=2)
    await refresh()
    assert [row[0] for row in warehouse_rows()] == ["lakehouse.raw.orders"]

    from app.engines.duckdb_engine import duckdb_engine

    conn = duckdb_engine.get_connection()
    try:
        conn.execute('DROP TABLE lakehouse."raw".orders')
    finally:
        conn.close()

    await refresh()

    assert warehouse_rows() == []


@pytest.mark.asyncio
async def test_a_row_without_a_file_is_removed(empty_lakehouse):
    """The drift the projection exists to prevent: a row that describes nothing."""
    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute(
            "INSERT INTO catalog_objects (id, fqn, name, kind, backend,"
            " location, version, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
            (
                str(uuid.uuid4()),
                "lakehouse.ghost.table1",
                "table1",
                "parquet",
                "local",
                "ghost/table1.parquet",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        con.commit()

    await refresh()

    assert warehouse_rows() == []


@pytest.mark.asyncio
async def test_annotations_survive_a_refresh(empty_lakehouse):
    write_table(empty_lakehouse / "raw" / "orders.parquet", rows=2)
    await refresh()

    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute(
            "UPDATE catalog_objects SET docs = ?, produced_by = ? WHERE fqn = ?",
            ("nightly load", "pipeline:demo", "lakehouse.raw.orders"),
        )
        con.commit()

    await refresh()

    with sqlite3.connect(settings.sqlite_db_path) as con:
        docs, produced_by = con.execute(
            "SELECT docs, produced_by FROM catalog_objects WHERE fqn = ?",
            ("lakehouse.raw.orders",),
        ).fetchone()
    assert (docs, produced_by) == ("nightly load", "pipeline:demo")


@pytest.mark.asyncio
async def test_the_tree_and_the_catalog_agree(empty_lakehouse):
    write_table(empty_lakehouse / "raw" / "orders.parquet", rows=3)
    write_table(empty_lakehouse / "loose.parquet", rows=1)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tree = (await client.get("/api/catalog/tree", params={"refresh": "true"})).json()
        objects = (await client.get("/api/catalog/objects")).json()

    lakehouse = next(node for node in tree if node["id"] == "lakehouse")
    tree_fqns = sorted(table["fqn"] for table in lakehouse["tables"])
    object_fqns = sorted(obj["fqn"] for obj in objects)
    assert tree_fqns == object_fqns == ["lakehouse.loose", "lakehouse.raw.orders"]

    entries = {table["fqn"]: table for table in lakehouse["tables"]}
    assert entries["lakehouse.raw.orders"]["row_count"] == 3
    assert entries["lakehouse.raw.orders"]["schema_name"] == "raw"
    assert entries["lakehouse.loose"]["sql_name"] == "lakehouse.loose"


@pytest.mark.asyncio
async def test_external_rows_are_never_dropped_by_a_lakehouse_scan(empty_lakehouse):
    """A source that is unreachable must not lose its catalog on a refresh."""
    connection_id = f"conn-{uuid.uuid4().hex[:8]}"
    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute(
            "INSERT INTO catalog_objects (id, fqn, name, kind, backend,"
            " location, connection_id, version, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
            (
                str(uuid.uuid4()),
                f"shop{uuid.uuid4().hex[:4]}.public.customers",
                "customers",
                "shortcut",
                "postgres",
                "public.customers",
                connection_id,
                "2026-01-01T00:00:00+00:00",
            ),
        )
        con.commit()

    await refresh()

    with sqlite3.connect(settings.sqlite_db_path) as con:
        kept = con.execute(
            "SELECT COUNT(*) FROM catalog_objects WHERE connection_id = ?", (connection_id,)
        ).fetchone()[0]
        con.execute("DELETE FROM catalog_objects WHERE connection_id = ?", (connection_id,))
        con.commit()
    assert kept == 1


def test_a_write_through_the_seam_is_visible_at_once(empty_lakehouse):
    store = LocalWarehouse(empty_lakehouse)
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2, 3]}), buffer)
    store.write_atomic("marts/sales/daily.parquet", buffer.getvalue())

    assert store.exists("marts/sales/daily.parquet")
    assert store.list_directories() == ["marts", "marts/sales"]


def test_deleting_a_table_takes_the_folders_it_emptied(empty_lakehouse):
    store = LocalWarehouse(empty_lakehouse)
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": [1]}), buffer)
    store.write_atomic("marts/sales/daily.parquet", buffer.getvalue())
    store.write_atomic("marts/keep.parquet", buffer.getvalue())

    store.delete("marts/sales/daily.parquet")
    assert store.list_directories() == ["marts"]

    store.delete("marts/keep.parquet")
    assert store.list_directories() == []
    assert list(empty_lakehouse.rglob("*")) == []
