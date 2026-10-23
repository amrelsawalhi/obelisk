"""Phase 1b — source registry: one place decides how a source is reached.

Properties under test:

* inference maps every known ``source_type`` to a driver, and refuses to guess;
* a pinned driver wins, and an attached source is read-only with pushdown;
* reconciling a source records ``shortcut`` rows (attachable) or ``external`` rows
  (must be pulled), and prunes rows the source no longer has — in one DELETE;
* deleting a connection cannot leave orphaned catalog rows behind.
"""
import sqlite3
import uuid
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.core.sources import (
    ATTACH,
    DRIVERS,
    HTTP,
    SDK,
    UnsupportedSource,
    connection_alias,
    infer_driver,
    resolve_source,
)
from app.database import AsyncSessionLocal
from app.main import app
from app.services.catalog import catalog_service


def _connection(source_type: str = "postgres", **overrides) -> SimpleNamespace:
    values = {
        "id": str(uuid.uuid4()),
        "name": f"probe {uuid.uuid4().hex[:6]}",
        "source_type": source_type,
        "driver": None,
        "read_only": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    "source_type,expected",
    [
        ("postgres", ATTACH),
        ("postgresql", ATTACH),
        ("mysql", SDK),
        ("mongodb", SDK),
        ("oracle", SDK),
        ("sqlserver", SDK),
        ("api", HTTP),
        ("rest", HTTP),
    ],
)
def test_inference_maps_every_known_type(source_type: str, expected: str) -> None:
    assert infer_driver(source_type) == expected


def test_legacy_row_without_a_driver_still_resolves() -> None:
    """Rows written before the column existed must not need a data migration."""
    recipe = resolve_source(_connection("postgres", driver=None))
    assert recipe.driver == ATTACH
    assert recipe.read_only is True
    assert recipe.has("pushdown")


def test_unknown_source_type_raises_instead_of_guessing() -> None:
    with pytest.raises(UnsupportedSource):
        infer_driver("cassandra")
    with pytest.raises(UnsupportedSource):
        resolve_source(_connection("cassandra"))


def test_pinned_driver_wins_over_inference() -> None:
    recipe = resolve_source(_connection("mysql", driver="attach"))
    assert recipe.driver == ATTACH
    # ... and an explicit read_only=False is respected even for an attach driver.
    assert resolve_source(_connection("postgres", read_only=False)).read_only is False


def test_sdk_and_http_are_not_attachable() -> None:
    assert resolve_source(_connection("mysql")).is_attachable is False
    assert resolve_source(_connection("api")).is_attachable is False


def test_alias_is_safe_and_stable() -> None:
    assert connection_alias("Warehouse") == "warehouse"
    assert connection_alias("My PG (prod)!") == "my_pg__prod"
    assert connection_alias("") == "external"
    import re

    for name in ("Prod PG", "mongo.main", "Ünïcode Source", "a-b c"):
        assert re.fullmatch(r"[a-z0-9_]+", connection_alias(name)), name


# --------------------------------------------------------------------------- #
# Reconcile: the source's live tables become catalog rows
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_reconcile_records_shortcuts_and_prunes() -> None:
    conn = _connection("postgres", driver="attach", read_only=True)
    alias = connection_alias(conn.name)
    tables = [
        {"name": "orders", "schema_name": "public", "columns": [{"name": "id"}], "size_bytes": 1024},
        {"name": "sales.customers", "schema_name": "sales", "columns": [], "size_bytes": None},
    ]

    try:
        async with AsyncSessionLocal() as session:
            result = await catalog_service.reconcile_connection(session, conn, tables)
            rows = await catalog_service.list_objects(session, connection_id=conn.id)

            assert result["kind"] == "shortcut"
            assert result["driver"] == "attach"
            assert result["discovered"] == 2
            assert result["removed"] == 0

            by_fqn = {row.fqn: row for row in rows}
            orders = by_fqn[f"{alias}.public.orders"]
            assert orders.kind == "shortcut"
            assert orders.backend == "postgres"
            assert orders.size_bytes == 1024
            assert orders.location == "public.orders"
            assert "pushdown" in orders.capabilities_json

            # The introspector prefixes non-public names; the catalog stores the stem.
            customers = by_fqn[f"{alias}.sales.customers"]
            assert customers.name == "customers"
            assert customers.location == "sales.customers"

        # A second pass that no longer sees one table must prune exactly that one.
        async with AsyncSessionLocal() as session:
            second = await catalog_service.reconcile_connection(session, conn, tables[:1])
            remaining = await catalog_service.list_objects(session, connection_id=conn.id)

            assert second["removed"] == 1
            assert [row.fqn for row in remaining] == [f"{alias}.public.orders"]
            # Re-seeing an existing table bumps its version instead of duplicating.
            assert remaining[0].version == 2
    finally:
        async with AsyncSessionLocal() as session:
            await catalog_service.delete_for_connection(session, conn.id)
            await session.commit()


@pytest.mark.asyncio
async def test_reconcile_marks_pulled_sources_external() -> None:
    conn = _connection("mysql")
    alias = connection_alias(conn.name)
    try:
        async with AsyncSessionLocal() as session:
            result = await catalog_service.reconcile_connection(
                session, conn, [{"name": "customers", "schema_name": "shop"}]
            )
            assert result["kind"] == "external"
            row = (await catalog_service.list_objects(session, connection_id=conn.id))[0]
            assert row.fqn == f"{alias}.shop.customers"
            assert row.kind == "external"
            assert "pushdown" not in (row.capabilities_json or [])
    finally:
        async with AsyncSessionLocal() as session:
            await catalog_service.delete_for_connection(session, conn.id)
            await session.commit()


# --------------------------------------------------------------------------- #
# API surface
# --------------------------------------------------------------------------- #


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _payload(**overrides) -> dict:
    values = {
        "name": f"zzsrc{uuid.uuid4().hex[:8]}",
        "source_type": "postgres",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "postgres",
        "username": "reader",
        "password": "secret",
    }
    values.update(overrides)
    return values


@pytest.mark.asyncio
async def test_create_connection_records_the_recipe() -> None:
    async with _client() as client:
        pg = await client.post("/api/catalog/connections", json=_payload())
        assert pg.status_code == 200, pg.text
        body = pg.json()
        assert body["driver"] == "attach"
        assert body["read_only"] is True
        assert "pushdown" in body["capabilities"]

        try:
            listed = (await client.get("/api/catalog/connections")).json()
            entry = next(c for c in listed if c["id"] == body["id"])
            assert entry["driver"] == "attach"
            assert entry["capabilities"] == body["capabilities"]
        finally:
            assert (
                await client.delete(f"/api/catalog/connections/{body['id']}")
            ).status_code == 200

        mysql = await client.post(
            "/api/catalog/connections",
            json=_payload(source_type="mysql", port=3306, database="shop"),
        )
        assert mysql.status_code == 200, mysql.text
        assert mysql.json()["driver"] == "sdk"
        assert mysql.json()["read_only"] is False
        await client.delete(f"/api/catalog/connections/{mysql.json()['id']}")


@pytest.mark.asyncio
async def test_create_connection_rejects_an_unknown_driver_or_type() -> None:
    async with _client() as client:
        bad_driver = await client.post(
            "/api/catalog/connections", json=_payload(driver="telepathy")
        )
        assert bad_driver.status_code == 400
        assert "attach" in bad_driver.json()["detail"]

        bad_type = await client.post(
            "/api/catalog/connections", json=_payload(source_type="cassandra")
        )
        assert bad_type.status_code == 400
        assert "Unknown source type" in bad_type.json()["detail"]


@pytest.mark.asyncio
async def test_deleting_a_connection_removes_its_catalog_rows() -> None:
    async with _client() as client:
        created = (await client.post("/api/catalog/connections", json=_payload())).json()
        conn_id = created["id"]

        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute(
                "INSERT INTO catalog_objects (id, fqn, name, kind, backend, connection_id,"
                " version, created_at) VALUES (?, 'probe.public.orders', 'orders', 'shortcut',"
                " 'postgres', ?, 1, CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), conn_id),
            )
            con.commit()

        assert (await client.delete(f"/api/catalog/connections/{conn_id}")).status_code == 200

        objects = (
            await client.get("/api/catalog/objects", params={"connection_id": conn_id})
        ).json()
        assert objects == []


@pytest.mark.asyncio
async def test_listed_connections_all_declare_a_driver() -> None:
    """Backfilled rows must resolve, so the UI never shows a driverless source."""
    async with _client() as client:
        listed = (await client.get("/api/catalog/connections")).json()

    for entry in listed:
        assert entry["driver"] in DRIVERS + ("unknown",), entry
        if entry["source_type"].lower() == "postgres":
            assert entry["driver"] == ATTACH


