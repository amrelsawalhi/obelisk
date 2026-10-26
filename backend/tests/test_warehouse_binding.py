"""The warehouse is chosen, never guessed.

What this pins:

* a Postgres connection is read-only by default, and a read-only one cannot be bound as
  the warehouse (409 with the reason) — Obelisk publishes marts there;
* with nothing bound, publishing refuses and says what to do, instead of picking
  "the newest Postgres row by created_at" the way it used to;
* binding a writable connection makes it the target, and ``dbt`` receives *its*
  credentials;
* reads stay permissive: Query Studio may federate across a read-only source.
"""
from __future__ import annotations

import os
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.warehouse_resolver import (
    WarehouseNotSelected,
    WarehouseReadOnly,
    describe_warehouse_postgres,
    get_warehouse_connection_id,
    resolve_warehouse_postgres_params,
    resolve_write_warehouse_postgres_params,
    set_warehouse_connection_id,
    warehouse_candidates,
)
from app.engines.dbt_runner import dbt_runner
from app.main import app

# Nothing listens on this port: a resolver that returns it is one that would fail while
# trying to write.
CANDIDATE_PORT = 5999


def _payload(**overrides) -> dict:
    values = {
        "name": f"zzwh{uuid.uuid4().hex[:8]}",
        "source_type": "postgres",
        "host": "127.0.0.1",
        "port": CANDIDATE_PORT,
        "database": "sourcedb",
        "username": "reader",
        "password": "secret",
    }
    values.update(overrides)
    return values


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _create(**overrides) -> dict:
    async with await _client() as client:
        response = await client.post("/api/catalog/connections", json=_payload(**overrides))
    assert response.status_code == 200, response.text
    return response.json()


async def _delete(conn_id: str) -> None:
    async with await _client() as client:
        await client.delete(f"/api/catalog/connections/{conn_id}")


@pytest.fixture(autouse=True)
def no_ambient_warehouse():
    """Each test starts with no binding and no environment override."""
    previous_binding = get_warehouse_connection_id()
    env_keys = ("DBT_PG_HOST", "POSTGRES_HOST")
    saved = {key: os.environ.pop(key, None) for key in env_keys}
    set_warehouse_connection_id(None)
    try:
        yield
    finally:
        set_warehouse_connection_id(previous_binding)
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value


@pytest.mark.asyncio
async def test_a_postgres_source_is_read_only_by_default() -> None:
    created = await _create()
    try:
        assert created["read_only"] is True
        assert any(c["id"] == created["id"] for c in warehouse_candidates())
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_a_read_only_connection_cannot_be_the_warehouse() -> None:
    created = await _create(read_only=True)
    try:
        async with await _client() as client:
            response = await client.put(
                "/api/dbt/warehouse", json={"connection_id": created["id"]}
            )
        assert response.status_code == 409, response.text
        assert "read-only" in response.json()["detail"].lower()
        assert get_warehouse_connection_id() is None
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_binding_a_writable_connection_makes_it_the_target() -> None:
    created = await _create(read_only=False)
    try:
        async with await _client() as client:
            response = await client.put(
                "/api/dbt/warehouse", json={"connection_id": created["id"]}
            )
        assert response.status_code == 200, response.text

        assert get_warehouse_connection_id() == created["id"]
        params = resolve_write_warehouse_postgres_params()
        assert params["port"] == CANDIDATE_PORT
        assert "read_only" not in params and "name" not in params

        env = dbt_runner._get_target_env(dbt_runner.postgres_target())
        assert env["DBT_TARGET"] == dbt_runner.postgres_target()
        assert env["DBT_PG_PORT"] == str(CANDIDATE_PORT)
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_without_a_binding_publishing_refuses() -> None:
    """The old behaviour was to publish into the newest Postgres row. Now it says no."""
    created = await _create(read_only=False)
    try:
        with pytest.raises(WarehouseNotSelected) as exc:
            resolve_write_warehouse_postgres_params()
        assert "Settings" in str(exc.value)
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_reads_still_use_read_only_sources() -> None:
    created = await _create(read_only=True)
    try:
        params = resolve_warehouse_postgres_params()
        assert params["port"] == CANDIDATE_PORT
        described = describe_warehouse_postgres()
        assert described["source"] == "unset"
        assert described["read_only"] is True
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_the_target_list_offers_one_row_per_connection() -> None:
    created = await _create(read_only=False)
    try:
        info = dbt_runner.list_targets()
        postgres_rows = [t for t in info["targets"] if t["adapter"] == "postgres"]
        ids = {row["connection_id"] for row in postgres_rows}
        assert created["id"] in ids
        assert all(row["name"] == dbt_runner.postgres_target() for row in postgres_rows)
        assert "warehouse" in info
        # A duckdb output appears exactly once, with no binding.
        duck = [t for t in info["targets"] if t["adapter"] == "duckdb"]
        assert len(duck) == 1 and duck[0]["connection_id"] is None
    finally:
        await _delete(created["id"])


@pytest.mark.asyncio
async def test_a_read_only_binding_refuses_to_write() -> None:
    """Belt and braces: even if a read-only id is stored directly, writes refuse."""
    created = await _create(read_only=True)
    try:
        set_warehouse_connection_id(created["id"])
        with pytest.raises(WarehouseReadOnly):
            resolve_write_warehouse_postgres_params()
    finally:
        set_warehouse_connection_id(None)
        await _delete(created["id"])
