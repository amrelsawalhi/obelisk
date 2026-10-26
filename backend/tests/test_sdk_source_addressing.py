"""Reading a source table through the SDK: attached or refused, never a mystery.

`obelisk.table("mongo_telemetry_source.sourcedb.sessions")` used to hand the identifier
straight to DuckDB, which answered ``schema "mongo_telemetry_source.sourcedb" does not
exist``. The truth is that only PostgreSQL has a DuckDB scanner; Mongo/MySQL/Oracle/HTTP
sources have to be ingested into the lakehouse before SQL can see them, so the SDK says
that — and points at the lakehouse table when one already exists.
"""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.sdk import obelisk
from app.sdk.obelisk import SourceNotQueryable
from lakehouse_helpers import drop_schema, unique_name, write_table


def _payload(source_type: str, **overrides) -> dict:
    values = {
        "name": f"zzsdk{uuid.uuid4().hex[:8]}",
        "source_type": source_type,
        "host": "127.0.0.1",
        "port": 27017,
        "database": "sourcedb",
        "username": "reader",
        "password": "secret",
    }
    values.update(overrides)
    return values


async def _create(source_type: str, **overrides) -> dict:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/catalog/connections", json=_payload(source_type, **overrides)
        )
    assert response.status_code == 200, response.text
    return response.json()


async def _delete(conn_id: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.delete(f"/api/catalog/connections/{conn_id}")


@pytest.mark.asyncio
async def test_a_pulled_source_is_refused_with_the_reason() -> None:
    mongo = await _create("mongodb")
    try:
        with pytest.raises(SourceNotQueryable) as exc:
            obelisk.table(f"{mongo['name']}.sourcedb.sessions")
        message = str(exc.value)
        assert "cannot be queried live" in message
        assert "Ingest" in message
    finally:
        await _delete(mongo["id"])


@pytest.mark.asyncio
async def test_the_refusal_points_at_the_lakehouse_table_when_it_exists() -> None:
    layer = unique_name("zzsdk")
    mongo = await _create("mongodb")
    write_table(f"{layer}/sessions.parquet", pa.table({"id": [1, 2, 3]}))
    try:
        with pytest.raises(SourceNotQueryable) as exc:
            obelisk.table(f"{mongo['name']}.sourcedb.sessions")
        assert f"lakehouse.{layer}.sessions" in str(exc.value)
    finally:
        await _delete(mongo["id"])
        drop_schema(layer)


def test_a_lakehouse_table_still_reads_normally() -> None:
    layer = unique_name("zzsdk")
    write_table(f"{layer}/orders.parquet", pa.table({"id": [1, 2]}))
    try:
        frame = obelisk.table(f"lakehouse.{layer}.orders").to_polars()
        assert frame.height == 2
    finally:
        drop_schema(layer)


@pytest.mark.asyncio
async def test_a_postgres_source_is_still_treated_as_attachable() -> None:
    """The refusal is about capability, not about naming a connection."""
    postgres = await _create("postgres", port=5432)
    try:
        wrapper = obelisk.table(f"{postgres['name']}.public.orders")
        assert postgres["name"] in wrapper.full_identifier
    finally:
        await _delete(postgres["id"])
