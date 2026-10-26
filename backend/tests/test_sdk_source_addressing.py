"""Reading a source table through the SDK: attached or refused, never a mystery.

`obelisk.table("<source>.db.table")` must answer one of two things. For a source DuckDB can
scan (postgres, mysql, mongo) it returns a wrapper that attaches on read; for one it cannot
(Oracle, SQL Server — reachable only through an operator-installed ODBC DSN) it says why,
instead of handing the identifier to DuckDB and printing ``schema "..." does not exist``.
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
async def test_a_scanner_less_source_is_refused_with_the_reason() -> None:
    oracle = await _create("oracle", port=1521)
    try:
        with pytest.raises(SourceNotQueryable) as exc:
            obelisk.table(f"{oracle['name']}.ORCL.SESSIONS")
        message = str(exc.value)
        assert "cannot be queried live" in message
        assert "Ingest" in message
    finally:
        await _delete(oracle["id"])


@pytest.mark.asyncio
async def test_the_refusal_points_at_the_lakehouse_table_when_it_exists() -> None:
    layer = unique_name("zzsdk")
    oracle = await _create("oracle", port=1521)
    write_table(f"{layer}/sessions.parquet", pa.table({"id": [1, 2, 3]}))
    try:
        with pytest.raises(SourceNotQueryable) as exc:
            obelisk.table(f"{oracle['name']}.ORCL.sessions")
        assert f"lakehouse.{layer}.sessions" in str(exc.value)
    finally:
        await _delete(oracle["id"])
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
@pytest.mark.parametrize("source_type", ["postgres", "mysql", "mongodb"])
async def test_a_scannable_source_is_addressed_as_that_source(source_type: str) -> None:
    """A live source wins over a lakehouse table that happens to share the leaf name."""
    created = await _create(source_type)
    try:
        wrapper = obelisk.table(f"{created['name']}.sourcedb.sessions")
        assert wrapper.full_identifier == f"{created['name']}.sourcedb.sessions"
        assert wrapper.file_path is None
    finally:
        await _delete(created["id"])
