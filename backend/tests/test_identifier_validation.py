"""Names are checked before they become SQL.

An ingest used to accept any table name, and ``quote_ident`` faithfully created a table
called ``x"; DROP TABLE …; --``. Deleting that table then pasted the name into
``DROP TABLE lakehouse."main"."…"`` without escaping, so DuckDB ran the stacked statement
and the other table was gone. ``watermark_column`` had the same shape: it was read back
with ``SELECT max("…")``, where a crafted value decides the answer.

These tests pin the rule at both doors — the request and the engine — and replay the
two-step attack to prove that the second step now refuses instead of executing.
"""
from __future__ import annotations

from urllib.parse import quote

import pyarrow as pa
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.catalog import (
    InvalidTablePath,
    quote_ident,
    validate_column,
    validate_folder,
    validate_segment,
)
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine
from app.main import app
from lakehouse_helpers import drop_schema, unique_name, write_table


@pytest.mark.parametrize("name", ["orders", "raw_2024", "a-b", "Order", "x" * 63])
def test_ordinary_names_pass(name: str) -> None:
    assert validate_segment(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "../secret",
        'a"b',
        "a;b",
        "x y",
        "café",
        "-x",
        "",
        "   ",
        "x.y",
        "a`b",
        "..",
        "x" * 64,
    ],
)
def test_anything_that_is_not_a_name_is_refused(name: str) -> None:
    with pytest.raises(InvalidTablePath):
        validate_segment(name)


def test_the_column_rule_is_stricter_than_the_table_rule() -> None:
    """A column is never a path, so the characters a table may use are not allowed."""
    assert validate_column("order_id") == "order_id"
    for bad in ("a-b", "x.y", "1x", 'a"b'):
        with pytest.raises(InvalidTablePath):
            validate_column(bad)


def test_folder_paths_normalise_and_refuse_traversal() -> None:
    assert validate_folder("raw\\orders") == "raw/orders"
    assert validate_folder("/raw/sales/") == "raw/sales"
    for bad in ("../etc", "raw/../orders", "raw//../x", "", "raw/my folder"):
        with pytest.raises(InvalidTablePath):
            validate_folder(bad)


def test_ingest_address_refuses_a_hostile_name() -> None:
    hostile = 'x"; DROP TABLE lakehouse.main.victim; --'
    with pytest.raises(InvalidTablePath):
        IngestEngine._address("raw", "orders", hostile)
    with pytest.raises(InvalidTablePath):
        IngestEngine._address("../etc", "orders", None)
    with pytest.raises(InvalidTablePath):
        IngestEngine._address(None, "sales 2024", None)


def test_ingest_address_still_maps_the_normal_cases() -> None:
    """A folder is a schema, ``schema.table`` keeps only the table half."""
    assert IngestEngine._address(None, "orders", None) == ("main", "orders")
    assert IngestEngine._address("raw", "orders", None) == ("raw", "orders")
    assert IngestEngine._address("raw/sales", "pg.orders", None) == ("raw.sales", "orders")
    assert IngestEngine._address("raw", "anything", "renamed") == ("raw", "renamed")
    # A dotted collection name keeps its last segment as the table, as it always has;
    # an operator who wants the whole name says so with target_name.
    assert IngestEngine._address(None, "events.2024", "events_2024") == ("main", "events_2024")


@pytest.mark.asyncio
async def test_ingest_endpoint_refuses_a_hostile_target() -> None:
    """The 422 lands before the connection is even looked up."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/ingest/trigger/async",
            json={
                "connection_id": "not-a-connection",
                "entity": "orders",
                "target_name": 'x"; DROP TABLE lakehouse.main.victim; --',
            },
        )
        column_response = await client.post(
            "/api/ingest/trigger/async",
            json={
                "connection_id": "not-a-connection",
                "entity": "orders",
                "watermark_column": 'x") AS v FROM (SELECT 42 AS x) --',
            },
        )

    assert response.status_code == 422, response.text
    assert "table name" in response.text.lower()
    assert column_response.status_code == 422, column_response.text


@pytest.mark.asyncio
async def test_catalog_routes_refuse_hostile_names() -> None:
    transport = ASGITransport(app=app)
    hostile = 'x"; DROP TABLE lakehouse.main.victim; --'
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        rename = await client.put(
            "/api/catalog/lakehouse/tables/rename",
            json={"folder": "main", "old_name": "orders", "new_name": hostile},
        )
        move = await client.post(
            "/api/catalog/lakehouse/tables/move",
            json={"source_folder": "main", "target_folder": "raw", "table_name": hostile},
        )

    assert rename.status_code == 400, rename.text
    assert move.status_code == 400, move.text


@pytest.mark.asyncio
async def test_deleting_a_booby_trapped_name_does_not_run_the_second_statement() -> None:
    """The attack, step by step: create the name, then ask the route to delete it.

    The table is created the way ingest used to create it, so this covers data that
    already exists rather than only the new validation on the way in.
    """
    layer = unique_name("zzid")
    victim = f"lakehouse.{layer}.victim"
    hostile = f'x"; DROP TABLE {victim}; --'

    write_table(f"{layer}/victim.parquet", pa.table({"keep": [1]}))

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(
            f"CREATE TABLE lakehouse.{quote_ident(layer)}.{quote_ident(hostile)} "
            "AS SELECT 1 AS a"
        )
    finally:
        conn.close()

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.delete(
                f"/api/catalog/lakehouse/tables?folder={layer}&name={quote(hostile, safe='')}"
            )

        assert response.status_code == 400, response.text

        conn = duckdb_engine.get_connection()
        try:
            kept = conn.execute(f"SELECT count(*) FROM {victim}").fetchone()[0]
        finally:
            conn.close()
        assert kept == 1, "the stacked DROP ran"
    finally:
        drop_schema(layer)


@pytest.mark.asyncio
async def test_deleting_an_ordinary_table_still_works() -> None:
    """The rejection must not break the normal path."""
    layer = unique_name("zzid")
    write_table(f"{layer}/orders.parquet", pa.table({"id": [1]}))

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.delete(
                f"/api/catalog/lakehouse/tables?folder={layer}&name=orders"
            )

        assert response.status_code == 200, response.text

        conn = duckdb_engine.get_connection()
        try:
            remaining = conn.execute(
                "SELECT count(*) FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
                "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s "
                "  ON s.schema_id = t.schema_id "
                "WHERE t.end_snapshot IS NULL AND s.schema_name = ? AND t.table_name = ?",
                [layer, "orders"],
            ).fetchone()[0]
        finally:
            conn.close()
        assert remaining == 0
    finally:
        drop_schema(layer)


def test_a_hostile_watermark_cannot_choose_the_answer() -> None:
    """The payload that used to return 42 no longer resolves to a column at all."""
    layer = unique_name("zzid")
    address = write_table(f"{layer}/w.parquet", pa.table({"x": [1]}))

    conn = duckdb_engine.get_connection()
    try:
        value = IngestEngine._max_watermark(
            conn, address, 'x") AS v FROM (SELECT 42 AS x) --'
        )
    finally:
        conn.close()

    drop_schema(layer)
    assert value is None, "a crafted column name must not become a query"
