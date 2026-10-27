"""A preview of an external table must honour the table's schema.

The catalog describes tables from every schema (`crm.customer`, `sales.sales_order_line`),
but the preview queried `public.<name>`: every table outside `public` answered "relation
does not exist", which the API reported as a bare 500 while the same table ran happily in
Query Studio — which is handed the fully-qualified name.
"""
import uuid

import psycopg2
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.vault import vault
from app.database import AsyncSessionLocal
from app.main import app
from app.models.connection import Connection


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.fixture()
def schema_and_table(source_postgres_params):
    """A real schema and table outside `public`, dropped even when the test fails.

    The table name is randomized so ``public.<name>`` can never exist: the third test needs
    the default-schema lookup to be a genuine miss.
    """
    p = source_postgres_params
    schema = f"preview_probe_{uuid.uuid4().hex[:6]}"
    table = f"probe_{uuid.uuid4().hex[:6]}"
    conn = psycopg2.connect(
        host=p["host"],
        port=p["port"],
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
        sslmode=p["sslmode"],
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id INT, note TEXT)')
            cur.execute(
                f'INSERT INTO "{schema}"."{table}" VALUES (%s, %s), (%s, %s)',
                (1, "first", 2, "second"),
            )
        yield schema, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


@pytest.fixture()
async def registered_connection(source_postgres_params):
    """A Connection row pointing at the real source, removed afterwards."""
    p = source_postgres_params
    async with AsyncSessionLocal() as session:
        record = Connection(
            name=f"preview_probe_{uuid.uuid4().hex[:6]}",
            source_type="postgres",
            host=p["host"],
            port=p["port"],
            database=p["database"],
            username=p["user"],
            encrypted_credentials=vault.encrypt(p["password"]),
            options={"sslmode": p["sslmode"]},
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        connection_id = record.id

    yield connection_id

    async with AsyncSessionLocal() as session:
        row = await session.get(Connection, connection_id)
        if row is not None:
            await session.delete(row)
            await session.commit()


@pytest.mark.asyncio
async def test_preview_reads_the_schema_named_in_the_query(
    registered_connection, schema_and_table
):
    schema, table = schema_and_table
    async with _client() as client:
        response = await client.get(
            f"/api/catalog/preview/{registered_connection}/{table}",
            params={"schema": schema},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["row_count"] == 2
    assert {"id", "note"} <= set(body["columns"])


@pytest.mark.asyncio
async def test_preview_accepts_the_dotted_name_the_catalog_shows(
    registered_connection, schema_and_table
):
    """`sales.sales_order_line` in the path says the same thing as `?schema=sales`."""
    schema, table = schema_and_table
    async with _client() as client:
        response = await client.get(
            f"/api/catalog/preview/{registered_connection}/{schema}.{table}"
        )

    assert response.status_code == 200, response.text
    assert response.json()["row_count"] == 2


@pytest.mark.asyncio
async def test_a_refusal_from_the_source_is_reported_as_one(
    registered_connection, schema_and_table
):
    """A miss is the source's verdict, not an Obelisk crash: 502, and its own words."""
    _schema, table = schema_and_table
    async with _client() as client:
        response = await client.get(
            f"/api/catalog/preview/{registered_connection}/{table}",
            params={"schema": "public"},
        )

    assert response.status_code == 502, response.text
    detail = response.json()["detail"]
    assert f"public.{table}" in detail, detail
    assert "could not answer the preview" in detail, detail
