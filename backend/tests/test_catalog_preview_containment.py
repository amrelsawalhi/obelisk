"""Regression test for catalog preview path containment (audit H6).

get_table_preview used to resolve a URL-supplied table name straight into a
filesystem path, so `/api/catalog/preview/lakehouse:../../secret` read any
.parquet on the host and a name containing a quote broke out of the DuckDB
literal.
"""
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "/api/catalog/preview/lakehouse:../../etc/passwd",
        "/api/catalog/preview/lakehouse:../secrets",
        "/api/catalog/preview/lakehouse:sub/../../../escape",
        "/api/catalog/preview/lakehouse:*.parquet",
        "/api/catalog/preview/lakehouse:root/..%2F..%2Fetc",
    ],
)
async def test_preview_rejects_path_traversal(url: str) -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(url)

    assert response.status_code in (400, 404), (
        f"{url} returned {response.status_code}, expected a rejection"
    )


@pytest.mark.asyncio
async def test_preview_still_serves_a_real_table() -> None:
    """The rejection must not break the normal path."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tree = (await client.get("/api/catalog/tree?refresh=true")).json()

    lakehouse = next(node for node in tree if node.get("id") == "lakehouse")
    table = next(
        (t for node in lakehouse.get("directories", []) for t in node.get("tables", [])),
        None,
    )
    if table is None:
        pytest.skip("no lakehouse table available to preview")

    folder = table["fqn"].rsplit(".", 1)[0]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/catalog/preview/lakehouse:{folder}/{table['name']}")

    assert response.status_code == 200
    assert "columns" in response.json()
