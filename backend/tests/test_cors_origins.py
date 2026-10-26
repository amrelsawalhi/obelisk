"""CORS is an allowlist, not a wildcard.

While auth was off the middleware served ``Access-Control-Allow-Origin: *``, so any page
the operator visited could POST to the local API — and a wildcard cannot carry credentials
anyway. These tests pin the allowlist in the default (auth-off) configuration.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app

ALLOWED = "http://localhost:5173"
FOREIGN = "http://evil.example"


@pytest.mark.asyncio
async def test_a_foreign_origin_gets_no_cors_grant() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health", headers={"Origin": FOREIGN})

    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.asyncio
async def test_a_foreign_preflight_is_refused() -> None:
    """A cross-origin write never even gets permission to try."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.options(
            "/api/pipelines",
            headers={
                "Origin": FOREIGN,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )

    assert "access-control-allow-origin" not in response.headers, response.headers


@pytest.mark.asyncio
async def test_the_dev_server_is_allowed() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health", headers={"Origin": ALLOWED})

    assert response.headers.get("access-control-allow-origin") == ALLOWED
