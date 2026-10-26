import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_health_check_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "Obelisk" in data["app"]

@pytest.mark.asyncio
async def test_advisor_specs_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/services/advisor/specs")
    assert response.status_code == 200
    data = response.json()
    assert data["total_ram_gb"] > 0
    assert data["cpu_count"] > 0
    assert data["max_safe_allocation_bytes"] > 0

@pytest.mark.asyncio
async def test_advisor_recommend_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/services/advisor/recommend/postgres")
    assert response.status_code == 200
    data = response.json()
    assert data["service_name"] == "postgres"
    assert data["recommended_ram_mb"] > 0

@pytest.mark.asyncio
async def test_services_catalog_status_and_no_remote_mode():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/services/status")
        assert resp.status_code == 200
        services = resp.json()
        assert len(services) == 5
        service_names = {s["service_name"] for s in services}
        assert "postgres" in service_names
        assert "superset" in service_names

        # Connecting to a remote instance is the Connection Vault's job now: a remote
        # database is a source, and a service is a container this host manages.
        resp = await ac.post(
            "/api/services/connect",
            json={"service_name": "postgres", "host": "remote-pg.corp.internal", "port": 5432},
        )
        assert resp.status_code in (404, 405), resp.text
