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
async def test_services_catalog_status_and_connect_remote():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Check listing catalog services
        resp = await ac.get("/api/services/status")
        assert resp.status_code == 200
        services = resp.json()
        assert len(services) == 5
        service_names = {s["service_name"] for s in services}
        assert "postgres" in service_names
        assert "superset" in service_names

        # Connect remote postgres
        connect_payload = {
            "service_name": "postgres",
            "host": "remote-pg.corp.internal",
            "port": 5432,
            "config_json": {"ssl": "require"}
        }
        resp = await ac.post("/api/services/connect", json=connect_payload)
        assert resp.status_code == 200
        connected = resp.json()
        assert connected["service_name"] == "postgres"
        assert connected["deployment_type"] == "remote"
        assert connected["status"] == "connected"
        assert connected["host"] == "remote-pg.corp.internal"

        # Verify status listing reflects remote connection
        resp = await ac.get("/api/services/status")
        services = resp.json()
        pg = next(s for s in services if s["service_name"] == "postgres")
        assert pg["deployment_type"] == "remote"
        assert pg["status"] == "connected"
        assert pg["host"] == "remote-pg.corp.internal"
