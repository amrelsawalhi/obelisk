import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.api.routes_auth import COOKIE_NAME, ADMIN_USERNAME, ADMIN_PASSWORD


@pytest.mark.asyncio
async def test_auth_login_success_and_cookie():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Valid login
        res = await client.post(
            "/api/auth/login",
            json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        )
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert data["user"]["authenticated"] is True
        assert data["user"]["username"] == ADMIN_USERNAME

        # Verify HttpOnly session cookie was set
        assert COOKIE_NAME in res.cookies
        cookie_val = res.cookies[COOKIE_NAME]
        assert cookie_val == data["access_token"]


@pytest.mark.asyncio
async def test_auth_login_invalid_credentials():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.post(
            "/api/auth/login",
            json={"username": "wrong_user", "password": "wrong_password"},
        )
        assert res.status_code == 401
        assert "Invalid credentials" in res.json()["detail"]


@pytest.mark.asyncio
async def test_auth_me_session_verification():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 1. Anonymous / unauthenticated check
        res_anon = await client.get("/api/auth/me")
        assert res_anon.status_code == 200
        assert res_anon.json()["authenticated"] is False

        # 2. Login to get cookie
        login_res = await client.post(
            "/api/auth/login",
            json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        )
        token = login_res.json()["access_token"]

        # 3. Authenticated check with cookie
        res_auth = await client.get("/api/auth/me", cookies={COOKIE_NAME: token})
        assert res_auth.status_code == 200
        data = res_auth.json()
        assert data["authenticated"] is True
        assert data["username"] == ADMIN_USERNAME

        # 4. Authenticated check with Bearer header
        res_bearer = await client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert res_bearer.status_code == 200
        assert res_bearer.json()["authenticated"] is True


@pytest.mark.asyncio
async def test_auth_logout_clears_cookie():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.post("/api/auth/logout")
        assert res.status_code == 200
        assert res.json()["status"] == "logged_out"
