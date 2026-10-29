"""The login flow end to end: status, verify, cookies, gates and lockout.

These exercise the real app over ASGITransport. ``AUTH_ENABLED`` is toggled on
``settings`` directly — that is how the deployment turns enforcement on, and the
guards read it per request.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import routes_auth
from app.api.routes_auth import COOKIE_NAME
from app.config import settings
from app.core.security import create_token, verify_token
from app.main import app


def _reset_login_state() -> None:
    routes_auth._LOGIN_ATTEMPTS.clear()
    routes_auth._LOGIN_LOCKED_UNTIL.clear()


def _good_credentials() -> dict:
    return {
        "username": settings.OBELISK_ADMIN_USER,
        "password": settings.OBELISK_ADMIN_PASSWORD,
    }


def _wrong_credentials() -> dict:
    return {"username": "nope", "password": "wrong-password"}


@pytest.mark.asyncio
async def test_status_reports_whether_authentication_is_enforced() -> None:
    previous = settings.AUTH_ENABLED
    try:
        settings.AUTH_ENABLED = False
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/api/auth/status")).json()["auth_enabled"] is False

        settings.AUTH_ENABLED = True
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            payload = (await client.get("/api/auth/status")).json()
            assert payload["auth_enabled"] is True
            assert payload["session_ttl_seconds"] > 0
    finally:
        settings.AUTH_ENABLED = previous


@pytest.mark.asyncio
async def test_verify_answers_204_only_for_a_valid_session() -> None:
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = True
    _reset_login_state()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/api/auth/verify")).status_code == 401

            login = await client.post("/api/auth/login", json=_good_credentials())
            token = login.json()["access_token"]

            ok = await client.get("/api/auth/verify", cookies={COOKIE_NAME: token})
            assert ok.status_code == 204
            assert ok.headers.get("X-Obelisk-User") == settings.OBELISK_ADMIN_USER

            tampered = token[:-2] + ("AA" if not token.endswith("AA") else "BB")
            assert (
                await client.get("/api/auth/verify", cookies={COOKIE_NAME: tampered})
            ).status_code == 401

            expired = create_token({"sub": "admin", "role": "admin"}, expires_seconds=-1)
            assert (
                await client.get("/api/auth/verify", cookies={COOKIE_NAME: expired})
            ).status_code == 401
    finally:
        settings.AUTH_ENABLED = previous
        _reset_login_state()


@pytest.mark.asyncio
async def test_verify_is_a_no_op_while_authentication_is_off() -> None:
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = False
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get("/api/auth/verify")
            assert res.status_code == 204
            assert res.headers.get("X-Obelisk-Role") == "admin"
    finally:
        settings.AUTH_ENABLED = previous


@pytest.mark.asyncio
async def test_a_protected_route_requires_the_session() -> None:
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = True
    _reset_login_state()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/api/services/status")).status_code == 401

            await client.post("/api/auth/login", json=_good_credentials())
            assert (await client.get("/api/services/status")).status_code == 200
    finally:
        settings.AUTH_ENABLED = previous
        _reset_login_state()



@pytest.mark.asyncio
async def test_the_session_cookie_carries_the_expected_flags() -> None:
    previous = (settings.SESSION_SECURE_COOKIE, settings.AUTH_ENABLED)
    _reset_login_state()
    try:
        settings.AUTH_ENABLED = True
        settings.SESSION_SECURE_COOKIE = False
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post("/api/auth/login", json=_good_credentials())
            set_cookie = res.headers["set-cookie"].lower()
            assert "httponly" in set_cookie
            assert "samesite=lax" in set_cookie
            assert "path=/" in set_cookie
            assert "secure" not in set_cookie
            assert res.headers.get("cache-control") == "no-store"
            assert verify_token(res.json()["access_token"])["sub"] == settings.OBELISK_ADMIN_USER

        settings.SESSION_SECURE_COOKIE = True
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post("/api/auth/login", json=_good_credentials())
            assert "secure" in res.headers["set-cookie"].lower()
    finally:
        settings.SESSION_SECURE_COOKIE, settings.AUTH_ENABLED = previous
        _reset_login_state()


@pytest.mark.asyncio
async def test_too_many_failures_lock_the_address_out() -> None:
    previous = (
        settings.AUTH_ENABLED,
        settings.AUTH_MAX_FAILED_ATTEMPTS,
        settings.AUTH_LOCKOUT_SECONDS,
    )
    _reset_login_state()
    try:
        settings.AUTH_ENABLED = True
        settings.AUTH_MAX_FAILED_ATTEMPTS = 3
        settings.AUTH_LOCKOUT_SECONDS = 300
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for _ in range(3):
                res = await client.post("/api/auth/login", json=_wrong_credentials())
                assert res.status_code == 401

            locked = await client.post("/api/auth/login", json=_good_credentials())
            assert locked.status_code == 429, "even the right password must wait out the lock"
            assert "Retry-After" in locked.headers

        _reset_login_state()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            ok = await client.post("/api/auth/login", json=_good_credentials())
            assert ok.status_code == 200, "a cleared lockout must admit the right password"
    finally:
        settings.AUTH_ENABLED, settings.AUTH_MAX_FAILED_ATTEMPTS, settings.AUTH_LOCKOUT_SECONDS = previous
        _reset_login_state()


@pytest.mark.asyncio
async def test_initial_admin_setup_flow() -> None:
    from app.database import AsyncSessionLocal
    from app.models.user import User
    from sqlalchemy import delete

    previous = settings.AUTH_ENABLED
    _reset_login_state()
    try:
        settings.AUTH_ENABLED = True
        # Clear any users in test db to verify clean first-boot setup
        async with AsyncSessionLocal() as session:
            await session.execute(delete(User))
            await session.commit()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # 1. Status indicates setup is needed
            status = await client.get("/api/auth/status")
            assert status.status_code == 200
            assert status.json()["needs_setup"] is True

            # 2. Too short password rejected
            short_res = await client.post(
                "/api/auth/setup",
                json={"username": "superadmin", "password": "123"},
            )
            assert short_res.status_code == 422

            # 3. Successful initial setup
            setup_res = await client.post(
                "/api/auth/setup",
                json={"username": "superadmin", "password": "correct-horse-battery-staple"},
            )
            assert setup_res.status_code == 200
            data = setup_res.json()
            assert data["user"]["username"] == "superadmin"
            assert data["user"]["role"] == "admin"
            assert COOKIE_NAME in setup_res.headers.get("set-cookie", "")

            # 4. Status now indicates setup is NOT needed
            status_after = await client.get("/api/auth/status")
            assert status_after.json()["needs_setup"] is False

            # 5. Calling setup again is forbidden (400 Bad Request)
            repeat_res = await client.post(
                "/api/auth/setup",
                json={"username": "anotheradmin", "password": "another-password-1234"},
            )
            assert repeat_res.status_code == 400
            assert "already been completed" in repeat_res.json()["detail"]

            # 6. Can log in with the new credentials
            login_res = await client.post(
                "/api/auth/login",
                json={"username": "superadmin", "password": "correct-horse-battery-staple"},
            )
            assert login_res.status_code == 200
            assert login_res.json()["user"]["username"] == "superadmin"
    finally:
        settings.AUTH_ENABLED = previous
        _reset_login_state()
