"""The auth seam: checked now, enforced when switched on.

Auth is not implemented yet, so what these tests pin is the *plumbing*: while it is
off nothing changes, and the moment ``AUTH_ENABLED`` is turned on every /api route and
every socket demands a session -- without a single handler being touched.

The websocket tests use ``/ws/events`` where possible because it has no side effects;
the terminal is only connected to in the refusal cases, so no shell is ever spawned.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.config import settings
from app.core.security import create_token, origin_allowed
from app.main import app


# --------------------------------------------------------------- origin rules


def test_origin_allowed_matrix():
    # No Origin at all: a non-browser client (curl, the SDK). Auth is the gate there,
    # not this check, and browsers always send one.
    assert origin_allowed(None, "127.0.0.1:8000") is True

    # The page and the API on the same host (the built SPA talking to itself).
    assert origin_allowed("http://127.0.0.1:8000", "127.0.0.1:8000") is True

    # Vite's dev server proxying to the API: Origin is the dev server, Host is the API.
    assert origin_allowed("http://localhost:5173", "127.0.0.1:8000") is True

    # A LAN deployment reached by IP.
    assert origin_allowed("http://192.168.1.20:8000", "192.168.1.20:8000") is True

    # An explicitly configured origin (reverse proxy that rewrites Host).
    assert origin_allowed("https://portal.example", "10.0.0.5:8000", ["https://portal.example"]) is True

    # Anything else is a page on another site reaching into this backend.
    assert origin_allowed("https://evil.example", "127.0.0.1:8000") is False
    assert origin_allowed("null", "127.0.0.1:8000") is False
    assert origin_allowed("ftp://127.0.0.1", "127.0.0.1:8000") is False


# ------------------------------------------------------------------- http api


def test_api_is_open_while_auth_is_disabled():
    client = TestClient(app)
    try:
        assert client.get("/api/pipelines").status_code == 200
    finally:
        client.close()


def test_api_requires_a_session_when_auth_is_enabled():
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = True
    client = TestClient(app)
    try:
        assert client.get("/api/pipelines").status_code == 401
        # Health and the login endpoints must stay reachable, or nobody can ever
        # authenticate in the first place.
        assert client.get("/health").status_code == 200
        assert client.get("/api/auth/me").status_code == 200

        token = create_token({"sub": "admin", "role": "admin"})
        authed = client.get("/api/pipelines", headers={"Authorization": f"Bearer {token}"})
        assert authed.status_code == 200

        # A token in the session cookie works too: that is the browser's path.
        client.cookies.set("obelisk_session", token)
        assert client.get("/api/pipelines").status_code == 200
    finally:
        client.close()
        settings.AUTH_ENABLED = previous


def test_a_forged_token_is_refused():
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = True
    client = TestClient(app)
    try:
        forged = create_token({"sub": "admin", "role": "admin"})
        header, payload, _signature = forged.split(".")
        tampered = f"{header}.{payload}.not-the-real-signature"
        response = client.get("/api/pipelines", headers={"Authorization": f"Bearer {tampered}"})
        assert response.status_code == 401
    finally:
        client.close()
        settings.AUTH_ENABLED = previous


# ----------------------------------------------------------------- websockets


def test_a_foreign_origin_cannot_open_a_socket():
    client = TestClient(app)
    try:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                "/ws/events", headers={"origin": "https://evil.example"}
            ):
                pass

        # The same host is the page talking to its own backend.
        with client.websocket_connect("/ws/events", headers={"origin": "http://testserver"}):
            pass
    finally:
        client.close()


def test_sockets_require_a_token_when_auth_is_enabled():
    previous = settings.AUTH_ENABLED
    settings.AUTH_ENABLED = True
    client = TestClient(app)
    try:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/events"):
                pass

        token = create_token({"sub": "admin", "role": "admin"})
        with client.websocket_connect(f"/ws/events?token={token}"):
            pass
    finally:
        client.close()
        settings.AUTH_ENABLED = previous


# ------------------------------------------------------------------- terminal


def test_terminal_refuses_a_foreign_origin_before_spawning_a_shell():
    client = TestClient(app)
    try:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                "/api/terminal/ws", headers={"origin": "https://evil.example"}
            ):
                pass
    finally:
        client.close()


def test_terminal_can_be_switched_off_for_a_deployment():
    previous = settings.TERMINAL_ENABLED
    settings.TERMINAL_ENABLED = False
    client = TestClient(app)
    try:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/api/terminal/ws"):
                pass
    finally:
        client.close()
        settings.TERMINAL_ENABLED = previous
# ------------------------------------------------------- startup guard
def test_the_guard_is_inert_while_auth_is_off():
    from app.core.security import assert_credentials_are_not_shipped_defaults

    # AUTH_ENABLED defaults to False, so the shipped password is not yet a problem.
    assert_credentials_are_not_shipped_defaults()


def test_auth_refuses_to_start_with_the_shipped_password():
    """Both shipped secrets are refused; setting one is not enough."""
    from app.core.security import assert_credentials_are_not_shipped_defaults

    previous_auth = settings.AUTH_ENABLED
    previous_password = settings.OBELISK_ADMIN_PASSWORD
    previous_key = settings.OBELISK_MASTER_KEY
    settings.AUTH_ENABLED = True
    try:
        with pytest.raises(RuntimeError):
            assert_credentials_are_not_shipped_defaults()

        # A real password is not enough while the token-signing key is published: a known
        # key lets anyone mint a session.
        settings.OBELISK_ADMIN_PASSWORD = "a-real-password"
        with pytest.raises(RuntimeError):
            assert_credentials_are_not_shipped_defaults()

        settings.OBELISK_MASTER_KEY = "c2Vjb25kLXZhbGlkLTMyLWJ5dGUta2V5LWZvci1vYmVsaXNr"
        assert_credentials_are_not_shipped_defaults()  # no raise
    finally:
        settings.AUTH_ENABLED = previous_auth
        settings.OBELISK_ADMIN_PASSWORD = previous_password
        settings.OBELISK_MASTER_KEY = previous_key


def test_the_login_reads_credentials_from_settings():
    """A password that only exists in `.env` must still authenticate.

    `os.environ` cannot see `.env` — Pydantic loads that file into *fields* — so the
    login route reads settings. This pins the seam the bug lived in.
    """
    import asyncio

    from app.api.routes_auth import LoginRequest, login

    class _Response:
        def __init__(self) -> None:
            self.headers: dict = {}

        def set_cookie(self, *args, **kwargs):
            pass

    class _Request:
        client = None
        headers: dict = {}

    previous_user = settings.OBELISK_ADMIN_USER
    previous_password = settings.OBELISK_ADMIN_PASSWORD
    settings.OBELISK_ADMIN_USER = "ops"
    settings.OBELISK_ADMIN_PASSWORD = "only-in-dotenv"
    try:
        result = asyncio.run(
            login(
                LoginRequest(username="ops", password="only-in-dotenv"),
                _Request(),
                _Response(),
            )
        )
        assert result.user.username == "ops"
    finally:
        settings.OBELISK_ADMIN_USER = previous_user
        settings.OBELISK_ADMIN_PASSWORD = previous_password
