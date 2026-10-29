"""Identity, asked in one place, enforced in none yet.

``AUTH_ENABLED`` is off by default, so every request is the local operator. What
exists now is the *question* -- who is calling? -- so switching enforcement on becomes
a setting rather than a refactor:

* ``require_principal`` is an app-level dependency. It is a no-op while auth is off
  and answers 401 for ``/api/*`` once it is on. ``/api/auth/*`` and ``/health`` stay
  open, or nobody could ever log in.
* ``authorize_websocket`` gates the sockets. A browser cannot set an
  ``Authorization`` header on a WebSocket, so it reads the session cookie, then
  ``?token=``.
* ``origin_allowed`` closes the drive-by hole: every browser sends ``Origin`` on a
  WebSocket handshake, and a page on another site must not be able to drive this
  backend's shell just because the backend is reachable from the browser.

The token primitive lives here rather than in ``routes_auth.py`` because every layer
needs it; the endpoint module imports it from here.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import urlparse

from fastapi import HTTPException, WebSocket, status
from starlette.requests import HTTPConnection

from app.config import settings
from app import config as config_defaults

logger = logging.getLogger(__name__)

COOKIE_NAME = "obelisk_session"
DEFAULT_EXPIRATION_SECONDS = 86400 * 7  # 7 days

#: What "nobody logged in" looks like while auth is off. Marked explicitly so a log
#: line or an audit trail can tell it apart from a real administrator.
LOCAL_PRINCIPAL: Dict[str, Any] = {
    "sub": "local",
    "role": "admin",
    "authenticated": False,
    "auth": "disabled",
}

#: Paths that must keep working without a session, or logging in is impossible.
PUBLIC_API_PREFIXES = ("/api/auth/",)
PUBLIC_PATHS = frozenset({
    "/health", "/api/health", "/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect",
})

LOOPBACK_ORIGIN_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def auth_enabled() -> bool:
    return bool(getattr(settings, "AUTH_ENABLED", False))


def is_loopback_client(host: Optional[str]) -> bool:
    """Whether a socket peer address is this machine.

    The terminal hands out a real shell, so while auth is off it is only offered to a
    client that is already on the box. ``TestClient``/ASGI transports report a name
    rather than an address; that is not loopback, and the terminal is refused there
    unless somebody explicitly authenticates.
    """
    if not host:
        return False
    normalized = str(host).strip().strip("[]").lower()
    if normalized.startswith("::ffff:"):
        normalized = normalized[len("::ffff:"):]
    if normalized in LOOPBACK_ORIGIN_HOSTS:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64_decode(data_str: str) -> bytes:
    padding = "=" * (4 - (len(data_str) % 4)) if len(data_str) % 4 != 0 else ""
    return base64.urlsafe_b64decode(data_str + padding)


def create_token(payload: Dict[str, Any], expires_seconds: int = DEFAULT_EXPIRATION_SECONDS) -> str:
    """Signs an HS256 JWT with the master key, in the format this build reads."""
    header = {"alg": "HS256", "typ": "JWT"}
    body = payload.copy()
    now = int(time.time())
    body["iat"] = now
    body["exp"] = now + expires_seconds

    header_b64 = _b64_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64_encode(json.dumps(body, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(settings.OBELISK_MASTER_KEY.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64_encode(signature)}"


def verify_token(token: str) -> Dict[str, Any]:
    """Validates signature and expiry; raises 401 rather than returning a verdict."""
    parts = token.strip().split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed session token.")

    header_b64, payload_b64, sig_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected = hmac.new(settings.OBELISK_MASTER_KEY.encode("utf-8"), signing_input, hashlib.sha256).digest()
    try:
        actual = _b64_decode(sig_b64)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed session token.") from exc
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session token signature.")

    try:
        payload = json.loads(_b64_decode(payload_b64).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session payload.") from exc

    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session has expired.")
    return payload
def _token_from(cookies: Mapping[str, Any], headers: Mapping[str, str]) -> Optional[str]:
    """Session cookie first, then a bearer header (HTTP clients only)."""
    token = (cookies or {}).get(COOKIE_NAME)
    if token:
        return token
    header = (headers or {}).get("authorization") or (headers or {}).get("Authorization") or ""
    if header.lower().startswith("bearer "):
        return header.split(" ", 1)[1].strip() or None
    return None


def require_principal(connection: HTTPConnection) -> Dict[str, Any]:
    """App-level guard: who is calling, if we are checking yet.

    Registered once on the app so every current and future ``/api`` route inherits it.
    While auth is off it returns the local principal and changes nothing; once it is on
    it answers 401 for the same routes without any per-router edits.
    """
    if not auth_enabled():
        return dict(LOCAL_PRINCIPAL)
    if isinstance(connection, WebSocket):
        # Sockets authorize in `authorize_websocket`; a JSON 401 is not something a
        # WebSocket handshake can answer with.
        return dict(LOCAL_PRINCIPAL)

    path = connection.url.path
    if not path.startswith("/api/"):
        return dict(LOCAL_PRINCIPAL)
    if path.startswith(PUBLIC_API_PREFIXES) or path in PUBLIC_PATHS:
        return dict(LOCAL_PRINCIPAL)

    token = _token_from(connection.cookies, connection.headers)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        return verify_token(token)
    except HTTPException as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


DEFAULT_ADMIN_PASSWORD = config_defaults.DEFAULT_ADMIN_PASSWORD
ALLOW_DEFAULT_CREDENTIALS_ENV = "OBELISK_ALLOW_DEFAULT_CREDENTIALS"


def assert_credentials_are_not_shipped_defaults() -> None:
    """Refuse to enforce auth while the shipped credentials are still in place.

    Turning enforcement on with a published default is a locked front door with the key
    taped to it. The master key matters as much as the password: it signs session tokens,
    so a known key lets anyone mint one. Both are read from ``settings``, which means a
    value set in ``.env`` counts — reading ``os.environ`` directly ignored it.
    """
    if not auth_enabled():
        return
    if (
        getattr(settings, "ALLOW_DEFAULT_CREDENTIALS", False)
        or os.getenv(ALLOW_DEFAULT_CREDENTIALS_ENV, "").strip().lower() in ("1", "true", "yes", "on")
    ):
        return

    problems: list[str] = []
    if settings.OBELISK_ADMIN_PASSWORD == config_defaults.DEFAULT_ADMIN_PASSWORD:
        problems.append("OBELISK_ADMIN_PASSWORD is still the shipped default")
    if settings.OBELISK_MASTER_KEY == config_defaults.DEFAULT_MASTER_KEY:
        problems.append("OBELISK_MASTER_KEY is still the shipped sample key")

    if problems:
        raise RuntimeError(
            "AUTH_ENABLED is on but " + " and ".join(problems) + ". Set real values "
            "(OBELISK_ADMIN_USER, OBELISK_ADMIN_PASSWORD, OBELISK_MASTER_KEY) in .env or "
            "the environment before exposing this instance."
        )


UNAUTHENTICATED_OPT_IN_ENV = "OBELISK_ALLOW_UNAUTHENTICATED"


def assert_deployment_is_safe() -> None:
    """Refuse an unauthenticated API that the network can reach.

    Auth is off by default so a single-operator install works without a login. That is
    only safe while the API is private: on ``0.0.0.0`` every route — ingestion, SQL,
    file writes, service deployment — answers anyone who can reach the port. A
    production-labelled deployment therefore has to choose one of three explicit
    states: authentication on, a loopback bind, or a stated opt-in.
    """
    if auth_enabled():
        return
    if (getattr(settings, "ENVIRONMENT", "") or "").strip().lower() not in (
        "production",
        "prod",
    ):
        return

    opt_in = os.getenv(UNAUTHENTICATED_OPT_IN_ENV, "").strip().lower()
    host = (getattr(settings, "HOST", "") or "").strip()
    # An empty HOST means uvicorn's "all interfaces" default — same exposure as 0.0.0.0.
    effective_host = host or "0.0.0.0"
    if (
        not is_loopback_client(effective_host)
        and opt_in not in ("1", "true", "yes", "on")
    ):
        raise RuntimeError(
            f"AUTH_ENABLED is off while ENVIRONMENT=production and HOST={host or '(empty)'!r}: "
            "every API route is open to anyone who can reach it. Set OBELISK_AUTH_ENABLED=1 "
            "(with a real OBELISK_ADMIN_PASSWORD and OBELISK_MASTER_KEY), bind "
            "HOST=127.0.0.1, or set "
            f"{UNAUTHENTICATED_OPT_IN_ENV}=1 if this port is genuinely private."
        )
    if opt_in in ("1", "true", "yes", "on"):
        logger.warning(
            "Serving an unauthenticated API on %s because %s=1 is set. Any client that "
            "can reach this port has full control of the instance.",
            effective_host,
            UNAUTHENTICATED_OPT_IN_ENV,
        )


def configured_trusted_proxies() -> list[Any]:
    """The proxy addresses whose ``X-Forwarded-For`` this instance may believe."""
    raw = (getattr(settings, "TRUSTED_PROXIES", "") or "").strip()
    networks = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            logger.warning("Ignoring unparseable trusted proxy %r", item)
    return networks


def peer_is_trusted_proxy(host: Optional[str]) -> bool:
    """Whether the TCP peer is one of the configured reverse proxies."""
    if not host:
        return False
    normalized = host.strip().strip("[]").lower()
    if normalized.startswith("::ffff:"):
        normalized = normalized[len("::ffff:"):]
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return any(address in network for network in configured_trusted_proxies())


def unauthenticated_terminal_allowed(peer: Optional[str], headers: Mapping[str, str]) -> bool:
    """While auth is off, whether this client may open the shell socket.

    The peer address alone is not enough behind a proxy: a proxy running on this machine
    makes every connection look loopback. So a forwarding header arriving from a peer
    that is not a configured trusted proxy is refused outright, and a header from a
    trusted proxy is resolved to the client it forwards for — which must still be
    loopback.
    """
    forwarded = headers.get("x-forwarded-for") or headers.get("X-Forwarded-For")
    if not forwarded:
        return is_loopback_client(peer)
    if not peer_is_trusted_proxy(peer):
        return False
    trusted = configured_trusted_proxies()
    for hop in reversed([part.strip() for part in forwarded.split(",") if part.strip()]):
        try:
            address = ipaddress.ip_address(hop.strip("[]"))
        except ValueError:
            return is_loopback_client(hop)
        if trusted and any(address in network for network in trusted):
            continue
        return address.is_loopback
    # Every hop is a trusted proxy: the caller is unknown, so it is not loopback.
    return False


def configured_origins() -> list[str]:
    """Extra origins a deployment allows, from ``ALLOWED_ORIGINS`` (comma separated)."""
    raw = getattr(settings, "ALLOWED_ORIGINS", "") or ""
    return [item.strip().rstrip("/") for item in raw.split(",") if item.strip()]


def origin_allowed(
    origin: Optional[str],
    host: Optional[str],
    allowed: Optional[Iterable[str]] = None,
) -> bool:
    """Whether a browser origin may open a socket on this backend.

    A browser always sends ``Origin`` on a WebSocket handshake, which is what makes
    this check meaningful: a page from another site must not be able to drive this
    backend just because the operator's browser can reach it. A missing origin means a
    non-browser client (curl, a bot, the SDK) and is left to the auth check.
    """
    if not origin:
        return True
    normalized = origin.rstrip("/")
    extra = {item.rstrip("/") for item in (allowed or ())}
    if normalized in extra:
        return True

    parsed = urlparse(normalized)
    if parsed.scheme not in ("http", "https"):
        return False
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return False
    if hostname in LOOPBACK_ORIGIN_HOSTS:
        return True
    if host:
        request_host = host.split(":")[0].strip("[]").lower()
        if hostname == request_host:
            return True
    return False


async def authorize_websocket(websocket: WebSocket, channel: str) -> Optional[Dict[str, Any]]:
    """Origin + session check for a socket. Closes it and returns None on refusal.

    Must be called *before* ``accept()`` so a refused client gets a handshake denial
    instead of a socket that opens and then misbehaves.
    """
    origin = websocket.headers.get("origin")
    host = websocket.headers.get("host")
    if not origin_allowed(origin, host, configured_origins()):
        logger.warning(
            "Refused %s socket: origin %r is not this host (%r).", channel, origin, host
        )
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Origin not allowed.")
        return None

    if not auth_enabled():
        return dict(LOCAL_PRINCIPAL)

    token = _token_from(websocket.cookies, websocket.headers) or websocket.query_params.get("token")
    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required.")
        return None
    try:
        principal = verify_token(token)
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid session.")
        return None

    logger.info("%s socket authorized for %r", channel, principal.get("sub"))
    return principal