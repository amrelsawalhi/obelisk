"""Superset source sync — the one place that writes Superset's database list.

Obelisk already knows where every data source is; Superset keeps its own separate list
that somebody would otherwise retype by hand. This module copies them across, and the
copy is not a copy of text: the platform reaches a source at ``127.0.0.1:5434`` because
it runs on the host, while Superset runs in a container where ``127.0.0.1`` is itself.
Pushing that address verbatim would store a datasource that looks fine and fails the
first time anyone queries it, so every connection is *translated* first:

* a connection owned by a deployed service → ``obelisk_<service>:<container port>``;
* a local address published by some container → that container's name and port;
* an external host → unchanged, because it is addressable from anywhere;
* a local address nothing publishes → **skipped with a reason**, never pushed broken.

Upserts are keyed on ``database_name`` (= the platform's connection name, which is
unique), so re-running is idempotent and a redeploy corrects drift instead of creating
duplicates. A source type Superset has no connector for (MongoDB, Oracle) is a skip, not
a failure: the report says why, and the deploy still succeeds.

A correct address is only half of a working datasource. Two things the live deployment
taught this module, both of which fail *after* a healthy dry run because a dry run sends
nothing: Superset rejects a write that carries a Bearer token but no ``X-CSRFToken``
(``400 The CSRF token is missing``), and it runs its own connection test on create, which
fails with a bare ``Connection failed`` when the container has no driver for the URI's
scheme. So the sync fetches a CSRF token at login, and :mod:`app.core.dsn` names the
drivers the Superset deploy installs into its container.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.docker_manager import docker_mgr
from app.core.dsn import bi_uri_driver, build_sqlalchemy_uri
from app.core.service_blueprints import get_blueprint
from app.core.vault import decrypt
from app.models.connection import Connection
from app.models.service_config import ServiceConfig

SUPERSET = "superset"

# Hosts that mean "the machine this process runs on". For a container consumer that is
# their own loopback, so these are the only addresses that must be rewritten.
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}

# Superset's engine spec names, which are not always the SQLAlchemy dialect names.
_ENGINE_NAMES = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mysql",
    "clickhouse": "clickhouse",
}

_MASK_RE = re.compile(r"://([^/@:]+):([^@]*)@")


def mask_uri(uri: str) -> str:
    """Replaces the password in a ``scheme://user:pass@host`` URI with ``***``.

    Reports are shown in the UI and written to logs, so the vaulted password must not
    travel with them. Everything else is left byte-for-byte so an operator can check that
    the translation to a container name actually happened.
    """
    return _MASK_RE.sub(lambda m: f"://{m.group(1)}:***@", uri, count=1)


class SupersetError(RuntimeError):
    """A Superset call failed; carries the HTTP status and message into the report."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class SourceResult:
    """What happened (or would happen) to one connection."""

    name: str
    action: str  # pending | would-upsert | created | updated | deleted | absent | skipped | failed
    detail: str = ""
    uri: Optional[str] = None  # masked; None when no URI could be built
    target: Optional["Target"] = None  # resolved address, so callers need not resolve twice

    def as_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "action": self.action, "detail": self.detail, "uri": self.uri}


@dataclass
class Target:
    """Where Superset has to look for a source, and why it cannot look at all."""

    host: str
    port: int
    container: Optional[str] = None  # container Superset must be able to resolve
    reason: Optional[str] = None     # set when the source is unreachable from Superset


@dataclass(frozen=True)
class Auth:
    """A logged-in Superset API session: the JWT plus the CSRF token writes need.

    Superset validates CSRF on every POST/PUT/DELETE even when the request carries a
    Bearer token, so the token is fetched once at login and reused for the whole run.
    Without it each source came back ``400 Bad Request: The CSRF token is missing``,
    which is invisible to the dry run because a dry run sends nothing.
    """

    token: str
    csrf: Optional[str] = None


def _engine_for(scheme: str) -> str:
    base = scheme.split("+", 1)[0]
    return _ENGINE_NAMES.get(base, base)


class SupersetSync:
    """Translates platform connections into Superset datasources.

    ``client`` and ``docker`` are injectable so tests never touch a socket or a daemon:
    one MockTransport client proves the request shapes, a stub docker proves the rewrite.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
        docker: Any = None,
    ):
        self._base_url = (base_url or "").rstrip("/")
        self._client = client
        self._docker = docker or docker_mgr

    # -- address translation ------------------------------------------------

    def _resolve(self, conn: Connection) -> Target:
        """Where Superset must connect to reach ``conn``. Reads Docker, writes nothing."""
        if conn.service_name:
            bp = get_blueprint(conn.service_name)
            if bp is None:
                return Target(conn.host, conn.port, reason=f"unknown service {conn.service_name!r}")
            container = f"obelisk_{conn.service_name}"
            try:
                container_port = int(str(bp.primary_container_port).split("/")[0])
            except (TypeError, ValueError):
                return Target(conn.host, conn.port, reason=f"{container} declares no primary port")
            return Target(container, container_port, container=container)

        host = (conn.host or "").strip().lower()
        if host not in _LOOPBACK_HOSTS:
            return Target(conn.host, conn.port)  # external host: reachable as written

        owner = self._docker.published_port_owner(conn.port)
        if not owner:
            return Target(
                conn.host,
                conn.port,
                reason=f"nothing publishes host port {conn.port}; Superset cannot reach it",
            )
        return Target(owner["container"], owner["container_port"], container=owner["container"])

    def _build(self, conn: Connection, target: Target) -> str:
        """The SQLAlchemy URI for one connection, pointed at its translated address."""
        extras = {
            str(k): str(v)
            for k, v in (conn.options or {}).items()
            if isinstance(v, (str, int, float, bool))
        }
        return build_sqlalchemy_uri(
            conn.source_type,
            target.host,
            target.port,
            username=conn.username,
            password=decrypt(conn.encrypted_credentials or ""),
            database=conn.database,
            extras=extras or None,
        )

    def _prepare(self, conn: Connection, heal: bool = True) -> SourceResult:
        """Build the URI and decide the outcome without any HTTP. Blocking (Docker).

        Whether Superset has an engine for this source type is a property of the *type*,
        so it is checked before the address: a MongoDB row must be told it has no BI
        connector, not that some port looks unpublished.

        ``heal`` distinguishes a sync from a preview. A sync attaches a source container to
        the Obelisk network when it is missing; a preview is reached by a GET, so it only
        *reports* that repair instead of performing it.
        """
        note = ""
        try:
            bi_uri_driver(conn.source_type)  # raises ValueError when there is no connector
            target = self._resolve(conn)
            if target.reason:
                return SourceResult(conn.name, "skipped", target.reason)
            if target.container:
                if heal and not self._docker.ensure_on_network(target.container):
                    return SourceResult(
                        conn.name,
                        "skipped",
                        f"container {target.container} is not running on the Obelisk network",
                    )
                if not heal and not self._docker.is_on_network(target.container):
                    note = f"a sync would also attach {target.container} to the Obelisk network"
            uri = self._build(conn, target)
        except ValueError as exc:  # no BI connector for this source type
            return SourceResult(conn.name, "skipped", str(exc))
        except Exception as exc:  # a decryption or driver fault must not stop the rest
            return SourceResult(conn.name, "failed", f"could not build a URI: {exc}")
        return SourceResult(conn.name, "pending", note, mask_uri(uri), target)

    # -- Superset API -------------------------------------------------------

    async def _login(self, client: httpx.AsyncClient, username: str, password: str) -> Auth:
        response = await client.post(
            "/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db", "refresh": False},
        )
        if response.status_code != 200:
            raise SupersetError(f"login failed: {_brief(response)}", response.status_code)
        token = (response.json() or {}).get("access_token")
        if not token:
            raise SupersetError("login returned no access token")
        return Auth(token, await self._csrf(client, token))

    async def _csrf(self, client: httpx.AsyncClient, token: str) -> str:
        """The token Superset demands on every write, fetched once per login.

        A Bearer token does not exempt a request from CSRF: without this header the create
        and update calls answer ``400 The CSRF token is missing`` and every source fails,
        while the dry run — which sends nothing — reports the sync would work.
        """
        response = await client.get(
            "/api/v1/security/csrf_token/", headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code != 200:
            raise SupersetError(
                f"could not obtain a CSRF token: {_brief(response)}", response.status_code
            )
        csrf = (response.json() or {}).get("result")
        if not csrf:
            raise SupersetError("the CSRF endpoint returned no token")
        return csrf

    @staticmethod
    def _auth(auth: Auth) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {auth.token}", "Content-Type": "application/json"}
        if auth.csrf:
            headers["X-CSRFToken"] = auth.csrf
        return headers

    async def _find(self, client: httpx.AsyncClient, auth: Auth, name: str) -> Optional[int]:
        """The id of the datasource named ``name``, or None. This is the upsert key."""
        query = json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": name}]})
        response = await client.get(
            "/api/v1/database/", params={"q": query}, headers=self._auth(auth)
        )
        if response.status_code != 200:
            raise SupersetError(f"listing databases failed: {_brief(response)}", response.status_code)
        for row in (response.json() or {}).get("result") or []:
            if row.get("database_name") == name:
                return row.get("id")
        return None

    async def _upsert(
        self, client: httpx.AsyncClient, auth: Auth, conn: Connection, uri: str
    ) -> str:
        payload = {
            "database_name": conn.name,
            "sqlalchemy_uri": uri,
            "engine": _engine_for(uri.split("://", 1)[0]),
            "configuration_method": "sqlalchemy_form",
            # Sources are read paths: SQL Lab yes, writes through the BI layer no.
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        }
        existing = await self._find(client, auth, conn.name)
        if existing is None:
            response = await client.post("/api/v1/database/", json=payload, headers=self._auth(auth))
            if response.status_code not in (200, 201):
                raise SupersetError(f"create failed: {_brief(response)}", response.status_code)
            return "created"
        response = await client.put(
            f"/api/v1/database/{existing}", json=payload, headers=self._auth(auth)
        )
        if response.status_code not in (200, 201):
            raise SupersetError(f"update failed: {_brief(response)}", response.status_code)
        return "updated"

    async def _open(self) -> tuple[httpx.AsyncClient, bool]:
        """An HTTP client and whether we own it (an injected one is the caller's)."""
        if self._client is not None:
            return self._client, False
        return httpx.AsyncClient(base_url=self._base_url, timeout=30.0), True

    # -- credentials --------------------------------------------------------

    @staticmethod
    async def _service(session: AsyncSession) -> Optional[ServiceConfig]:
        return (
            await session.execute(select(ServiceConfig).where(ServiceConfig.service_name == SUPERSET))
        ).scalar_one_or_none()

    @staticmethod
    def _credentials(service: ServiceConfig) -> tuple[Optional[str], Optional[str], str]:
        """(username, password, base_url) for the Superset admin account."""
        config = dict(service.config_json or {})
        blueprint = get_blueprint(SUPERSET)
        username = config.get("username") or (blueprint.credential_username if blueprint else None)
        try:
            password = decrypt(config["encrypted_password"]) if config.get("encrypted_password") else None
        except Exception:
            password = None
        port = config.get("host_port") or (blueprint.primary_host_port if blueprint else 8088)
        explicit = os.environ.get("SUPERSET_URL") or getattr(settings, "SUPERSET_URL", None)
        return username, password, (explicit or f"http://127.0.0.1:{port}").rstrip("/")

    def _daemon_alive(self) -> bool:
        """Whether Docker actually answers. Blocking, one ping.

        ``DockerManager.client`` is lazy: the object exists even when nothing is listening
        on the pipe, so holding one proves nothing. Only a request does.
        """
        client = getattr(self._docker, "client", None)
        if client is None:
            return False
        try:
            return bool(client.ping())
        except Exception:
            return False

    def _running_container(self) -> bool:
        """True when the Superset container exists and is running. Blocking (Docker).

        The database row outlives the container: it is written when a deploy succeeds and
        is not cleared when someone removes the container by hand. Trusting it alone would
        report "did not answer", blaming the network for a container that is not there.
        """
        if not self._daemon_alive():
            # No daemon means "unknown", not "absent": telling the operator to deploy again
            # would send them to fix a container that may well be running.
            raise SupersetError(
                "the Docker daemon is unreachable; start Docker, then sync again"
            )
        try:
            container = self._docker.get_service_container(SUPERSET)
        except ValueError as exc:  # a foreign container is refused, never adopted
            raise SupersetError(str(exc))
        if container is None:
            return False
        return getattr(container, "status", "running") == "running"

    async def _connect(self, session: AsyncSession) -> tuple[str, str, str]:
        """Loads the deployed Superset and its admin credentials, refusing politely."""
        service = await self._service(session)
        if service is None:
            raise SupersetError("Superset is not deployed")
        if not await asyncio.to_thread(self._running_container):
            raise SupersetError(
                "Superset container obelisk_superset is not running; deploy Superset first"
            )
        username, password, base_url = self._credentials(service)
        if not username or not password:
            raise SupersetError(
                "no Superset admin credentials are vaulted; redeploy Superset to create them"
            )
        if service.status not in ("ready", "running", "starting"):
            raise SupersetError(f"Superset is {service.status}; start it before syncing sources")
        if not self._base_url:
            self._base_url = base_url
        return username, password, self._base_url

    # -- public API ---------------------------------------------------------

    async def _connections(self, session: AsyncSession) -> List[Connection]:
        rows = await session.execute(select(Connection).order_by(Connection.name))
        return list(rows.scalars().all())

    async def plan(self, session: AsyncSession) -> Dict[str, Any]:
        """What a sync would do, without touching Superset. Passwords are masked."""
        connections = await self._connections(session)
        prepared = [await asyncio.to_thread(self._prepare, conn, False) for conn in connections]
        sources = [
            SourceResult(
                r.name,
                r.action if r.action != "pending" else "would-upsert",
                r.detail,
                r.uri,
            )
            for r in prepared
        ]
        return self._report(True, "dry run", sources)

    async def run(self, session: AsyncSession) -> Dict[str, Any]:
        """Pushes every reachable connection into Superset. Never raises for one source."""
        connections = await self._connections(session)
        try:
            username, password, _ = await self._connect(session)
        except SupersetError as exc:
            return self._report(
                False, str(exc), [SourceResult(c.name, "skipped", str(exc)) for c in connections]
            )

        client, owned = await self._open()
        try:
            auth = await self._login(client, username, password)
        except (httpx.HTTPError, SupersetError) as exc:
            if owned:
                await client.aclose()
            detail = f"Superset did not answer at {self._base_url}: {exc}"
            return self._report(
                False, detail, [SourceResult(c.name, "failed", detail) for c in connections]
            )

        results: List[SourceResult] = []
        try:
            for conn in connections:
                prepared = await asyncio.to_thread(self._prepare, conn)
                if prepared.action != "pending":
                    results.append(prepared)
                    continue
                uri = await asyncio.to_thread(self._build, conn, prepared.target)
                try:
                    action = await self._upsert(client, auth, conn, uri)
                    results.append(SourceResult(conn.name, action, "synced", prepared.uri))
                except (httpx.HTTPError, SupersetError) as exc:
                    results.append(SourceResult(conn.name, "failed", str(exc), prepared.uri))
        finally:
            if owned:
                await client.aclose()
        return self._report(True, "synced", results)

    async def sync_one(self, session: AsyncSession, conn: Connection) -> SourceResult:
        """Upserts a single connection after it was written to the vault."""
        try:
            username, password, _ = await self._connect(session)
        except SupersetError as exc:
            return SourceResult(conn.name, "skipped", str(exc))
        prepared = await asyncio.to_thread(self._prepare, conn)
        if prepared.action != "pending":
            return prepared
        client, owned = await self._open()
        try:
            auth = await self._login(client, username, password)
            uri = await asyncio.to_thread(self._build, conn, prepared.target)
            action = await self._upsert(client, auth, conn, uri)
            return SourceResult(conn.name, action, "synced", prepared.uri)
        except (httpx.HTTPError, SupersetError) as exc:
            return SourceResult(conn.name, "failed", str(exc), prepared.uri)
        finally:
            if owned:
                await client.aclose()

    async def remove(self, session: AsyncSession, name: str) -> SourceResult:
        """Deletes the datasource whose platform connection was deleted."""
        try:
            username, password, _ = await self._connect(session)
        except SupersetError as exc:
            return SourceResult(name, "skipped", str(exc))
        client, owned = await self._open()
        try:
            auth = await self._login(client, username, password)
            existing = await self._find(client, auth, name)
            if existing is None:
                return SourceResult(name, "absent", "not present in Superset")
            response = await client.delete(f"/api/v1/database/{existing}", headers=self._auth(auth))
            if response.status_code not in (200, 204):
                raise SupersetError(f"delete failed: {_brief(response)}", response.status_code)
            return SourceResult(name, "deleted", "removed from Superset")
        except (httpx.HTTPError, SupersetError) as exc:
            return SourceResult(name, "failed", str(exc))
        finally:
            if owned:
                await client.aclose()

    @staticmethod
    def _report(ok: bool, detail: str, sources: List[SourceResult]) -> Dict[str, Any]:
        counts = Counter(s.action for s in sources)
        return {
            "ok": ok,
            "detail": detail,
            "counts": {
                action: counts.get(action, 0)
                for action in (
                    "created",
                    "updated",
                    "would-upsert",
                    "deleted",
                    "absent",
                    "skipped",
                    "failed",
                )
            },
            "sources": [s.as_dict() for s in sources],
        }


def _brief(response: httpx.Response) -> str:
    """One line of Superset's error, without dumping a whole HTML page into the UI."""
    try:
        body = response.json()
        message = body.get("message") if isinstance(body, dict) else None
        if message:
            return f"HTTP {response.status_code}: {message}"
    except Exception:
        pass
    lines = (response.text or "").strip().splitlines()
    first = lines[0][:200] if lines else ""
    return f"HTTP {response.status_code}: {first}" if first else f"HTTP {response.status_code}"


# -- drift hooks -------------------------------------------------------------
#
# Called from connection CRUD. Each opens its own session because the request's session is
# closed by the time a background task runs, and each swallows every error: a Superset that
# is down must never block writing to the vault.

async def _in_background(name: str, remove: bool) -> None:
    import logging

    from app.database import AsyncSessionLocal

    log = logging.getLogger(__name__)
    try:
        async with AsyncSessionLocal() as session:
            service = await SupersetSync._service(session)
            if service is None or service.status not in ("ready", "running"):
                return
            sync = SupersetSync()
            if remove:
                result = await sync.remove(session, name)
            else:
                conn = (
                    await session.execute(select(Connection).where(Connection.name == name))
                ).scalar_one_or_none()
                if conn is None:
                    return
                result = await sync.sync_one(session, conn)
            log.info("superset sync %s: %s (%s)", name, result.action, result.detail)
    except Exception:
        log.warning("superset sync %s failed; Superset is now out of date", name, exc_info=True)


async def sync_connection_quietly(name: str) -> None:
    """Background: upsert one connection into Superset. Never raises."""
    await _in_background(name, remove=False)


async def remove_connection_quietly(name: str) -> None:
    """Background: delete one datasource from Superset. Never raises."""
    await _in_background(name, remove=True)


superset_sync = SupersetSync()




