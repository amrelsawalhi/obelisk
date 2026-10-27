"""Superset source sync — Superset's database list must be Obelisk's list, translated.

Properties under test:

* every component of a URI is percent-encoded, so a hostile password cannot break it;
* a loopback address is rewritten to the container that publishes that port, and a
  managed service connection goes to its own container by name;
* an external host is passed through untouched, because it is reachable as written;
* an address nothing publishes is **skipped with a reason**, never pushed broken;
* a source type Superset has no connector for (MongoDB) is a skip, not a failure;
* upserts key on ``database_name``: a second sync updates, it does not duplicate;
* every write carries the CSRF token fetched at login — a Bearer token alone is refused;
* a Superset that is down produces a report instead of an exception.

There is no mocking here: ``httpx.MockTransport`` is httpx's own in-process transport,
and ``FakeDocker`` is a real object implementing the two methods the syncer calls.
"""
import json
import uuid

import httpx
import pytest
from sqlalchemy import delete
from sqlalchemy.engine import make_url

from app.core.dsn import (
    BI_DRIVER_MODULES,
    BI_DRIVER_PACKAGES,
    BI_URI_DRIVERS,
    bi_container_packages,
    build_sqlalchemy_uri,
)
from app.core.service_blueprints import get_blueprint
from app.core.vault import encrypt
from app.database import AsyncSessionLocal
from app.models.connection import Connection
from app.models.service_config import ServiceConfig
from app.services.superset import SupersetSync, mask_uri


def _name(slug: str) -> str:
    return f"test_{slug}_{uuid.uuid4().hex[:6]}"


class _Container:
    """The only thing the syncer reads off a container: whether it is running."""

    def __init__(self, status="running"):
        self.status = status


class _Daemon:
    """Stands in for ``docker.DockerClient``: only ``ping`` is consulted."""

    def __init__(self, alive: bool):
        self.alive = alive

    def ping(self):
        if not self.alive:
            raise ConnectionError("cannot connect to the Docker daemon")
        return True


class FakeDocker:
    """A real stand-in for DockerManager: answers the reads the syncer makes."""

    def __init__(
        self, publishers=None, reachable=True, container="running", on_network=True, daemon=True
    ):
        self.publishers = {int(k): v for k, v in (publishers or {}).items()}
        self.reachable = reachable
        self.container = _Container(container) if container else None
        self.on_network = on_network
        # DockerManager holds a lazy client on ``self.client``; the syncer pings it.
        self.client = _Daemon(daemon)
        self.asked = []
        self.ensured = []

    def get_service_container(self, service_name):
        return self.container

    def published_port_owner(self, host_port):
        self.asked.append(int(host_port))
        return self.publishers.get(int(host_port))

    def is_on_network(self, container_name, network=None):
        return self.on_network

    def ensure_on_network(self, container_name, network=None):
        self.ensured.append(container_name)
        return self.reachable


class SupersetAPI:
    """An in-process Superset REST API that records every call it receives."""

    def __init__(self, databases=None, fail_login=False, offline=False):
        self.databases = {name: dict(meta) for name, meta in (databases or {}).items()}
        self.calls = []
        self.fail_login = fail_login
        self.offline = offline
        self._next_id = 1 + max((d["id"] for d in self.databases.values()), default=0)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handle), base_url="http://superset.test"
        )

    def posts(self):
        return [c for c in self.calls if c["method"] == "POST" and c["path"] == "/api/v1/database/"]

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.offline:
            raise httpx.ConnectError("connection refused", request=request)
        path = request.url.path
        self.calls.append(
            {
                "method": request.method,
                "path": path,
                "csrf": request.headers.get("X-CSRFToken"),
            }
        )

        if path == "/api/v1/security/login":
            if self.fail_login:
                return httpx.Response(401, json={"message": "bad credentials"})
            return httpx.Response(200, json={"access_token": "tok"})
        if request.headers.get("Authorization") != "Bearer tok":
            return httpx.Response(401, json={"message": "unauthorized"})

        if request.method == "GET" and path == "/api/v1/security/csrf_token/":
            return httpx.Response(200, json={"result": "csrf-token"})

        # Superset 4.x enforces this even for a Bearer-authenticated write: without the
        # header every create/update answers 400 and nothing is ever registered.
        if request.method != "GET" and request.headers.get("X-CSRFToken") != "csrf-token":
            return httpx.Response(
                400,
                json={"errors": [{"message": "400 Bad Request: The CSRF token is missing."}]},
            )

        if request.method == "GET" and path == "/api/v1/database/":
            query = json.loads(request.url.params.get("q") or "{}")
            wanted = (query.get("filters") or [{}])[0].get("value")
            result = [
                {"id": meta["id"], "database_name": name}
                for name, meta in self.databases.items()
                if name == wanted
            ]
            return httpx.Response(200, json={"result": result, "count": len(result)})

        if request.method == "POST" and path == "/api/v1/database/":
            body = json.loads(request.content)
            if body["database_name"] in self.databases:
                return httpx.Response(422, json={"message": "already exists"})
            self.databases[body["database_name"]] = {
                "id": self._next_id,
                "uri": body["sqlalchemy_uri"],
                "payload": body,
            }
            self._next_id += 1
            return httpx.Response(201, json={"id": self._next_id - 1})

        if request.method == "PUT" and path.startswith("/api/v1/database/"):
            db_id = int(path.rsplit("/", 1)[-1])
            body = json.loads(request.content)
            for name, meta in self.databases.items():
                if meta["id"] == db_id and body["database_name"] == name:
                    meta["uri"] = body["sqlalchemy_uri"]
                    meta["payload"] = body
                    return httpx.Response(200, json={"id": db_id})
            return httpx.Response(404, json={"message": "not found"})

        if request.method == "DELETE" and path.startswith("/api/v1/database/"):
            db_id = int(path.rsplit("/", 1)[-1])
            for name, meta in list(self.databases.items()):
                if meta["id"] == db_id:
                    del self.databases[name]
                    return httpx.Response(200, json={"message": "deleted"})
            return httpx.Response(404, json={"message": "not found"})

        return httpx.Response(404, json={"message": f"no route {request.method} {path}"})


def _postgres(name: str, **overrides) -> Connection:
    values = {
        "name": name,
        "source_type": "postgres",
        "host": "127.0.0.1",
        "port": 5434,
        "database": "sourcedb",
        "username": "testuser",
        "encrypted_credentials": encrypt("supersecret"),
        "options": None,
        "service_name": None,
    }
    values.update(overrides)
    return Connection(**values)


async def _seed(connections, superset_status="ready", deploy_superset=True) -> None:
    """Writes the rows a sync reads; ``_unseed`` takes them back out."""
    async with AsyncSessionLocal() as session:
        await _clear(session)
        if deploy_superset:
            session.add(
                ServiceConfig(
                    service_name="superset",
                    deployment_type="local",
                    status=superset_status,
                    host="localhost",
                    port=8088,
                    allocated_memory_mb=0,
                    config_json={
                        "username": "admin",
                        "encrypted_password": encrypt("supersecret"),
                        "host_port": 8088,
                    },
                )
            )
        for conn in connections:
            session.add(conn)
        await session.commit()


async def _unseed() -> None:
    async with AsyncSessionLocal() as session:
        await _clear(session)
        await session.commit()


async def _clear(session) -> None:
    await session.execute(delete(Connection).where(Connection.name.like("test%")))
    await session.execute(
        delete(ServiceConfig).where(ServiceConfig.service_name == "superset")
    )


# -- the URI itself ----------------------------------------------------------

def test_a_hostile_password_survives_the_round_trip() -> None:
    """``@``, ``:``, ``/`` and a space must stay *in* the password, not change the URL."""
    uri = build_sqlalchemy_uri(
        "postgres",
        "obelisk_source_postgres",
        5432,
        username="analyst",
        password="p@ss:w/rd ",
        database="sales",
    )
    parsed = make_url(uri)
    assert parsed.username == "analyst"
    assert parsed.password == "p@ss:w/rd "
    assert (parsed.host, parsed.port) == ("obelisk_source_postgres", 5432)
    assert parsed.database == "sales"
    assert mask_uri(uri) == "postgresql+psycopg2://analyst:***@obelisk_source_postgres:5432/sales"


# -- address translation -----------------------------------------------------

@pytest.mark.asyncio
async def test_loopback_is_rewritten_to_the_container_publishing_the_port() -> None:
    """The platform stores 127.0.0.1:5434; inside Superset that address is Superset."""
    name = _name("pg")
    api = SupersetAPI()
    docker = FakeDocker({5434: {"container": "obelisk_source_postgres", "container_port": 5432}})
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=docker).run(session)
    finally:
        await _unseed()

    assert report["ok"] is True
    assert report["counts"]["created"] == 1
    uri = make_url(api.databases[name]["uri"])
    assert (uri.host, uri.port, uri.database) == ("obelisk_source_postgres", 5432, "sourcedb")
    assert uri.password == "supersecret"
    assert docker.ensured == ["obelisk_source_postgres"]


@pytest.mark.asyncio
async def test_a_service_connection_uses_its_own_container_by_name() -> None:
    """A row owned by a deployed service needs no port lookup: the owner is known."""
    name = _name("svc")
    api, docker = SupersetAPI(), FakeDocker()
    await _seed([_postgres(name, service_name="postgres")])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=docker).run(session)
    finally:
        await _unseed()

    assert report["counts"]["created"] == 1
    uri = make_url(api.databases[name]["uri"])
    assert (uri.host, uri.port) == ("obelisk_postgres", 5432)
    assert docker.asked == [], "a known owner must not be guessed from host ports"


@pytest.mark.asyncio
async def test_a_dry_run_attaches_nothing_and_reports_the_repair() -> None:
    """The preview is reached by a GET: it may say a sync would attach the container, but
    it must not attach one to find out."""
    name = _name("preview")
    docker = FakeDocker(on_network=False)
    await _seed([_postgres(name, service_name="postgres")])
    try:
        async with AsyncSessionLocal() as session:
            report = await SupersetSync(docker=docker).plan(session)
    finally:
        await _unseed()

    source = report["sources"][0]
    assert docker.ensured == [], "a preview must not touch the daemon"
    assert source["action"] == "would-upsert"
    assert "would also attach obelisk_postgres" in source["detail"]


@pytest.mark.asyncio
async def test_an_unpublished_loopback_port_is_skipped_not_pushed() -> None:
    """A datasource that cannot connect is worse than an absent one: report the skip."""
    name = _name("dead")
    api = SupersetAPI()
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker()).run(session)
    finally:
        await _unseed()

    assert report["counts"]["skipped"] == 1
    assert api.posts() == []
    assert "nothing publishes host port 5434" in report["sources"][0]["detail"]


@pytest.mark.asyncio
async def test_an_external_host_is_passed_through_untouched() -> None:
    name = _name("ext")
    api, docker = SupersetAPI(), FakeDocker()
    await _seed([_postgres(name, host="aws-0-eu-west-1.pooler.supabase.com", port=6543)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            await SupersetSync(client=client, docker=docker).run(session)
    finally:
        await _unseed()

    uri = make_url(api.databases[name]["uri"])
    assert (uri.host, uri.port) == ("aws-0-eu-west-1.pooler.supabase.com", 6543)
    assert docker.asked == [] and docker.ensured == []


@pytest.mark.asyncio
async def test_a_source_without_a_superset_connector_is_skipped_with_a_reason() -> None:
    """MongoDB has no Superset engine: that is a skip, never a failed deploy."""
    name = _name("mongo")
    api = SupersetAPI()
    await _seed([_postgres(name, source_type="mongodb", port=27018)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker()).run(session)
    finally:
        await _unseed()

    assert report["counts"] == {"created": 0, "updated": 0, "would-upsert": 0, "deleted": 0,
                                "absent": 0, "skipped": 1, "failed": 0}
    assert "No BI connector" in report["sources"][0]["detail"]
    assert api.posts() == []


# -- idempotency and failure -------------------------------------------------

@pytest.mark.asyncio
async def test_a_second_sync_updates_the_same_datasource() -> None:
    """Keyed on database_name: re-running corrects the row instead of duplicating it."""
    name = _name("twice")
    api = SupersetAPI()
    docker = FakeDocker({5434: {"container": "obelisk_source_postgres", "container_port": 5432}})
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            first = await SupersetSync(client=client, docker=docker).run(session)
        async with AsyncSessionLocal() as session, api.client() as client:
            second = await SupersetSync(client=client, docker=docker).run(session)
    finally:
        await _unseed()

    assert first["counts"]["created"] == 1
    assert second["counts"]["updated"] == 1
    assert len(api.databases) == 1
    assert [c["method"] for c in api.calls].count("PUT") == 1


@pytest.mark.asyncio
async def test_superset_being_down_is_reported_not_raised() -> None:
    """A BI outage must not fail a deploy; the operator gets a reason instead."""
    name = _name("down")
    api = SupersetAPI(offline=True)
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker()).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert report["counts"]["failed"] == 1
    assert "did not answer" in report["detail"]


@pytest.mark.asyncio
async def test_superset_not_deployed_is_a_skip_not_a_crash() -> None:
    name = _name("nosuperset")
    api = SupersetAPI()
    await _seed([_postgres(name)], deploy_superset=False)
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker()).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert "not deployed" in report["detail"]
    assert api.calls == [], "nothing may be sent when there is no Superset to send it to"


@pytest.mark.asyncio
async def test_an_unreachable_daemon_is_not_blamed_on_the_container() -> None:
    """With no daemon we know nothing about the container. Saying "not running" would send
    the operator to redeploy a Superset that may be up and healthy."""
    name = _name("nodaemon")
    api = SupersetAPI()
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker(daemon=False)).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert "Docker daemon is unreachable" in report["detail"]
    assert "is not running" not in report["detail"]
    assert api.calls == []


@pytest.mark.asyncio
async def test_a_deleted_container_is_reported_as_not_running() -> None:
    """A row saying ``ready`` outlives a container removed by hand; say so, do not blame
    the network for a Superset that is simply not there."""
    name = _name("removed")
    api = SupersetAPI()
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker(container=None)).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert "is not running" in report["detail"]
    assert report["counts"]["skipped"] == 1
    assert api.calls == []


@pytest.mark.asyncio
async def test_a_stopped_container_is_not_treated_as_running() -> None:
    name = _name("exited")
    api = SupersetAPI()
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker(container="exited")).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert "is not running" in report["detail"]
    assert api.calls == []


@pytest.mark.asyncio
async def test_a_dry_run_shows_the_uri_with_the_password_masked() -> None:
    """The preview endpoint is safe to show in a UI: no vault secret leaves the backend."""
    name = _name("preview")
    docker = FakeDocker({5434: {"container": "obelisk_source_postgres", "container_port": 5432}})
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session:
            report = await SupersetSync(docker=docker).plan(session)
    finally:
        await _unseed()

    assert report["sources"][0]["action"] == "would-upsert"
    assert report["sources"][0]["uri"].endswith("@obelisk_source_postgres:5432/sourcedb")
    assert "supersecret" not in json.dumps(report)


@pytest.mark.asyncio
async def test_deleting_a_connection_deletes_its_datasource() -> None:
    """The Superset row is keyed on the connection name, so it goes with it."""
    name = _name("gone")
    api = SupersetAPI({name: {"id": 7}})
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            result = await SupersetSync(client=client, docker=FakeDocker()).remove(session, name)
    finally:
        await _unseed()

    assert result.action == "deleted"
    assert name not in api.databases




@pytest.mark.asyncio
async def test_every_write_carries_the_csrf_token_fetched_at_login() -> None:
    """Superset refuses a write without ``X-CSRFToken``, Bearer token or not.

    This is exactly what the live deployment did: login succeeded and the dry run promised
    a clean sync, but every create came back ``400 The CSRF token is missing`` — the token
    was never fetched, so nothing was ever registered and Superset's list stayed empty.
    """
    name = _name("csrf")
    api = SupersetAPI()
    docker = FakeDocker({5434: {"container": "obelisk_source_postgres", "container_port": 5432}})
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=docker).run(session)
    finally:
        await _unseed()

    assert report["counts"]["created"] == 1, report
    assert name in api.databases, "the datasource must exist; a bare Bearer token is refused"
    writes = [
        c
        for c in api.calls
        if c["method"] in ("POST", "PUT", "DELETE") and c["path"] != "/api/v1/security/login"
    ]
    assert writes, "the run made no write worth checking"
    assert all(c["csrf"] == "csrf-token" for c in writes), writes
    paths = [c["path"] for c in api.calls]
    assert paths.index("/api/v1/security/csrf_token/") < paths.index("/api/v1/database/"), (
        "the token must be fetched before the first write"
    )


@pytest.mark.asyncio
async def test_a_superset_that_refuses_the_csrf_token_fails_loudly() -> None:
    """If the token cannot be had, say so once with its reason — never push blind."""

    class _NoCsrf(SupersetAPI):
        def handle(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v1/security/csrf_token/":
                self.calls.append({"method": request.method, "path": request.url.path, "csrf": None})
                return httpx.Response(403, json={"message": "Forbidden"})
            return super().handle(request)

    name = _name("nocsrf")
    api = _NoCsrf()
    await _seed([_postgres(name)])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            report = await SupersetSync(client=client, docker=FakeDocker()).run(session)
    finally:
        await _unseed()

    assert report["ok"] is False
    assert "CSRF token" in report["detail"]
    assert api.posts() == [], "no datasource may be attempted without a token"

# -- the container that has to load those drivers ----------------------------

def test_every_uri_scheme_has_a_package_that_provides_its_driver() -> None:
    """A scheme with no package is a source type Superset can never load."""
    drivers = {uri.split("+", 1)[1] for uri in BI_URI_DRIVERS.values()}

    assert drivers == set(BI_DRIVER_PACKAGES), (
        "BI_URI_DRIVERS and BI_DRIVER_PACKAGES disagree about which drivers exist"
    )
    assert drivers == set(BI_DRIVER_MODULES), (
        "every driver package needs the import name the deploy guards on"
    )


def test_the_superset_deploy_installs_the_drivers_its_container_lacks() -> None:
    """``apache/superset:4.1.1`` ships no psycopg2.

    Without this step every Postgres source failed Superset's own connection test with a
    bare "Connection failed, please check your connection settings" — the sync looked
    broken while the address translation was perfectly correct.
    """
    blueprint = get_blueprint("superset")
    install = " ".join(blueprint.command)

    missing = [package for package in bi_container_packages() if package not in install]
    assert not missing, f"Superset cannot query sources that need {missing}"
    assert "--no-deps" in install, "dependency copies would shadow the server's own libraries"
    assert "/app/superset_home/pylibs" in blueprint.env["PYTHONPATH"], (
        "the installed drivers must be importable by the server"
    )


def test_the_drivers_are_installed_before_the_superset_server_starts() -> None:
    """Superset caches a failed engine-spec import at boot, so ordering is the whole fix.

    Installing the drivers from ``post_start_commands`` runs *after* the entrypoint has
    already started gunicorn: the files land on the volume, and that server still answers
    every ``POST /api/v1/database/`` with 422 ``Could not load database driver:
    PostgresEngineSpec`` until it is restarted. A fresh install therefore has to have the
    drivers in place before the server process exists.
    """
    blueprint = get_blueprint("superset")
    command = " ".join(blueprint.command)

    assert "pip install" in command, "the container has to provision its own drivers"
    assert command.index("pip install") < command.index("run-server.sh"), (
        "the drivers must be installed before the gunicorn entrypoint is exec'd"
    )
    assert "superset db upgrade" in command, "schema migration must run before the server boots"
    assert "superset init" in command, "roles and permissions must be initialized before the server boots"
    assert command.index("superset db upgrade") < command.index("run-server.sh")
    assert command.index("superset init") < command.index("run-server.sh")
    assert not blueprint.post_start_commands, (
        "init sequence runs ahead of run-server.sh in command, leaving post_start_commands empty"
    )
    assert "exec /usr/bin/run-server.sh" in command, (
        "the image's own CMD must still be honoured after the install"
    )
    assert 'python -c "import ' in command, (
        "an already-provisioned volume must not pay for pip again on every restart"
    )
    assert blueprint.restart_after_init is True, (
        "Superset must trigger restart after initialization so Gunicorn loads installed drivers in clean memory"
    )



# -- what Superset already holds ---------------------------------------------
#
# The Dashboards page shows counts and recent dashboards. Those numbers must come from
# Superset itself, and a launcher that cannot read them must say so rather than render
# four zeros an operator would read as "nothing here".


class _InventoryAPI:
    """Superset's four list endpoints, as Superset 4.x answers them.

    ``page_size=1`` is the whole trick: Superset returns the exact ``count`` with a
    one-row page, so counting never depends on how many pages exist.
    """

    def __init__(self, counts=None, dashboards=None, fail=(), login_status=200, offline=False):
        self.counts = counts or {"dashboard": 2, "chart": 7, "dataset": 3, "database": 1}
        self.dashboards = (
            [
                {
                    "id": 9,
                    "dashboard_title": "Revenue",
                    "changed_on_utc": "2026-01-02T03:04:05",
                    "published": True,
                    # Superset's own url carries the base path it was started with.
                    "url": "/superset/dashboard/9/",
                },
                {
                    "id": 4,
                    "dashboard_title": "Ops",
                    "changed_on": {"__typename": "Datetime"},
                    "published": False,
                },
            ]
            if dashboards is None
            else dashboards
        )
        self.fail = set(fail)
        self.login_status = login_status
        self.offline = offline
        self.calls = []
        self.queries = []

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handle), base_url="http://superset.test"
        )

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.offline:
            raise httpx.ConnectError("connection refused", request=request)
        path = request.url.path
        self.calls.append(path)

        if path == "/api/v1/security/login":
            if self.login_status != 200:
                return httpx.Response(self.login_status, json={"message": "bad credentials"})
            return httpx.Response(200, json={"access_token": "tok"})
        if path == "/api/v1/security/csrf_token/":
            return httpx.Response(200, json={"result": "csrf-token"})

        resource = path.strip("/").rsplit("/", 1)[-1]
        if resource in self.fail:
            return httpx.Response(500, json={"message": "boom"})
        query = json.loads(request.url.params.get("q") or "{}")
        self.queries.append((resource, query))
        result = self.dashboards if resource == "dashboard" else []
        return httpx.Response(200, json={"count": self.counts[resource], "result": result})


def _queries_for(api: _InventoryAPI, resource: str):
    """Every ``q=`` this run sent for one resource: a count first, then the dashboard page."""
    return [query for name, query in api.queries if name == resource]


@pytest.mark.asyncio
async def test_the_inventory_counts_what_superset_holds() -> None:
    """Every number is Superset's own, asked for with a one-row page."""
    api = _InventoryAPI()
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is True
    assert inventory["counts"] == {"dashboards": 2, "charts": 7, "datasets": 3, "databases": 1}
    assert inventory["detail"] is None
    assert inventory["checked_at"], "the page says how fresh a number is"

    for resource in ("dashboard", "chart", "dataset", "database"):
        assert _queries_for(api, resource)[0]["page_size"] == 1, (
            f"{resource}s: counting must ask for one row, not a whole page"
        )


@pytest.mark.asyncio
async def test_the_recent_dashboards_are_rebuilt_on_supersets_own_origin() -> None:
    """Superset's stored ``url`` carries a base path; the launcher links to host:port.

    Keeping Superset's url would reintroduce the /superset prefix the page just stopped
    depending on, and nothing would notice until the click 404'd.
    """
    api = _InventoryAPI()
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["dashboards"] == [
        {
            "id": 9,
            "title": "Revenue",
            "path": "/superset/dashboard/9/",
            "changed_on": "2026-01-02T03:04:05",
            "published": True,
        },
        {
            "id": 4,
            "title": "Ops",
            "path": "/superset/dashboard/4/",
            # A dict timestamp is not a string the UI can format.
            "changed_on": None,
            "published": False,
        },
    ]
    recent = _queries_for(api, "dashboard")[-1]
    assert recent["order_column"] == "changed_on"
    assert recent["order_direction"] == "desc"
    assert recent["page_size"] == 5, "the card lists five, newest first"


@pytest.mark.asyncio
async def test_a_blank_superset_is_zero_and_available_not_unavailable() -> None:
    """An empty Superset is a fact; an unreachable one is a failure. The page says
    different things for each, so they must not look the same here."""
    api = _InventoryAPI(
        counts={"dashboard": 0, "chart": 0, "dataset": 0, "database": 0}, dashboards=[]
    )
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is True
    assert inventory["counts"] == {"dashboards": 0, "charts": 0, "datasets": 0, "databases": 0}
    assert inventory["dashboards"] == []


@pytest.mark.asyncio
async def test_an_unreachable_superset_reports_unavailable_instead_of_zero() -> None:
    api = _InventoryAPI(offline=True)
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is False
    assert inventory["counts"] == {}, "a number nobody could read must not be rendered as 0"
    assert "did not answer" in inventory["detail"]


@pytest.mark.asyncio
async def test_a_rejected_login_is_reported_without_inventing_numbers() -> None:
    api = _InventoryAPI(login_status=401)
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is False
    assert "login failed" in inventory["detail"]
    assert [p for p in api.calls if p != "/api/v1/security/login"] == []


@pytest.mark.asyncio
async def test_one_unlistable_resource_leaves_the_others_counted() -> None:
    """A missing permission on charts must not blank the whole card."""
    api = _InventoryAPI(fail={"chart"})
    await _seed([])
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is True
    assert inventory["counts"]["charts"] is None, "the one that failed is unknown, not zero"
    assert inventory["counts"]["dashboards"] == 2
    assert "charts" in inventory["detail"]


@pytest.mark.asyncio
async def test_the_inventory_is_never_read_when_superset_is_not_deployed() -> None:
    api = _InventoryAPI()
    await _seed([], deploy_superset=False)
    try:
        async with AsyncSessionLocal() as session, api.client() as client:
            inventory = await SupersetSync(client=client, docker=FakeDocker()).inventory(session)
    finally:
        await _unseed()

    assert inventory["available"] is False
    assert "not deployed" in inventory["detail"]
    assert api.calls == [], "nothing may be sent when there is no Superset to ask"

