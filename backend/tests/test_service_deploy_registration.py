"""Deploying a service registers its database — and binds it as the warehouse.

What this pins:

* a ready deploy owns one ``Connection`` row keyed by ``service_name``, created
  **writable**, whose vaulted password is the very string injected into the container as
  ``POSTGRES_PASSWORD`` — the thing that keeps the vault and the running database from
  drifting apart;
* the database just deployed becomes the chosen warehouse, so dbt publishes into it
  without anyone visiting Settings;
* a container that never answers registers nothing;
* an existing container is **never redeployed in place**: deploy answers 409 and the
  credentials the volume was initialized with stay untouched. Stop keeps the container
  (same id, same environment) and start brings it back with the stored password. That is
  the only way a service's credentials can be changed: destroy it, then deploy.

The warehouse binding is written *after* the deploy's transaction commits. Doing it
while the session still held the write transaction made SQLite answer "database is
locked", which aborted a deploy whose container was already healthy and rolled the
Connection row back with it. ``test_a_ready_deploy...`` fails on that regression.

There is no mocking here: ``Engine`` is a real object implementing exactly what the
route asks of ``docker_mgr``, swapped in and restored.
"""
from __future__ import annotations

import dataclasses

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select

from app.api import routes_services
from app.core.service_blueprints import get_blueprint
from app.core.vault import decrypt, encrypt
from app.core.warehouse_resolver import get_warehouse_connection_id, set_warehouse_connection_id
from app.database import AsyncSessionLocal
from app.main import app
from app.models.connection import Connection
from app.models.service_config import ServiceConfig

SERVICE = "postgres"
EXPECTED_NAME = f"{get_blueprint(SERVICE).display_name} (local)"


class _Container:
    """Only the attributes the deploy route reads off a container."""

    def __init__(self, container_id: str, status: str = "running"):
        self.id = container_id
        self.status = status
        self.attrs = {"HostConfig": {"Memory": 1024 * 1024 * 1024}}


class Engine:
    """A stand-in for ``docker_mgr``: a real object answering what deploy asks."""

    def __init__(self, ready: bool = True):
        self.ready = ready
        self.explode = False
        self.deployed = []
        self.probed = []
        self.existing = None
        self.stopped = []
        self.started = []

    def engine_status(self):
        return {"available": True, "error": None}

    def get_service_container(self, service_name):
        return self.existing

    def deploy_service(self, service_name, image, memory_mb, environment, ports, pull_progress_cb):
        self.deployed.append(
            {"service_name": service_name, "image": image, "environment": dict(environment), "ports": ports}
        )
        return _Container(f"fake-{service_name}")

    def stop_service(self, service_name):
        """Stops in place: the container object survives, exactly like `docker stop`."""
        self.stopped.append(service_name)
        if self.existing is not None:
            self.existing.status = "exited"

    def start_service(self, service_name):
        self.started.append(service_name)
        if self.existing is None:
            raise LookupError(f"No managed container for '{service_name}' to start. Deploy it first.")
        self.existing.status = "running"
        return self.existing

    def restart_service(self, service_name, timeout=10):
        self.started.append(service_name)
        if self.existing is None:
            raise LookupError(f"No managed container for '{service_name}' to restart. Deploy it first.")
        self.existing.status = "running"
        return self.existing

    def probe_readiness(self, service_name, host_port, username, database, password=None):
        if self.explode:
            raise RuntimeError("probe blew up after the container already existed")
        self.probed.append(
            {"host_port": host_port, "username": username, "database": database, "password": password}
        )
        return {"ready": self.ready, "detail": "/var/run/postgresql:5432 - accepting connections"}


class Disks:
    """Plenty of room: the pre-flight must not depend on the host's free space."""

    def get_disk_info(self, path):
        return {"free_gb": 100.0, "total_gb": 500.0}


@pytest.fixture
def no_binding():
    """Each test starts and ends with no warehouse chosen."""
    previous = get_warehouse_connection_id()
    set_warehouse_connection_id(None)
    try:
        yield
    finally:
        set_warehouse_connection_id(previous)


@pytest.fixture
def engine():
    """Replaces the module-level ``docker_mgr`` and the disk advisor for this test."""
    previous_engine = routes_services.docker_mgr
    previous_advisor = routes_services.advisor
    fake = Engine()
    routes_services.docker_mgr = fake
    routes_services.advisor = Disks()
    try:
        yield fake
    finally:
        routes_services.docker_mgr = previous_engine
        routes_services.advisor = previous_advisor


@pytest.fixture
def free_port():
    """A port nothing holds. The deploy route refuses a busy one."""
    previous = routes_services._is_host_port_free
    routes_services._is_host_port_free = lambda port: True
    try:
        yield
    finally:
        routes_services._is_host_port_free = previous


async def _forget_the_service() -> None:
    """Removes whatever an earlier run left behind, so each test starts from scratch."""
    async with AsyncSessionLocal() as db:
        await db.execute(delete(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
        await db.execute(delete(Connection).where(Connection.service_name == SERVICE))
        await db.commit()


async def _registered() -> Connection | None:
    async with AsyncSessionLocal() as db:
        return (
            await db.execute(select(Connection).where(Connection.service_name == SERVICE))
        ).scalar_one_or_none()


async def _deploy(port: int, **payload):
    """Deploys over the real route; ``payload`` carries whatever the modal would send."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(
            "/api/services/deploy",
            json={"service_name": SERVICE, "host_port": port, **payload},
        )


async def _call(action: str):
    """Hits a lifecycle route (start/stop) the way the console does."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(f"/api/services/{SERVICE}/{action}")


@pytest.mark.asyncio
async def test_a_ready_deploy_registers_a_writable_connection_and_binds_it(
    no_binding, engine, free_port
) -> None:
    await _forget_the_service()
    try:
        response = await _deploy(5993)
        assert response.status_code == 200, response.text
        body = response.json()

        assert body["ready"] is True
        assert body["registered_connection"] == EXPECTED_NAME
        password = body["credentials"]["password"]

        # The container got the password the vault holds — not a second copy of it.
        injected = engine.deployed[0]["environment"]
        assert injected["POSTGRES_PASSWORD"] == password
        assert injected["POSTGRES_USER"] == "postgres"
        assert injected["POSTGRES_DB"] == "obelisk"

        row = await _registered()
        assert row is not None, "a ready deploy owns a Connection row"
        assert row.name == EXPECTED_NAME
        assert row.source_type == "postgres"
        assert (row.host, row.port, row.database, row.username) == (
            "localhost", 5993, "obelisk", "postgres"
        )
        assert row.read_only is False, "Obelisk publishes marts into the database it deployed"
        assert decrypt(row.encrypted_credentials) == password

        # Deploying the warehouse is itself a choice.
        assert get_warehouse_connection_id() == row.id
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_a_deploy_adopts_the_row_already_linked_to_the_service(
    no_binding, engine, free_port
) -> None:
    """``service_name`` is the identity of a deployed database's Connection.

    A row that already names this service — added by hand, or left by an older install —
    is updated in place, never duplicated, and becomes the warehouse.
    """
    await _forget_the_service()
    seeded = Connection(
        name=EXPECTED_NAME,
        source_type="postgres",
        host="localhost",
        port=5993,
        database="stale",
        username="stale",
        encrypted_credentials=encrypt("stale"),
        read_only=True,
    )
    seeded.service_name = SERVICE
    async with AsyncSessionLocal() as db:
        db.add(seeded)
        await db.commit()
        seeded_id = seeded.id
    try:
        assert (await _deploy(5994)).status_code == 200

        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(select(Connection).where(Connection.service_name == SERVICE))
            ).scalars().all()
        assert len(rows) == 1, "service_name is the identity: a deploy updates, never duplicates"
        assert rows[0].id == seeded_id
        assert (rows[0].port, rows[0].username) == (5994, "postgres")
        assert rows[0].read_only is False
        assert get_warehouse_connection_id() == seeded_id
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_deploying_over_the_container_that_already_exists_is_refused(
    no_binding, engine, free_port
) -> None:
    """A deployed service is started or destroyed — never redeployed in place.

    Postgres applies POSTGRES_USER/PASSWORD/DB on first boot only, so a second deploy over
    the same volume creates nothing: it overwrites the container's environment and the
    vault with credentials the initialized cluster has never heard of. The refusal has to
    come before the container is touched, and leave the stored credentials alone.
    """
    await _forget_the_service()
    try:
        first = await _deploy(
            5998, username="analytics", password="Correct-Horse-9", database="warehouse"
        )
        assert first.status_code == 200, first.text

        # The container still exists, merely stopped — the state a stopped service is in,
        # and the state a crashed deploy leaves behind.
        engine.existing = _Container("fake-postgres", status="exited")

        second = await _deploy(
            5999, username="postgres", password="Second-Attempt-1", database="obelisk"
        )
        assert second.status_code == 409, second.text
        assert "already deployed" in second.json()["detail"]

        # Nothing was built, probed or registered a second time.
        assert len(engine.deployed) == 1
        assert len(engine.probed) == 1

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.config_json["username"] == "analytics"
        assert config.config_json["database"] == "warehouse"
        assert decrypt(config.config_json["encrypted_password"]) == "Correct-Horse-9"

        row = await _registered()
        assert row is not None
        assert (row.username, row.database) == ("analytics", "warehouse")
        assert decrypt(row.encrypted_credentials) == "Correct-Horse-9"
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_an_unready_deploy_registers_nothing(no_binding, engine, free_port) -> None:
    """The container is not answering yet, so there is no database to point a row at."""
    await _forget_the_service()
    blueprint = dataclasses.replace(get_blueprint(SERVICE), probe_timeout_seconds=0)
    previous_blueprint = routes_services.get_blueprint
    routes_services.get_blueprint = (
        lambda name: blueprint if name == SERVICE else previous_blueprint(name)
    )
    try:
        response = await _deploy(5995)
        assert response.status_code == 200, response.text
        body = response.json()

        assert body["ready"] is False
        assert body["registered_connection"] is None
        assert await _registered() is None
        assert get_warehouse_connection_id() is None

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.status == "starting", "the service row still records the attempt"
    finally:
        routes_services.get_blueprint = previous_blueprint
        await _forget_the_service()


@pytest.mark.asyncio
async def test_the_credentials_the_operator_typed_are_the_ones_registered(
    no_binding, engine, free_port
) -> None:
    """A deploy carrying credentials must never fall back to the blueprint defaults.

    The username/password/database typed into the deploy panel reach the container *and*
    the Connection row, and the vaulted password is the one the probe logs in with. A row
    that quietly advertised ``postgres``/``obelisk`` under a generated token would look
    deployed while pointing at a database that answers to something else.
    """
    await _forget_the_service()
    try:
        response = await _deploy(
            5996, username="analytics", password="Correct-Horse-9", database="warehouse"
        )
        assert response.status_code == 200, response.text
        body = response.json()

        injected = engine.deployed[0]["environment"]
        assert injected["POSTGRES_USER"] == "analytics"
        assert injected["POSTGRES_PASSWORD"] == "Correct-Horse-9"
        assert injected["POSTGRES_DB"] == "warehouse"

        # Readiness authenticated with the same password the vault is about to hold.
        assert engine.probed[-1]["password"] == "Correct-Horse-9"

        row = await _registered()
        assert row is not None
        assert (row.username, row.database) == ("analytics", "warehouse")
        assert decrypt(row.encrypted_credentials) == "Correct-Horse-9"
        assert body["credentials"]["username"] == "analytics"
        assert body["credentials"]["password"] == "Correct-Horse-9"

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.config_json["username"] == "analytics"
        assert config.config_json["database"] == "warehouse"
        assert decrypt(config.config_json["encrypted_password"]) == "Correct-Horse-9"
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_a_failure_after_the_container_exists_keeps_the_credentials(
    no_binding, engine, free_port
) -> None:
    """The volume already holds these credentials, so the row must not forget them.

    Postgres applies POSTGRES_USER/PASSWORD/DB on its first boot only. When a later step
    failed, the whole deploy transaction used to roll back — including the vaulted
    password the cluster had just been initialized with. The next deploy then found no
    stored password, minted a fresh token nothing can authenticate, and registered the
    blueprint defaults in place of what the operator typed.
    """
    await _forget_the_service()
    engine.explode = True
    try:
        with pytest.raises(RuntimeError):
            await _deploy(
                5997, username="analytics", password="Correct-Horse-9", database="warehouse"
            )

        assert engine.deployed[0]["environment"]["POSTGRES_PASSWORD"] == "Correct-Horse-9"

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.status == "starting", "the container is up; the row has to say so"
        assert config.config_json["username"] == "analytics"
        assert config.config_json["database"] == "warehouse"
        assert decrypt(config.config_json["encrypted_password"]) == "Correct-Horse-9"

        # A deploy that never proved the database works registers nothing.
        assert await _registered() is None
        assert get_warehouse_connection_id() is None
    finally:
        await _forget_the_service()



@pytest.mark.asyncio
async def test_stop_keeps_the_container_and_start_reuses_the_stored_password(
    no_binding, engine, free_port
) -> None:
    """Stop is not a rebuild, and start accepts no credentials.

    Stopping keeps the container (same id, same environment) and starting it hands the
    readiness probe the *stored* password — so there is no moment at which a new password
    could be minted for a volume that already has an old one.
    """
    await _forget_the_service()
    try:
        assert (
            await _deploy(
                5999, username="analytics", password="Correct-Horse-9", database="warehouse"
            )
        ).status_code == 200
        engine.existing = _Container("fake-postgres", status="running")
        probes_before = len(engine.probed)

        assert (await _call("stop")).status_code == 200
        assert engine.stopped == [SERVICE], "stop stops the container in place"

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.status == "stopped"
        assert config.container_id == "fake-postgres", "the container still exists, so its id stays"
        assert decrypt(config.config_json["encrypted_password"]) == "Correct-Horse-9"

        started = await _call("start")
        assert started.status_code == 200, started.text
        assert started.json()["ready"] is True
        assert engine.started == [SERVICE]
        assert len(engine.deployed) == 1, "start never creates a container"
        assert engine.probed[-1]["password"] == "Correct-Horse-9", "the stored password, not a new one"
        assert engine.probed[-1]["username"] == "analytics"
        assert len(engine.probed) == probes_before + 1

        async with AsyncSessionLocal() as db:
            config = (
                await db.execute(select(ServiceConfig).where(ServiceConfig.service_name == SERVICE))
            ).scalar_one()
        assert config.status == "ready"
        assert decrypt(config.config_json["encrypted_password"]) == "Correct-Horse-9"
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_starting_something_that_was_never_deployed_is_not_found(
    no_binding, engine, free_port
) -> None:
    """There is nothing to start until something has been deployed."""
    await _forget_the_service()
    engine.existing = None
    try:
        response = await _call("start")
        assert response.status_code == 404, response.text
        # The engine refused, and the route reported it as "nothing deployed here".
        assert "Deploy it first" in response.json()["detail"]
    finally:
        await _forget_the_service()


@pytest.mark.asyncio
async def test_restart_route_restarts_container_and_probes(
    no_binding, engine, free_port
) -> None:
    """Restarting invokes docker restart, preserves credentials and probes readiness."""
    await _forget_the_service()
    try:
        deploy_res = await _deploy(5993)
        assert deploy_res.status_code == 200
        engine.existing = _Container("fake-postgres")

        response = await _call("restart")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["ready"] is True
        assert body["service_name"] == SERVICE
        assert body["status"] == "ready"
        assert engine.started[-1] == SERVICE
    finally:
        await _forget_the_service()

