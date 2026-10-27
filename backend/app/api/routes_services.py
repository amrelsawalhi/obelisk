import asyncio
import os
import secrets
import socket
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.service_config import ServiceConfig
from app.models.connection import Connection
from app.core.resource_advisor import advisor
from app.core.docker_manager import docker_mgr
from app.core.service_blueprints import get_blueprint, blueprint_catalog_entries
from app.core.deploy_tracker import deploy_tracker
from app.core.process_manager import process_manager
from app.core.vault import encrypt, decrypt
from app.core.warehouse_resolver import (
    get_warehouse_connection_id,
    set_warehouse_connection_id,
)
from app.config import settings
from app.services.superset import SupersetSync

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

router = APIRouter(prefix="/api/services", tags=["services"])

SUPPORTED_SERVICES = [
    "postgres",
    "superset",
    "clickhouse",
    "redpanda",
    "gitea"
]


def _sync_summary(report: Dict[str, Any]) -> str:
    """'2 created, 1 skipped' — the one line a deploy log has room for."""
    parts = [f"{count} {action}" for action, count in report.get("counts", {}).items() if count]
    return ", ".join(parts) or "nothing to do"


def _origin_from_request(request: Request) -> str:
    """The browser-facing origin of this request, for links a human will click.

    ``public_url`` is the caller's explicit answer and wins. Without it, the host the
    browser actually used is the only correct one: the server's own idea of its hostname
    was ``localhost``, so a LAN deployment handed out links that pointed at whichever
    machine was reading the page.

    Behind a reverse proxy, ``Origin`` (or the ``X-Forwarded-*`` pair) is what the browser
    saw, so those win over ``Host``, which the proxy rewrites.
    """
    origin = (request.headers.get("origin") or "").strip()
    if origin and origin != "null":
        return origin

    proto = (
        request.headers.get("x-forwarded-proto") or request.url.scheme or "http"
    ).split(",")[0].strip()
    host = (
        request.headers.get("x-forwarded-host")
        or request.headers.get("host")
        or request.url.netloc
    )
    host = (host or "").split(",")[0].strip()
    return f"{proto}://{host}" if host else "http://localhost"


# Request / Response Schemas
class DeployLocalRequest(BaseModel):
    service_name: str = Field(..., description="Service identifier, e.g. postgres or superset")
    memory_mb: Optional[int] = Field(None, description="Optional explicit memory limit in MB")
    image: Optional[str] = Field(None, description="Optional custom image override (blueprint pins are used by default)")
    host_port: Optional[int] = Field(None, description="Optional host port override for the primary port")
    public_url: Optional[str] = Field(None, description="Browser origin used to reach Obelisk (e.g. http://localhost:5173 or http://server-ip); expands {public_url} in blueprint env")
    username: Optional[str] = Field(None, description="Custom admin/database username (e.g. for PostgreSQL or Superset)")
    password: Optional[str] = Field(None, description="Custom password (if omitted, auto-generates a secure token)")
    database: Optional[str] = Field(None, description="Custom database name (e.g. for PostgreSQL)")
    env_vars: Optional[Dict[str, str]] = Field(None, description="Additional environment variable overrides")

class ServiceStatusResponse(BaseModel):
    service_name: str
    deployment_type: str # retained for older clients; services are always local now
    status: str # running, stopped, error
    host: str
    port: int
    allocated_memory_mb: int
    container_id: Optional[str] = None
    live_docker_status: Optional[str] = None
    # A stopped container still exists (with its environment, credentials and data), so the
    # console offers "Start" for it and "Deploy" only when nothing has been deployed yet.
    container_exists: bool = False


class ServiceLifecycleResponse(BaseModel):
    """Result of starting or stopping a service that is already deployed."""
    service_name: str
    status: str
    ready: bool
    message: Optional[str] = None

class DeployedCredentials(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None


class DeployLocalResponse(ServiceStatusResponse):
    ready: bool = False
    message: Optional[str] = None
    credentials: Optional[DeployedCredentials] = None
    registered_connection: Optional[str] = None
    # Superset only: what happened to each registered connection when it was pushed in.
    synced_sources: Optional[List[Dict[str, Any]]] = None


class SupersetSyncResponse(BaseModel):
    ok: bool
    detail: str
    counts: Dict[str, int]
    sources: List[Dict[str, Any]]


class HostSpecsResponse(BaseModel):
    total_ram_bytes: int
    total_ram_gb: float
    available_ram_gb: float
    ram_percent_used: float
    cpu_count: int
    cpu_percent: float
    max_safe_allocation_bytes: int
    max_safe_allocation_gb: float

async def _register_service_connection(
    db: AsyncSession, bp, credentials, host_port: int
) -> Optional[Connection]:
    """Makes a ready service's database a first-class Connection.

    Linked by ``service_name`` — not by display name — so a redeploy updates the same row,
    and created **writable**: Obelisk publishes marts into the database it deployed, and a
    read-only row would make the write resolver refuse its own warehouse.
    """
    if not bp.provides or credentials is None:
        return None

    record = (
        await db.execute(select(Connection).where(Connection.service_name == bp.name))
    ).scalar_one_or_none()
    values = {
        "name": f"{bp.display_name} (local)",
        "source_type": bp.provides,
        "host": "localhost",
        "port": host_port,
        "database": credentials.database or "",
        "username": credentials.username or "",
        "encrypted_credentials": encrypt(credentials.password or ""),
        "options": {},
        "read_only": False,
    }
    if record:
        for key, value in values.items():
            setattr(record, key, value)
    else:
        record = Connection(service_name=bp.name, **values)
        db.add(record)
    await db.flush()
    return record


async def _forget_service_connection(db: AsyncSession, service_name: str) -> None:
    """A destroyed service's database is gone, so its Connection goes with it."""
    record = (
        await db.execute(select(Connection).where(Connection.service_name == service_name))
    ).scalar_one_or_none()
    if not record:
        return
    if get_warehouse_connection_id() == record.id:
        set_warehouse_connection_id(None)
    await db.delete(record)
    await db.commit()


@router.get("/advisor/specs", response_model=HostSpecsResponse)
async def get_hardware_specs():
    """Returns live hardware capacity probed via psutil."""
    # Fast async hygiene: Wrap synchronous psutil probe in asyncio.to_thread
    specs = await asyncio.to_thread(advisor.get_host_specs)
    return specs

@router.get("/advisor/recommend/{service_name}")
async def get_recommendation(service_name: str):
    """Returns dynamic recommended RAM/CPU limits for a specific service."""
    if service_name.lower() not in SUPPORTED_SERVICES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported service '{service_name}'. Supported: {SUPPORTED_SERVICES}"
        )
    rec = await asyncio.to_thread(advisor.recommend_allocation, service_name)
    return rec

def get_service_host_port(svc_name: str, live_container: Any = None, default_port: Optional[int] = None) -> int:
    """Extracts the published host port for the blueprint's primary container port.

    Multi-port services (e.g. Gitea web + SSH) publish several host ports; only the
    blueprint's primary container port is the canonical web/db port. Fallback order:
    primary binding -> first binding -> explicit/default port.
    """
    bp = get_blueprint(svc_name)
    default = default_port or (bp.primary_host_port if bp else 8000)
    if live_container:
        primary_cp = bp.primary_container_port if bp else None

        def _first_host_port(bindings: Dict[str, Any]) -> Optional[int]:
            if primary_cp and primary_cp in bindings:
                b = bindings[primary_cp]
                if b and b[0].get("HostPort"):
                    return int(b[0]["HostPort"])
            for c_port, b in bindings.items():
                if b and b[0].get("HostPort"):
                    return int(b[0]["HostPort"])
            return None

        try:
            # Check HostConfig PortBindings first
            port_bindings = live_container.attrs.get("HostConfig", {}).get("PortBindings", {}) or {}
            hp = _first_host_port(port_bindings)
            if hp:
                return hp
            # Then NetworkSettings Ports
            net_ports = live_container.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}
            hp = _first_host_port(net_ports)
            if hp:
                return hp
        except Exception:
            pass
    return default

def _is_host_port_free(host_port: int) -> bool:
    """Binds the port to check availability (deploy pre-flight)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", host_port))
            return True
    except OSError:
        return False


@router.get("/status", response_model=List[ServiceStatusResponse])
async def list_service_statuses(db: AsyncSession = Depends(get_db)):
    """Returns current deployment & runtime status for all catalog services."""
    # Fetch configured services from database
    stmt = select(ServiceConfig)
    result = await db.execute(stmt)
    configured_services = {s.service_name.lower(): s for s in result.scalars().all()}

    statuses: List[ServiceStatusResponse] = []
    
    # Query live Docker containers in background thread
    active_containers = await asyncio.to_thread(docker_mgr.list_managed_containers)
    container_map = {}
    for c in active_containers:
        svc = c.attrs.get("Config", {}).get("Labels", {}).get("service")
        if svc:
            container_map[svc.lower()] = c

    for svc_name in SUPPORTED_SERVICES:
        db_conf = configured_services.get(svc_name)
        live_c = container_map.get(svc_name)

        if db_conf:
            deployment_type = db_conf.deployment_type
            db_status = db_conf.status
            host = db_conf.host
            port = get_service_host_port(svc_name, live_c, db_conf.port)
            mem_mb = db_conf.allocated_memory_mb
            c_id = db_conf.container_id
        else:
            deployment_type = "local"
            db_status = "stopped"
            host = "localhost"
            port = get_service_host_port(svc_name, live_c)
            mem_mb = 0
            c_id = None

        live_status = None
        if live_c:
            live_status = live_c.status
            if deployment_type == "local":
                db_status = live_c.status
                c_id = live_c.id
                mem_bytes = live_c.attrs.get("HostConfig", {}).get("Memory", 0)
                if mem_bytes > 0:
                    mem_mb = mem_bytes // (1024 * 1024)

        if deployment_type == "local" and live_c is None:
            _st_bp = get_blueprint(svc_name)
            if _st_bp is not None and _st_bp.run_mode == "process":
                proc = await asyncio.to_thread(process_manager.status, svc_name)
                if proc["alive_pids"]:
                    db_status = "running"
                    live_status = "process"
                elif proc["pids"]:
                    db_status = "stopped"

        statuses.append(
            ServiceStatusResponse(
                service_name=svc_name,
                deployment_type=deployment_type,
                status=db_status,
                host=host,
                port=port,
                allocated_memory_mb=mem_mb,
                container_id=c_id,
                live_docker_status=live_status,
                container_exists=live_c is not None,
            )
        )

    return statuses


async def _deploy_process_mode(svc_name: str, bp, req, db) -> DeployLocalResponse:
    """
    Deploys a run_mode='process' service as host processes (no Docker, no image).

    Host-process services run from the backend venv, so deploys take
    seconds, need no registry/disk, and logs land in data/logs/<service>_*.log.
    """
    host_port = req.host_port or bp.primary_host_port

    deploy_tracker.update(svc_name, phase="pre-flight", detail="checking host port")
    current = await asyncio.to_thread(process_manager.status, svc_name)
    if not current["alive_pids"] and not _is_host_port_free(host_port):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
            detail=f"Host port {host_port} is already in use by another process. Choose a different host_port.")

    deploy_tracker.update(svc_name, phase="starting", detail="starting host processes")

    def _on_log(line: str) -> None:
        deploy_tracker.update(svc_name, log=line)

    start_info = await asyncio.to_thread(process_manager.start, svc_name, bp, _on_log)

    deploy_tracker.update(svc_name, phase="probing", detail="waiting for readiness probe")
    ready = False
    probe_detail = "no probe configured"
    deadline = time.time() + bp.probe_timeout_seconds
    while time.time() < deadline:
        probe = await asyncio.to_thread(docker_mgr.probe_readiness, svc_name, host_port)
        ready = probe["ready"]
        probe_detail = probe["detail"]
        if ready:
            break
        await asyncio.sleep(1.0)
    status_value = "ready" if ready else "starting"
    deploy_tracker.finish(svc_name, status_value, probe_detail)

    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    svc_record = (await db.execute(stmt)).scalar_one_or_none()
    config_json: Dict[str, Any] = dict(svc_record.config_json or {}) if svc_record else {}
    config_json.update({"run_mode": "process", "host_port": host_port,
                        "pids": start_info["pids"], "log_dir": start_info["log_dir"]})
    if not svc_record:
        svc_record = ServiceConfig(
            service_name=svc_name, deployment_type="local", status=status_value,
            container_id=None, host="localhost", port=host_port,
            allocated_memory_mb=0, config_json=config_json,
        )
        db.add(svc_record)
    else:
        svc_record.deployment_type = "local"
        svc_record.status = status_value
        svc_record.container_id = None
        svc_record.host = "localhost"
        svc_record.port = host_port
        svc_record.config_json = config_json
    await db.commit()
    await db.refresh(svc_record)

    message = None
    if not ready:
        message = (f"Processes started but the service is not ready yet (probe: {probe_detail}). "
                   f"Check logs at /api/services/{svc_name}/logs.")
    return DeployLocalResponse(
        service_name=svc_name, deployment_type="local", status=status_value,
        host="localhost", port=host_port, allocated_memory_mb=0, container_id=None,
        live_docker_status="process", ready=ready, message=message,
        credentials=None, registered_connection=None,
    )


@router.post("/deploy", response_model=DeployLocalResponse)
@router.post("/deploy-local", response_model=DeployLocalResponse)
async def deploy_local_service(
    req: DeployLocalRequest,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    1-Click Local Deploy (Services 2.0):
    pre-flight checks -> blueprint-driven deploy (pinned image, named data volume)
    -> per-deploy vaulted credentials -> readiness probing -> Connection auto-register.

    Deploy is first creation only. If a container already exists — running or stopped — the
    request is refused with 409, because a data volume that has already been initialized can
    never be re-keyed by a second deploy: postgres reads POSTGRES_USER/PASSWORD/DB on first
    boot and ignores them afterwards. Start the existing container, or destroy it, instead.

    The request stays synchronous (callers await the final result), but every phase
    is recorded in DeployTracker so the UI can poll GET /{name}/deploy-progress:
    pre-flight -> pulling (layer %) -> creating -> initializing -> probing -> ready.
    """
    svc_name = req.service_name.lower()
    bp = get_blueprint(svc_name)
    deploy_tracker.start(svc_name)
    try:
        if svc_name not in SUPPORTED_SERVICES or bp is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported service '{req.service_name}'. Supported: {SUPPORTED_SERVICES}")
        if not bp.deployable:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{bp.display_name} cannot be deployed locally: {bp.reason_not_deployable}")

        if bp.run_mode == "process":
            # A host-process service that is already alive must be stopped first: deploy is
            # what installs *and* starts it, so re-running it under a live process would
            # rebuild configuration behind a running service.
            proc_state = await asyncio.to_thread(process_manager.status, svc_name)
            if proc_state["alive_pids"]:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(f"{bp.display_name} is already running. Stop it and start it "
                            f"again to keep everything as it is, or destroy it to deploy fresh."),
                )
            # Host-process service: no Docker engine, image, or disk needed.
            return await _deploy_process_mode(svc_name, bp, req, db)

        # A deploy always creates a container, and that container's environment is what
        # initializes a database volume. If one already exists — running or merely stopped —
        # the operator must choose explicitly: start it again untouched, or destroy it
        # (which deletes its data) before deploying new credentials/configuration.
        # Deploying over a stopped container is how a fresh POSTGRES_PASSWORD used to be
        # written over a volume that had been initialized with a different one, leaving a
        # service nothing could log into.
        existing_container = await asyncio.to_thread(docker_mgr.get_service_container, svc_name)
        if existing_container is not None:
            state = existing_container.status
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"{bp.display_name} is already deployed (container {state}). "
                        f"Start it to bring it back exactly as it was — same credentials, same data — "
                        f"or destroy it, which deletes its data, before deploying again."),
            )

        # Pre-flight 1: Docker engine reachable
        deploy_tracker.update(svc_name, phase="pre-flight", detail="checking Docker engine")
        engine = await asyncio.to_thread(docker_mgr.engine_status)
        if not engine["available"]:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Docker engine is not reachable: {engine['error']}. Start Docker Desktop (or set DOCKER_HOST) and try again.")

        # Pre-flight 2: host port availability (nothing of ours holds it — see the guard above)
        host_port = req.host_port or bp.primary_host_port
        if not _is_host_port_free(host_port):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                detail=f"Host port {host_port} is already in use by another process. Choose a different host_port.")

        # Pre-flight 3: disk space for the image pull
        disk = await asyncio.to_thread(advisor.get_disk_info, str(_PROJECT_ROOT))
        if disk["free_gb"] < bp.image_size_gb:
            raise HTTPException(status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
                detail=f"Not enough disk space: {disk['free_gb']} GB free, image '{bp.image}' needs ~{bp.image_size_gb} GB.")

        # Load persisted config: redeploy reuses credentials so connections keep working
        stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
        svc_record = (await db.execute(stmt)).scalar_one_or_none()
        config_json: Dict[str, Any] = dict(svc_record.config_json or {}) if svc_record else {}

        # Credentials: user-supplied values win, otherwise the vaulted ones from a
        # previous deploy are reused, otherwise a fresh secure token is generated.
        env_override: Dict[str, str] = {}
        credentials = None
        username = (
            (req.username.strip() if req.username and req.username.strip() else None)
            or config_json.get("username")
            or bp.credential_username
            or "postgres"
        )
        database = (
            (req.database.strip() if req.database and req.database.strip() else None)
            or config_json.get("database")
            or bp.credential_database
            or "obelisk"
        )
        if "__GENERATED__" in bp.env.values():
            if req.password and req.password.strip():
                password = req.password.strip()
                config_json["encrypted_password"] = encrypt(password)
            elif config_json.get("encrypted_password"):
                password = decrypt(config_json["encrypted_password"])
            else:
                password = secrets.token_urlsafe(18)
                config_json["encrypted_password"] = encrypt(password)
            config_json["username"] = username
            config_json["database"] = database
            for env_key, env_val in bp.env.items():
                if env_val == "__GENERATED__":
                    env_override[env_key] = password

            if svc_name == "postgres":
                # The image creates its superuser/database from these on first boot
                env_override["POSTGRES_USER"] = username
                env_override["POSTGRES_PASSWORD"] = password
                env_override["POSTGRES_DB"] = database
            elif svc_name == "superset":
                # Consumed by the `superset fab create-admin` post-start command
                env_override["SUPERSET_ADMIN_USER"] = username
                env_override["SUPERSET_ADMIN_PASSWORD"] = password

            credentials = DeployedCredentials(username=username, password=password,
                                              database=database, host="localhost", port=host_port)

        # Expand gateway-relative placeholders (e.g. Gitea ROOT_URL) using the browser
        # origin that reached Obelisk — correct in local dev (:5173) and behind a
        # remote gateway alike, so subpath links resolve without hardcoding a host.
        # ``localhost`` used to be the fallback, which silently produced links that only
        # work on the server itself: on a LAN deployment every "Open UI" pointed at the
        # operator's own machine.
        public_url = (req.public_url or _origin_from_request(request)).rstrip("/")
        for env_key, env_val in bp.env.items():
            if env_val and "{public_url}" in env_val:
                env_override[env_key] = env_val.replace("{public_url}", public_url)

        # pass_env: inject live settings so container code resolves identically to host code
        for env_key in bp.pass_env:
            if env_key == "DATABASE_URL":
                data_mount = bp.bind_mounts.get("data", {}).get("bind", "/opt/obelisk/data")
                env_override["DATABASE_URL"] = f"sqlite+aiosqlite:///{data_mount}/obelisk.db"
            else:
                env_override[env_key] = getattr(settings, env_key, None) or os.environ.get(env_key, "")

        # Explicit user-supplied env overrides win over every blueprint default.
        if req.env_vars:
            env_override.update({str(k): str(v) for k, v in req.env_vars.items()})

        ports_override = None
        if req.host_port and req.host_port != bp.primary_host_port:
            ports_override = {bp.primary_container_port: ("0.0.0.0", host_port)}

        # Deploy with live pull progress (layer % streamed to the tracker)
        deploy_tracker.update(svc_name, phase="pulling", detail=f"pulling {req.image or bp.image}")
        layer_totals: Dict[str, int] = {}
        layer_currents: Dict[str, int] = {}

        def _on_pull(*args) -> None:
            # Dual-purpose progress callback:
            # - image pull events: (layer, status, current, total) -> aggregate layer %
            # - image build events: (line,) -> build log lines
            if len(args) == 4:
                layer, _pull_status, current, total = args
                if total > 0:
                    layer_totals[layer] = total
                    layer_currents[layer] = current
                    t_total = sum(layer_totals.values())
                    t_current = sum(layer_currents.values())
                    if t_total > 0:
                        deploy_tracker.update(svc_name, phase="pulling",
                                              pull_percent=(t_current / t_total) * 100.0)
            elif args:
                deploy_tracker.update(svc_name, log=str(args[0]))

        deploy_tracker.update(svc_name, phase="creating", detail="creating container")
        container = await asyncio.to_thread(
            docker_mgr.deploy_service,
            service_name=svc_name,
            image=req.image,
            memory_mb=req.memory_mb,
            environment=env_override,
            ports=ports_override,
            pull_progress_cb=_on_pull,
        )

        # The credentials live in the volume from this moment on: postgres reads
        # POSTGRES_USER/PASSWORD/DB only on its first boot, so whichever token was
        # decided above is now the cluster's superuser. Persist it *before* probing,
        # registration and post-start commands — every one of which can still raise, and
        # a rollback would keep the container (and the password baked into its volume)
        # while discarding config_json. The next deploy would then find no vaulted
        # password, mint a fresh token the running cluster cannot authenticate, and
        # quietly register the blueprint defaults instead of what the operator typed.
        config_json.update({"image": req.image or bp.image, "host_port": host_port,
                            "volume_name": bp.volume_name})
        if svc_record is None:
            svc_record = ServiceConfig(
                service_name=svc_name, deployment_type="local", status="starting",
                container_id=container.id, host="localhost", port=host_port,
                allocated_memory_mb=0, config_json=config_json,
            )
            db.add(svc_record)
        else:
            svc_record.deployment_type = "local"
            svc_record.status = "starting"
            svc_record.container_id = container.id
            svc_record.host = "localhost"
            svc_record.port = host_port
            svc_record.config_json = config_json
        await db.commit()

        # Post-start initialization (e.g. superset db upgrade / admin creation)
        if bp.post_start_commands:
            deploy_tracker.update(svc_name, phase="initializing",
                                  detail=f"running {len(bp.post_start_commands)} init commands")

            def _on_cmd(cmd: str, output: str) -> None:
                deploy_tracker.update(svc_name, log=f"$ {cmd}\n{output}")

            await asyncio.to_thread(
                docker_mgr.run_post_start_commands, svc_name, bp.post_start_commands, _on_cmd,
            )

        # Readiness probing: "ready" only when the service actually answers
        deploy_tracker.update(svc_name, phase="probing", detail="waiting for readiness probe")
        ready = False
        probe_detail = "no probe configured"
        deadline = time.time() + bp.probe_timeout_seconds
        while time.time() < deadline:
            probe = await asyncio.to_thread(
                docker_mgr.probe_readiness, svc_name, host_port, username, database,
                password=credentials.password if credentials else None,
            )
            ready = probe["ready"]
            probe_detail = probe["detail"]
            if ready:
                break
            await asyncio.sleep(2.0)
        status_value = "ready" if ready else "starting"

        # Second commit records the probe's verdict; for a database service the
        # Connection created below lands in the same transaction.
        svc_record.status = status_value
        svc_record.deployment_type = "local"
        svc_record.container_id = container.id

        # A service that provides a database owns a Connection row the moment it is
        # ready: one rule, applied by what the blueprint declares rather than by a flag
        # somebody has to remember to set.
        registered_name = None
        conn_record = None
        if ready and bp.provides and credentials:
            conn_record = await _register_service_connection(db, bp, credentials, host_port)
            registered_name = conn_record.name if conn_record else None

        await db.commit()
        await db.refresh(svc_record)

        # Deploying the warehouse is itself a choice, so adopt it unless the operator
        # already picked a different one. Written *after* the commit on purpose: the
        # setting is stored through its own SQLite connection, and while this session
        # still holds the deploy's write transaction that write returns "database is
        # locked" — which used to abort a deploy whose container was already healthy and
        # roll the Connection row back with it.
        if (
            conn_record is not None
            and bp.provides == "postgres"
            and get_warehouse_connection_id() is None
        ):
            try:
                set_warehouse_connection_id(conn_record.id)
            except Exception as exc:
                # Binding is a convenience; it never fails a running service.
                probe_detail = f"{probe_detail}; warehouse binding failed: {exc}"

        # Superset is only useful once it knows where the data is: push every registered
        # connection the moment the webserver answers, so the first login shows sources
        # instead of an empty dropdown. It runs after the config commit because the syncer
        # reads the vaulted admin password from that row, and a failure is a warning, not
        # a failed deploy — the container is already healthy.
        synced_sources = None
        if ready and svc_name == "superset":
            deploy_tracker.update(svc_name, phase="syncing-sources",
                                  detail="pushing registered connections into Superset")
            try:
                report = await SupersetSync().run(db)
            except Exception as exc:
                probe_detail = f"{probe_detail}; source sync failed: {exc}"
            else:
                synced_sources = report["sources"]
                probe_detail = f"{probe_detail}; sources: {_sync_summary(report)}"

        deploy_tracker.finish(svc_name, status_value, probe_detail)

        mem_bytes = container.attrs.get("HostConfig", {}).get("Memory", 0)
        mem_mb = mem_bytes // (1024 * 1024) if mem_bytes > 0 else (req.memory_mb or 1024)

        message = None
        if not ready:
            message = (f"Container started but the service is not ready yet (probe: {probe_detail}). "
                       f"It may still be initializing — check logs at /api/services/{svc_name}/logs.")

        return DeployLocalResponse(
            service_name=svc_name, deployment_type="local", status=status_value,
            host=svc_record.host, port=svc_record.port, allocated_memory_mb=mem_mb,
            container_id=container.id, live_docker_status=container.status,
            ready=ready, message=message, credentials=credentials, registered_connection=registered_name,
            synced_sources=synced_sources,
        )
    except HTTPException as he:
        deploy_tracker.fail(svc_name, str(he.detail))
        raise
    except Exception as e:
        deploy_tracker.fail(svc_name, str(e))
        raise


@router.get("/superset/sources", response_model=SupersetSyncResponse)
async def preview_superset_sources(db: AsyncSession = Depends(get_db)):
    """Dry run of the sync: the URIs it would push, with passwords masked.

    Declared before the ``/{service_name}/...`` routes so ``superset`` here is this
    endpoint's literal path and not a service name.
    """
    return await SupersetSync().plan(db)


@router.post("/superset/sync", response_model=SupersetSyncResponse)
async def sync_superset_sources(db: AsyncSession = Depends(get_db)):
    """Pushes every registered connection into Superset, upserting by connection name."""
    report = await SupersetSync().run(db)
    if not report["ok"]:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=report["detail"])
    return report


@router.post("/{service_name}/stop")
async def stop_service(
    service_name: str,
    db: AsyncSession = Depends(get_db)
):
    """Stops a service **in place**, keeping everything it has.

    A container is stopped, not removed: its environment — hence the credentials its data
    volume was initialized with — its published ports, its configuration and its data all
    survive, so /start brings back the very same service. Nothing about it can be changed
    while it sits stopped; use /destroy to remove it and its data, then deploy fresh.
    """
    svc_name = service_name.lower()
    _bp = get_blueprint(svc_name)
    try:
        if _bp is not None and _bp.run_mode == "process":
            await asyncio.to_thread(process_manager.stop, svc_name)
        else:
            await asyncio.to_thread(docker_mgr.stop_service, svc_name)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop service '{svc_name}': {str(e)}"
        )

    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    result = await db.execute(stmt)
    svc_record = result.scalar_one_or_none()
    if svc_record:
        # Only the status flips: credentials, ports and container_id stay exactly as they
        # were, because the container they describe is still there.
        svc_record.status = "stopped"
        await db.commit()

    return {"service_name": svc_name, "status": "stopped"}


@router.post("/{service_name}/start", response_model=ServiceLifecycleResponse)
async def start_service(service_name: str, db: AsyncSession = Depends(get_db)):
    """Starts a stopped service with everything it already had.

    A container start is ``docker start``: same container, same environment (so the same
    credentials), same data volume. Nothing is rebuilt, and this endpoint accepts no
    credentials or configuration — those are decided once, at deploy time. Readiness is
    re-probed with the *stored* credentials, so a container whose volume disagrees with its
    environment is reported instead of being quietly declared healthy.
    """
    svc_name = service_name.lower()
    bp = get_blueprint(svc_name)
    if svc_name not in SUPPORTED_SERVICES or bp is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Unknown service '{service_name}'.")

    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    svc_record = (await db.execute(stmt)).scalar_one_or_none()
    config_json: Dict[str, Any] = dict(svc_record.config_json or {}) if svc_record else {}

    if bp.run_mode == "process":
        try:
            await asyncio.to_thread(process_manager.start, svc_name, bp)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Failed to start '{svc_name}': {e}")
        if svc_record:
            svc_record.status = "running"
            await db.commit()
        return ServiceLifecycleResponse(
            service_name=svc_name, status="running", ready=True,
            message=f"{bp.display_name} started",
        )

    try:
        container = await asyncio.to_thread(docker_mgr.start_service, svc_name)
    except LookupError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Failed to start '{svc_name}': {e}")

    host_port = get_service_host_port(svc_name, container, config_json.get("host_port"))
    username = config_json.get("username")
    database = config_json.get("database")
    password: Optional[str] = None
    if config_json.get("encrypted_password"):
        try:
            password = decrypt(config_json["encrypted_password"])
        except Exception:
            # A vault we cannot open is not a reason to refuse to start the container;
            # the probe simply cannot authenticate and will say so.
            password = None

    ready = False
    probe_detail = "no probe configured"
    deadline = time.time() + bp.probe_timeout_seconds
    while time.time() < deadline:
        probe = await asyncio.to_thread(
            docker_mgr.probe_readiness, svc_name, host_port, username, database,
            password=password,
        )
        ready = probe["ready"]
        probe_detail = probe["detail"]
        if ready:
            break
        await asyncio.sleep(2.0)

    if svc_record:
        svc_record.status = "ready" if ready else "starting"
        svc_record.deployment_type = "local"
        svc_record.container_id = container.id
        svc_record.port = host_port
        await db.commit()

    return ServiceLifecycleResponse(
        service_name=svc_name, status="ready" if ready else "starting",
        ready=ready, message=probe_detail,
    )


@router.get("/engine")
async def docker_engine_status():
    """Live Docker engine reachability + disk space (drives the UI engine-offline state)."""
    engine = await asyncio.to_thread(docker_mgr.engine_status)
    disk = await asyncio.to_thread(advisor.get_disk_info, str(_PROJECT_ROOT))
    return {"docker": engine, "disk": disk}


@router.get("/catalog")
async def service_catalog():
    """Blueprint-driven deployable service metadata for the pre-deploy configuration form."""
    return blueprint_catalog_entries()


@router.get("/{service_name}/deploy-progress")
async def get_deploy_progress(service_name: str):
    """Live deploy progress (polled by the UI while POST /deploy is in flight)."""
    svc_name = service_name.lower()
    job = deploy_tracker.get(svc_name)
    if job is None:
        return {"service_name": svc_name, "phase": "idle", "active": False,
                "detail": "", "pull_percent": 0.0, "logs": [], "error": None}
    return job.to_dict()


@router.get("/{service_name}/logs")
async def get_service_logs(service_name: str, tail: int = 200):
    """Returns the last log lines of a managed local container."""
    svc_name = service_name.lower()
    if svc_name not in SUPPORTED_SERVICES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown service '{service_name}'.")
    _log_bp = get_blueprint(svc_name)
    try:
        if _log_bp is not None and _log_bp.run_mode == "process":
            logs = await asyncio.to_thread(process_manager.logs, svc_name, tail)
        else:
            logs = await asyncio.to_thread(docker_mgr.get_container_logs, svc_name, tail)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Failed to fetch logs: {e}")
    return {"service_name": svc_name, "logs": logs}


@router.post("/{service_name}/destroy")
async def destroy_service(service_name: str, db: AsyncSession = Depends(get_db)):
    """
    Stops the managed container AND permanently deletes its data volume.
    This destroys all persisted service data — the frontend must confirm explicitly.
    """
    svc_name = service_name.lower()
    if svc_name not in SUPPORTED_SERVICES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown service '{service_name}'.")

    _proc_bp = get_blueprint(svc_name)
    if _proc_bp is not None and _proc_bp.run_mode == "process":
        status_info = await asyncio.to_thread(process_manager.status, svc_name)
        home_dir = _PROJECT_ROOT / "data" / f"{svc_name}_home"
        if not status_info["alive_pids"] and not home_dir.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No managed local process or data for '{svc_name}'.")
        try:
            await asyncio.to_thread(process_manager.destroy, svc_name)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to destroy '{svc_name}': {e}")
        stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
        svc_record = (await db.execute(stmt)).scalar_one_or_none()
        if svc_record:
            # Destroy means "as if never deployed": the home directory, the credentials it
            # was configured with and its settings all go, so the next deploy starts clean.
            await db.delete(svc_record)
            await db.commit()
        await _forget_service_connection(db, svc_name)
        return {"service_name": svc_name, "status": "destroyed", "data_removed": True}

    # Destroy is also the cleanup path for orphaned volumes (after stop or a crash),
    # so a running container is not required — but *something* must exist to destroy.
    container = await asyncio.to_thread(docker_mgr.get_service_container, svc_name)
    bp = get_blueprint(svc_name)
    has_volume = False
    if bp and bp.volume_name and docker_mgr.client:
        try:
            await asyncio.to_thread(docker_mgr.client.volumes.get, bp.volume_name)
            has_volume = True
        except Exception:
            pass
    if not container and not has_volume:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No managed local container or data volume for '{svc_name}'.")
    try:
        await asyncio.to_thread(docker_mgr.destroy_service, svc_name)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to destroy '{svc_name}': {e}")

    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    svc_record = (await db.execute(stmt)).scalar_one_or_none()
    if svc_record:
        # Destroy means "as if never deployed": the container, its data volume, its
        # vaulted credentials and its settings all go, so the next deploy starts clean and
        # may choose new credentials freely.
        await db.delete(svc_record)
        await db.commit()

    await _forget_service_connection(db, svc_name)
    return {"service_name": svc_name, "status": "destroyed", "data_removed": True}
