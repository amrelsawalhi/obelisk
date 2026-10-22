import asyncio
import os
import secrets
import socket
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
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
from app.config import settings
from app.engines.dagster_client import dagster_client

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

router = APIRouter(prefix="/api/services", tags=["services"])

SUPPORTED_SERVICES = [
    "postgres",
    "dagster",
    "superset",
    "clickhouse",
    "redpanda",
    "gitea"
]

# Request / Response Schemas
class DeployLocalRequest(BaseModel):
    service_name: str = Field(..., description="Service identifier, e.g. postgres, dagster")
    memory_mb: Optional[int] = Field(None, description="Optional explicit memory limit in MB")
    image: Optional[str] = Field(None, description="Optional custom image override (blueprint pins are used by default)")
    host_port: Optional[int] = Field(None, description="Optional host port override for the primary port")
    public_url: Optional[str] = Field(None, description="Browser origin used to reach Obelisk (e.g. http://localhost:5173 or http://server-ip); expands {public_url} in blueprint env")
    username: Optional[str] = Field(None, description="Custom admin/database username (e.g. for PostgreSQL or Superset)")
    password: Optional[str] = Field(None, description="Custom password (if omitted, auto-generates a secure token)")
    database: Optional[str] = Field(None, description="Custom database name (e.g. for PostgreSQL)")
    env_vars: Optional[Dict[str, str]] = Field(None, description="Additional environment variable overrides")

class ConnectRemoteRequest(BaseModel):
    service_name: str = Field(..., description="Service identifier, e.g. postgres, clickhouse")
    host: str = Field(..., description="Remote host or IP")
    port: int = Field(..., description="Remote port")
    config_json: Optional[Dict[str, Any]] = Field(None, description="Additional connection params")
    username: Optional[str] = Field(None, description="Remote database username")
    password: Optional[str] = Field(None, description="Remote database password (stored encrypted)")
    database: Optional[str] = Field(None, description="Remote database name")
    sslmode: Optional[str] = Field(None, description="SSL mode, e.g. 'disable' or 'require'")

class ServiceStatusResponse(BaseModel):
    service_name: str
    deployment_type: str # local or remote
    status: str # running, stopped, connected, error
    host: str
    port: int
    allocated_memory_mb: int
    container_id: Optional[str] = None
    live_docker_status: Optional[str] = None

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


class HostSpecsResponse(BaseModel):
    total_ram_bytes: int
    total_ram_gb: float
    available_ram_gb: float
    ram_percent_used: float
    cpu_count: int
    cpu_percent: float
    max_safe_allocation_bytes: int
    max_safe_allocation_gb: float

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
                proc = process_manager.status(svc_name)
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
            )
        )

    return statuses


async def _warmup_dagster_workspace(svc_name: str) -> None:
    """Best-effort pre-warm of the Dagster code-server after a process deploy.

    The first GraphQL query after a cold deploy pays the code-location load
    cost; running discover_workspace() here amortizes that into the deploy so
    the first Pipelines panel open is instant. Failures are intentionally
    silent — the panel already renders an empty state when Dagster is down.
    """
    if svc_name != "dagster":
        return
    try:
        await asyncio.to_thread(dagster_client.discover_workspace)
    except Exception:
        pass


async def _deploy_process_mode(svc_name: str, bp, req, db) -> DeployLocalResponse:
    """
    Deploys a run_mode='process' service as host processes (no Docker, no image).

    Used by Dagster: webserver + daemon run from the backend venv, so deploys take
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

    if ready and svc_name == "dagster":
        # Amortize Dagster's code-location cold load into the deploy, so the
        # first open of the Pipelines panel doesn't pay it. Fire-and-forget:
        # the deploy response returns immediately.
        asyncio.create_task(_warmup_dagster_workspace(svc_name))

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
    db: AsyncSession = Depends(get_db)
):
    """
    1-Click Local Deploy (Services 2.0):
    pre-flight checks -> blueprint-driven deploy (pinned image, named data volume)
    -> per-deploy vaulted credentials -> readiness probing -> Connection auto-register.

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
            # Host-process service (e.g. Dagster): no Docker engine, image, or disk needed.
            return await _deploy_process_mode(svc_name, bp, req, db)

        # Pre-flight 1: Docker engine reachable
        deploy_tracker.update(svc_name, phase="pre-flight", detail="checking Docker engine")
        engine = await asyncio.to_thread(docker_mgr.engine_status)
        if not engine["available"]:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Docker engine is not reachable: {engine['error']}. Start Docker Desktop (or set DOCKER_HOST) and try again.")

        # Pre-flight 2: host port availability (skipped when our own container holds it)
        host_port = req.host_port or bp.primary_host_port
        own_container = await asyncio.to_thread(docker_mgr.get_service_container, svc_name)
        if not own_container and not _is_host_port_free(host_port):
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
        public_url = (req.public_url or "http://localhost").rstrip("/")
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
            )
            ready = probe["ready"]
            probe_detail = probe["detail"]
            if ready:
                break
            await asyncio.sleep(2.0)
        status_value = "ready" if ready else "starting"
        deploy_tracker.finish(svc_name, status_value, probe_detail)
        config_json.update({"image": req.image or bp.image, "host_port": host_port, "volume_name": bp.volume_name})
        if not svc_record:
            svc_record = ServiceConfig(
                service_name=svc_name, deployment_type="local", status=status_value,
                container_id=container.id, host="localhost", port=host_port,
                allocated_memory_mb=0, config_json=config_json,
            )
            db.add(svc_record)
        else:
            svc_record.deployment_type = "local"
            svc_record.status = status_value
            svc_record.container_id = container.id
            svc_record.host = "localhost"
            svc_record.port = host_port
            svc_record.config_json = config_json

        # Auto-register as a catalog Connection so Query Studio / pipelines can use it immediately
        registered_name = None
        if ready and bp.register_connection and bp.connection_type and credentials:
            conn_name = f"Obelisk {bp.display_name} (local)"
            existing_conn = (await db.execute(
                select(Connection).where(Connection.name == conn_name))).scalar_one_or_none()
            enc_pw = encrypt(credentials.password or "")
            if existing_conn:
                existing_conn.host = "localhost"
                existing_conn.port = host_port
                existing_conn.database = credentials.database or ""
                existing_conn.username = credentials.username or ""
                existing_conn.encrypted_credentials = enc_pw
            else:
                db.add(Connection(name=conn_name, source_type=bp.connection_type, host="localhost",
                                  port=host_port, database=credentials.database or "",
                                  username=credentials.username or "", encrypted_credentials=enc_pw))
            registered_name = conn_name

        await db.commit()
        await db.refresh(svc_record)

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
        )
    except HTTPException as he:
        deploy_tracker.fail(svc_name, str(he.detail))
        raise
    except Exception as e:
        deploy_tracker.fail(svc_name, str(e))
        raise


@router.post("/connect", response_model=ServiceStatusResponse)
async def connect_remote_service(
    req: ConnectRemoteRequest,
    db: AsyncSession = Depends(get_db)
):
    """Connect existing remote enterprise database or service."""
    svc_name = req.service_name.lower()
    if svc_name not in SUPPORTED_SERVICES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported service '{svc_name}'."
        )

    # Stop local container if running
    try:
        await asyncio.to_thread(docker_mgr.stop_service, svc_name)
    except Exception:
        pass

    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    result = await db.execute(stmt)
    svc_record = result.scalar_one_or_none()

    # Persist the connection parameters so Query Studio, dbt and the SDK all
    # resolve the same credentials. The password is vaulted, never plaintext.
    config_json: Dict[str, Any] = dict(svc_record.config_json or {}) if svc_record else {}
    config_json.update(dict(req.config_json or {}))
    if req.username and req.username.strip():
        config_json["username"] = req.username.strip()
    if req.database and req.database.strip():
        config_json["database"] = req.database.strip()
    if req.sslmode and req.sslmode.strip():
        config_json["sslmode"] = req.sslmode.strip()
    if req.password and req.password.strip():
        config_json["encrypted_password"] = encrypt(req.password.strip())

    if not svc_record:
        svc_record = ServiceConfig(
            service_name=svc_name,
            deployment_type="remote",
            status="connected",
            container_id=None,
            host=req.host,
            port=req.port,
            allocated_memory_mb=0,
            config_json=config_json,
        )
        db.add(svc_record)
    else:
        svc_record.deployment_type = "remote"
        svc_record.status = "connected"
        svc_record.container_id = None
        svc_record.host = req.host
        svc_record.port = req.port
        svc_record.allocated_memory_mb = 0
        svc_record.config_json = config_json

    # A remote PostgreSQL becomes a first-class catalog Connection so it appears in
    # the Data Catalog and Query Studio straight away.
    if svc_name == "postgres" and config_json.get("username") and config_json.get("encrypted_password"):
        conn_name = "Obelisk PostgreSQL (remote)"
        remote_conn = (await db.execute(
            select(Connection).where(Connection.name == conn_name))).scalar_one_or_none()
        conn_values: Dict[str, Any] = {
            "source_type": "postgres",
            "host": req.host,
            "port": req.port,
            "database": config_json.get("database") or "",
            "username": config_json["username"],
            "encrypted_credentials": config_json["encrypted_password"],
            "options": {"sslmode": config_json.get("sslmode", "disable")},
        }
        if remote_conn:
            for key, value in conn_values.items():
                setattr(remote_conn, key, value)
        else:
            db.add(Connection(name=conn_name, **conn_values))

    await db.commit()
    await db.refresh(svc_record)

    return ServiceStatusResponse(
        service_name=svc_name,
        deployment_type="remote",
        status="connected",
        host=req.host,
        port=req.port,
        allocated_memory_mb=0,
        container_id=None,
        live_docker_status=None,
    )

@router.post("/{service_name}/stop")
async def stop_service(
    service_name: str,
    db: AsyncSession = Depends(get_db)
):
    """Stops and removes the local container managed by Obelisk. Data volumes are PRESERVED (use /destroy to remove data)."""
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
        svc_record.status = "stopped"
        svc_record.container_id = None
        await db.commit()

    return {"service_name": svc_name, "status": "stopped"}


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
            svc_record.status = "stopped"
            svc_record.container_id = None
            await db.commit()
        return {"service_name": svc_name, "status": "destroyed", "data_removed": True}

    # Destroy is also the cleanup path for orphaned volumes (after stop or a crash),
    # so a running container is not required — but *something* must exist to destroy.
    container = await asyncio.to_thread(docker_mgr.get_service_container, svc_name)
    bp = get_blueprint(svc_name)
    has_volume = False
    if bp and bp.volume_name and docker_mgr.client:
        try:
            docker_mgr.client.volumes.get(bp.volume_name)
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
        svc_record.status = "stopped"
        svc_record.container_id = None
        await db.commit()

    return {"service_name": svc_name, "status": "destroyed", "data_removed": True}
