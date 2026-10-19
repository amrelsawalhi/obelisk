import asyncio
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.service_config import ServiceConfig
from app.core.resource_advisor import advisor
from app.core.docker_manager import docker_mgr

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
    image: Optional[str] = Field(None, description="Optional custom image override")

class ConnectRemoteRequest(BaseModel):
    service_name: str = Field(..., description="Service identifier, e.g. postgres, clickhouse")
    host: str = Field(..., description="Remote host or IP")
    port: int = Field(..., description="Remote port")
    config_json: Optional[Dict[str, Any]] = Field(None, description="Additional connection params")

class ServiceStatusResponse(BaseModel):
    service_name: str
    deployment_type: str # local or remote
    status: str # running, stopped, connected, error
    host: str
    port: int
    allocated_memory_mb: int
    container_id: Optional[str] = None
    live_docker_status: Optional[str] = None

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

DEFAULT_SERVICE_PORTS: Dict[str, int] = {
    "postgres": 5432,
    "dagster": 3000,
    "superset": 8088,
    "clickhouse": 8123,
    "redpanda": 9092,
    "gitea": 3001,
}

def get_service_host_port(svc_name: str, live_container: Any = None, default_port: Optional[int] = None) -> int:
    """Extracts the actual published host port from live container bindings or falls back to catalog defaults."""
    default = default_port or DEFAULT_SERVICE_PORTS.get(svc_name.lower(), 8000)
    if live_container:
        try:
            # Check HostConfig PortBindings first
            port_bindings = live_container.attrs.get("HostConfig", {}).get("PortBindings", {}) or {}
            for c_port, bindings in port_bindings.items():
                if bindings and len(bindings) > 0:
                    hp = bindings[0].get("HostPort")
                    if hp:
                        return int(hp)
            # Check NetworkSettings Ports
            net_ports = live_container.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}
            for c_port, bindings in net_ports.items():
                if bindings and len(bindings) > 0:
                    hp = bindings[0].get("HostPort")
                    if hp:
                        return int(hp)
        except Exception:
            pass
    return default

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
            port = get_service_host_port(svc_name, live_c, DEFAULT_SERVICE_PORTS.get(svc_name, 8000))
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

@router.post("/deploy", response_model=ServiceStatusResponse)
@router.post("/deploy-local", response_model=ServiceStatusResponse)
async def deploy_local_service(
    req: DeployLocalRequest, 
    db: AsyncSession = Depends(get_db)
):
    """1-Click Local Deploy on localhost Docker with dynamic hardware sizing."""
    svc_name = req.service_name.lower()
    if svc_name not in SUPPORTED_SERVICES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported service '{svc_name}'."
        )

    try:
        # Fast async hygiene: Docker container creation in background thread
        container = await asyncio.to_thread(
            docker_mgr.deploy_service,
            service_name=svc_name,
            image=req.image,
            memory_mb=req.memory_mb
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deploy container on localhost Docker: {str(e)}"
        )

    # Calculate allocated memory
    mem_bytes = container.attrs.get("HostConfig", {}).get("Memory", 0)
    mem_mb = mem_bytes // (1024 * 1024) if mem_bytes > 0 else (req.memory_mb or 1024)

    # Persist in SQLite
    stmt = select(ServiceConfig).where(ServiceConfig.service_name == svc_name)
    result = await db.execute(stmt)
    svc_record = result.scalar_one_or_none()

    svc_port = get_service_host_port(svc_name, container, DEFAULT_SERVICE_PORTS.get(svc_name, 8000))

    if not svc_record:
        svc_record = ServiceConfig(
            service_name=svc_name,
            deployment_type="local",
            status=container.status,
            container_id=container.id,
            host="localhost",
            port=svc_port,
            allocated_memory_mb=mem_mb,
        )
        db.add(svc_record)
    else:
        svc_record.deployment_type = "local"
        svc_record.status = container.status
        svc_record.container_id = container.id
        svc_record.port = svc_port
        svc_record.allocated_memory_mb = mem_mb

    await db.commit()
    await db.refresh(svc_record)

    return ServiceStatusResponse(
        service_name=svc_name,
        deployment_type="local",
        status=container.status,
        host=svc_record.host,
        port=svc_record.port,
        allocated_memory_mb=mem_mb,
        container_id=container.id,
        live_docker_status=container.status,
    )

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

    if not svc_record:
        svc_record = ServiceConfig(
            service_name=svc_name,
            deployment_type="remote",
            status="connected",
            container_id=None,
            host=req.host,
            port=req.port,
            allocated_memory_mb=0,
            config_json=req.config_json,
        )
        db.add(svc_record)
    else:
        svc_record.deployment_type = "remote"
        svc_record.status = "connected"
        svc_record.container_id = None
        svc_record.host = req.host
        svc_record.port = req.port
        svc_record.allocated_memory_mb = 0
        svc_record.config_json = req.config_json

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
    """Stops and removes local container managed by Obelisk."""
    svc_name = service_name.lower()
    try:
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
