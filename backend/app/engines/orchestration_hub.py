"""
Orchestration Hub Engine:
Pluggable registry for orchestration engines (Obelisk Built-in Cron, Dagster, Apache Airflow).
Probes active services and provides unified metadata for the frontend Pipelines tab.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.service_config import ServiceConfig
import asyncio
from app.core.service_blueprints import get_blueprint


class OrchestrationEngineInfo(BaseModel):
    id: str  # "cron", "dagster", "airflow", "prefect"
    name: str  # "Obelisk Built-in Cron", "Dagster Engine", "Apache Airflow"
    icon: str  # "clock", "dagster", "airflow"
    status: str  # "running", "offline", "not_configured"
    web_url: Optional[str] = None
    description: str
    is_builtin: bool = False
    can_deploy_local: bool = False
    active_jobs_count: int = 0


class OrchestrationHub:
    """Manages discovery, status probing, and routing for all orchestration engines."""

    async def get_engines(self) -> List[OrchestrationEngineInfo]:
        engines: List[OrchestrationEngineInfo] = []

        # 1. Built-in Cron Engine (Always available, in-process, zero-RAM overhead)
        from app.models.cron_job import CronJob
        active_cron_count = 0
        try:
            async with AsyncSessionLocal() as session:
                res = await session.execute(
                    select(CronJob).where(CronJob.is_enabled == True)
                )
                active_cron_count = len(res.scalars().all())
        except Exception:
            active_cron_count = 0

        engines.append(
            OrchestrationEngineInfo(
                id="cron",
                name="Obelisk Built-in Cron",
                icon="clock",
                status="running",
                web_url=None,
                description="Lightweight in-process async scheduler (Zero-container RAM overhead)",
                is_builtin=True,
                active_jobs_count=active_cron_count,
            )
        )

        # 2. Query configured services in SQLite (Dagster, Airflow)
        services_map: Dict[str, ServiceConfig] = {}
        try:
            async with AsyncSessionLocal() as session:
                res = await session.execute(select(ServiceConfig))
                for svc in res.scalars().all():
                    services_map[svc.service_name.lower()] = svc
        except Exception:
            pass

        # 3. Dagster Orchestrator (local: live GraphQL probe; remote: DB status)
        from app.engines.dagster_client import dagster_client as dagster_service
        dagster_bp = get_blueprint("dagster")
        dagster_svc = services_map.get("dagster")
        dagster_status = "offline"
        dagster_url = None
        if dagster_svc:
            if dagster_svc.deployment_type == "remote":
                dagster_status = (dagster_svc.status or "connected").lower()
                dagster_url = f"http://{dagster_svc.host}:{dagster_svc.port or 3000}"
            else:
                probe = await asyncio.to_thread(dagster_service.service_status)
                dagster_status = "running" if probe["available"] else "stopped"
                dagster_url = f"http://localhost:{dagster_svc.port or (dagster_bp.primary_host_port if dagster_bp else 3000)}"

        engines.append(
            OrchestrationEngineInfo(
                id="dagster",
                name="Dagster Engine",
                icon="dagster",
                status=dagster_status,
                web_url=dagster_url,
                description="Asset orchestrator: webserver + daemon running Obelisk's real workspace",
                is_builtin=False,
                can_deploy_local=bool(dagster_bp and dagster_bp.deployable),
            )
        )

        # 4. Apache Airflow Orchestrator
        airflow_svc = services_map.get("airflow")
        airflow_status = "offline"
        airflow_url = "/airflow"
        if airflow_svc:
            airflow_status = airflow_svc.status.lower()
            if airflow_svc.deployment_type == "remote":
                airflow_url = f"http://{airflow_svc.host}:{airflow_svc.port or 8080}"

        engines.append(
            OrchestrationEngineInfo(
                id="airflow",
                name="Apache Airflow",
                icon="airflow",
                status=airflow_status,
                web_url=airflow_url,
                description="Enterprise workflow platform with task DAGs and operators",
                is_builtin=False,
            )
        )

        return engines


orchestration_hub = OrchestrationHub()
