"""
Pipelines API Routes: Exposes Medallion lineage graph, pipeline run triggers, and live logs.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.pipeline_definition import PipelineDefinition
from app.core.warehouse_resolver import resolve_warehouse_postgres_params
from app.engines.dbt_runner import dbt_runner

router = APIRouter(prefix="/api/pipelines", tags=["Pipelines"])

logger = logging.getLogger(__name__)


class TriggerPipelineRequest(BaseModel):
    postgres_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Target PostgreSQL warehouse connection parameters. Defaults to active environment."
    )
    table_names: Optional[List[str]] = Field(
        default=None,
        description="Optional list of Bronze Parquet table stems to sync and transform."
    )
    selector: Optional[str] = Field(
        default=None,
        description="Optional dbt model selector pattern (e.g. 'stg_orders+', 'models/staging', '*')"
    )
    pipeline_name: Optional[str] = Field(
        default=None,
        description="Optional human-readable pipeline name"
    )


class CreatePipelineRequest(BaseModel):
    name: str = Field(..., description="Unique human-readable pipeline name")
    description: Optional[str] = Field(default="", description="Pipeline description")
    selector: str = Field(default="*", description="dbt model selector pattern")
    schedule: Optional[str] = Field(default=None, description="Optional cron schedule expression")


@router.get("")
async def list_pipelines():
    """Returns all registered pipeline definitions (read-only). Default pipelines are seeded once at startup via app.database.seed_defaults."""
    async with AsyncSessionLocal() as session:
        stmt = select(PipelineDefinition).order_by(PipelineDefinition.created_at.asc())
        res = await session.execute(stmt)
        pipelines = res.scalars().all()

        return [
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "selector": p.selector,
                "schedule": p.schedule,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in pipelines
        ]


@router.post("")
async def create_pipeline(req: CreatePipelineRequest):
    """Creates a new pipeline definition with a specific model selector scope."""
    async with AsyncSessionLocal() as session:
        stmt = select(PipelineDefinition).where(PipelineDefinition.name == req.name.strip())
        res = await session.execute(stmt)
        if res.scalars().first():
            raise HTTPException(status_code=400, detail=f"Pipeline with name '{req.name}' already exists")

        new_p = PipelineDefinition(
            name=req.name.strip(),
            description=req.description.strip() if req.description else "",
            selector=req.selector.strip() if req.selector else "*",
            schedule=req.schedule.strip() if req.schedule else None,
        )
        session.add(new_p)
        await session.commit()
        await session.refresh(new_p)
        return {
            "id": new_p.id,
            "name": new_p.name,
            "description": new_p.description,
            "selector": new_p.selector,
            "schedule": new_p.schedule,
            "created_at": new_p.created_at.isoformat() if new_p.created_at else None,
        }


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """Deletes a custom pipeline definition."""
    async with AsyncSessionLocal() as session:
        stmt = select(PipelineDefinition).where(PipelineDefinition.id == pipeline_id)
        res = await session.execute(stmt)
        p = res.scalars().first()
        if not p:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        await session.delete(p)
        await session.commit()
        return {"status": "deleted", "id": pipeline_id}


@router.get("/assets")
async def get_medallion_asset_graph(selector: Optional[str] = None):
    """Returns the Medallion lineage graph, optionally tagged or scoped to a selector."""
    return dbt_runner.get_asset_graph(selector=selector)


@router.post("/run")
async def trigger_pipeline_run(req: Optional[TriggerPipelineRequest] = None):
    """
    Triggers an end-to-end Medallion transformation pipeline:
    1. Stages Bronze Parquet files to PostgreSQL 'bronze' schema via DuckDB C++ COPY.
    2. Runs dbt models matching the specified selector (or all models).
    3. Runs dbt tests matching the selector.
    4. Updates SQLite assets catalog.
    """
    pg_params = req.postgres_params if req and req.postgres_params else None
    table_names = req.table_names if req else None
    selector = req.selector if req else None
    pipeline_name = req.pipeline_name if req else None

    # Auto-resolve the warehouse from the deployed service, the data catalog,
    # environment variables or a reachable local test container.
    if not pg_params:
        pg_params = await asyncio.to_thread(resolve_warehouse_postgres_params)

    try:
        # dbt run/test are long blocking subprocess invocations — execute in a worker
        # thread so the event loop (UI, terminal, telemetry) stays responsive.
        res = await asyncio.to_thread(
            dbt_runner.run_medallion_pipeline,
            postgres_params=pg_params,
            table_names=table_names,
            selector=selector,
            pipeline_name=pipeline_name,
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


class SaveModelRequest(BaseModel):
    layer: str = Field(..., description="Target layer: 'silver' (staging) or 'gold' (marts)")
    name: str = Field(..., description="Model name (e.g. stg_orders or fct_revenue)")
    sql: str = Field(..., description="SQL Jinja model code")


@router.get("/model/{layer}/{model_name}")
async def get_pipeline_model_code(layer: str, model_name: str):
    """Returns the raw SQL/Jinja source code of a dbt model."""
    code = dbt_runner.get_model_code(layer=layer, name=model_name)
    if code is None:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found in layer '{layer}'.")
    return {"name": model_name, "layer": layer, "sql": code}


@router.post("/model")
async def save_or_create_pipeline_model(req: SaveModelRequest):
    """Saves or creates a dbt model in git_repo/dbt_project/models/."""
    try:
        res = dbt_runner.save_model_code(layer=req.layer, name=req.name, sql=req.sql)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save model: {str(e)}")


@router.get("/runs")
async def get_pipeline_runs():
    """Returns historical pipeline run metadata and execution logs."""
    runs = dbt_runner.get_runs_history()
    return {"runs": runs, "total": len(runs)}


# --- DAG Auto-Discovery & Management Endpoints (git_repo/dags/) ---

class CreateDagRequest(BaseModel):
    name: str = Field(..., description="Unique alphanumeric DAG asset identifier (e.g. daily_sales_kpi)")
    template: str = Field(default="kpi", description="Template: 'kpi', 'notebook', or 'blank'")
    deps: Optional[List[str]] = Field(default=["fct_orders_enriched"], description="Upstream asset dependency names")
    description: Optional[str] = Field(default="", description="Description of the asset logic")
    notebook_name: Optional[str] = Field(default="Quickstart_Analytics.py", description="Target notebook if template is 'notebook'")


class UpdateDagCodeRequest(BaseModel):
    code: str = Field(..., description="Python source code for the DAG asset")


@router.get("/dags")
async def list_custom_dags():
    """Returns all discovered DAG files in git_repo/dags/."""
    from app.engines.dag_loader import dag_loader
    return dag_loader.list_dag_files()


@router.post("/dags")
async def create_custom_dag(req: CreateDagRequest):
    """Creates a new DAG asset file in git_repo/dags/ from a boilerplate template."""
    from app.engines.dag_loader import dag_loader
    clean_name = req.name.strip().lower().replace(" ", "_").replace("-", "_").replace(".py", "")
    if not clean_name:
        raise HTTPException(status_code=400, detail="DAG name cannot be empty.")

    deps_repr = repr(req.deps if req.deps else ["fct_orders_enriched"])
    desc = req.description or f"Custom pipeline asset '{clean_name}'"
    nb_name = req.notebook_name or "Quickstart_Analytics.py"
    if not nb_name.endswith(".py"):
        nb_name = f"{nb_name}.py"

    if req.template == "notebook":
        code = f'''"""
Obelisk Custom DAG Asset: {clean_name} (Notebook Runner)
"""

import sys
import subprocess
from pathlib import Path
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue


@asset(
    name="{clean_name}",
    deps={deps_repr},
    group_name="notebooks",
    description="{desc}",
)
def {clean_name}(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Triggering Marimo notebook {nb_name}...")
    root_dir = Path(__file__).resolve().parent.parent.parent
    nb_path = root_dir / "git_repo" / "notebooks" / "{nb_name}"

    if not nb_path.exists():
        context.log.warning(f"Notebook {{nb_path}} not found.")
        return MaterializeResult(metadata={{"status": MetadataValue.text("NOT_FOUND")}})

    result = subprocess.run([sys.executable, str(nb_path)], cwd=str(root_dir), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Notebook execution failed: {{result.stderr}}")

    context.log.info("Notebook executed successfully.")
    return MaterializeResult(metadata={{"notebook": MetadataValue.text("{nb_name}"), "status": MetadataValue.text("SUCCESS")}})
'''
    elif req.template == "blank":
        code = f'''"""
Obelisk Custom DAG Asset: {clean_name}
"""

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import obelisk


@asset(
    name="{clean_name}",
    deps={deps_repr},
    group_name="custom",
    description="{desc}",
)
def {clean_name}(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Executing {clean_name}...")
    # Add custom processing logic here using obelisk.table() or obelisk.query()
    return MaterializeResult(metadata={{"status": MetadataValue.text("SUCCESS")}})
'''
    else:
        # Default: KPI Mart template
        code = f'''"""
Obelisk Custom DAG Asset: {clean_name} (KPI Summary)
"""

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import obelisk


@asset(
    name="{clean_name}",
    deps={deps_repr},
    group_name="analytics",
    description="{desc}",
)
def {clean_name}(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Executing {clean_name}...")
    batch_size = int(obelisk.var("BATCH_SIZE", default="1000"))
    env = obelisk.var("ENV", default="production")

    # Read upstream Gold marts using obelisk SDK
    try:
        df = obelisk.query("SELECT * FROM gold.fct_orders_enriched").to_polars()
        row_count = len(df)
    except Exception:
        row_count = 0

    context.log.info(f"Loaded {{row_count}} records from gold warehouse.")
    return MaterializeResult(
        metadata={{
            "rows_processed": MetadataValue.int(row_count),
            "environment": MetadataValue.text(env),
            "status": MetadataValue.text("SUCCESS"),
        }}
    )
'''

    file_path = dag_loader.save_dag_code(clean_name, code)
    return {
        "status": "created",
        "name": clean_name,
        "filename": f"{clean_name}.py",
        "path": file_path,
        "code": code,
    }


@router.get("/dags/{dag_name}/code")
async def get_custom_dag_code(dag_name: str):
    """Returns the raw Python code of a DAG file."""
    from app.engines.dag_loader import dag_loader
    code = dag_loader.get_dag_code(dag_name)
    if code is None:
        raise HTTPException(status_code=404, detail=f"DAG '{dag_name}' not found.")
    return {"name": dag_name, "code": code}


@router.put("/dags/{dag_name}/code")
async def update_custom_dag_code(dag_name: str, req: UpdateDagCodeRequest):
    """Updates the Python code of a DAG file."""
    from app.engines.dag_loader import dag_loader
    file_path = dag_loader.save_dag_code(dag_name, req.code)
    return {"status": "saved", "name": dag_name, "path": file_path}


@router.delete("/dags/{dag_name}")
async def delete_custom_dag(dag_name: str):
    """Deletes a DAG file from git_repo/dags/."""
    from app.engines.dag_loader import dag_loader
    deleted = dag_loader.delete_dag_file(dag_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"DAG '{dag_name}' not found.")
    return {"status": "deleted", "name": dag_name}


# ---------------------------------------------------------------------------
# Pluggable Orchestrator Hub & Built-in Cron Scheduler Endpoints
# ---------------------------------------------------------------------------

class CreateCronJobRequest(BaseModel):
    name: str = Field(..., description="Unique human-readable name for the job")
    description: Optional[str] = Field(default="", description="Job description")
    target_type: str = Field(default="dbt", description="'dbt', 'script', or 'notebook'")
    target: str = Field(..., description="dbt selector, script filename, or notebook filename")
    cron_expression: str = Field(default="0 2 * * *", description="Standard 5-field cron expression")


@router.get("/engines")
async def list_orchestration_engines():
    """Returns available orchestration engines (Built-in Cron, Dagster) with live status."""
    from app.engines.orchestration_hub import orchestration_hub
    engines = await orchestration_hub.get_engines()
    # Preferred engine for new pipelines: Dagster when it is actually reachable,
    # otherwise the always-available in-process Cron scheduler.
    dagster_running = any(e.id == "dagster" and e.status == "running" for e in engines)
    return {
        "engines": [e.model_dump() for e in engines],
        "default_engine": "dagster" if dagster_running else "cron",
    }


@router.get("/cron/targets")
async def list_cron_targets():
    """Returns available runnable targets (scripts, notebooks, dbt models) for the job creation picker."""
    from app.engines.cron_engine import cron_engine
    return cron_engine.list_available_targets()


@router.get("/cron/jobs")
async def list_cron_jobs():
    """Returns all user-created scheduled cron jobs."""
    from app.models.cron_job import CronJob
    async with AsyncSessionLocal() as session:
        stmt = select(CronJob).order_by(CronJob.created_at.desc())
        res = await session.execute(stmt)
        jobs = res.scalars().all()
        return [
            {
                "id": j.id,
                "name": j.name,
                "description": j.description,
                "target_type": j.target_type,
                "target": j.target,
                "cron_expression": j.cron_expression,
                "is_enabled": j.is_enabled,
                "last_run_at": j.last_run_at.isoformat() if j.last_run_at else None,
                "last_status": j.last_status,
                "last_duration_seconds": j.last_duration_seconds,
                "last_logs": j.last_logs,
                "next_run_at": j.next_run_at.isoformat() if j.next_run_at else None,
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in jobs
        ]


@router.post("/cron/jobs")
async def create_cron_job(req: CreateCronJobRequest):
    """Creates a new scheduled cron job in SQLite."""
    from app.models.cron_job import CronJob
    from app.engines.cron_engine import cron_engine

    clean_name = req.name.strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Job name cannot be empty.")

    next_run = cron_engine.compute_next_run(req.cron_expression)
    if not next_run:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cron expression '{req.cron_expression}'. Expected standard 5-part format (e.g. '0 2 * * *')."
        )

    async with AsyncSessionLocal() as session:
        # Check duplicate name
        existing = await session.execute(select(CronJob).where(CronJob.name == clean_name))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail=f"A scheduled job with name '{clean_name}' already exists.")

        new_job = CronJob(
            name=clean_name,
            description=req.description.strip() if req.description else "",
            target_type=req.target_type,
            target=req.target.strip(),
            cron_expression=req.cron_expression.strip(),
            is_enabled=True,
            last_status="idle",
            next_run_at=next_run,
        )
        session.add(new_job)
        await session.commit()
        await session.refresh(new_job)

        return {
            "id": new_job.id,
            "name": new_job.name,
            "description": new_job.description,
            "target_type": new_job.target_type,
            "target": new_job.target,
            "cron_expression": new_job.cron_expression,
            "is_enabled": new_job.is_enabled,
            "last_status": new_job.last_status,
            "next_run_at": new_job.next_run_at.isoformat() if new_job.next_run_at else None,
            "created_at": new_job.created_at.isoformat(),
        }


@router.post("/cron/jobs/{job_id}/run")
async def run_cron_job_now(job_id: str):
    """Triggers immediate on-demand execution of a scheduled cron job."""
    from app.engines.cron_engine import cron_engine
    try:
        res = await cron_engine.run_job(job_id)
        return res
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job execution failed: {str(e)}")


@router.patch("/cron/jobs/{job_id}/toggle")
async def toggle_cron_job(job_id: str):
    """Toggles a cron job between enabled and disabled."""
    from app.models.cron_job import CronJob
    from app.engines.cron_engine import cron_engine

    async with AsyncSessionLocal() as session:
        stmt = select(CronJob).where(CronJob.id == job_id)
        res = await session.execute(stmt)
        job = res.scalar_one_or_none()
        if not job:
            raise HTTPException(status_code=404, detail=f"CronJob '{job_id}' not found.")

        job.is_enabled = not job.is_enabled
        if job.is_enabled:
            job.next_run_at = cron_engine.compute_next_run(job.cron_expression)
        else:
            job.next_run_at = None

        await session.commit()
        return {
            "id": job.id,
            "is_enabled": job.is_enabled,
            "next_run_at": job.next_run_at.isoformat() if job.next_run_at else None,
        }


@router.delete("/cron/jobs/{job_id}")
async def delete_cron_job(job_id: str):
    """Deletes a cron job from SQLite."""
    from app.models.cron_job import CronJob

    async with AsyncSessionLocal() as session:
        stmt = select(CronJob).where(CronJob.id == job_id)
        res = await session.execute(stmt)
        job = res.scalar_one_or_none()
        if not job:
            raise HTTPException(status_code=404, detail=f"CronJob '{job_id}' not found.")

        await session.delete(job)
        await session.commit()
        return {"status": "deleted", "id": job_id}

