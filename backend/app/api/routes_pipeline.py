"""dbt-facing endpoints that used to live beside the schedulers.

These routes are the dbt Studio's data plane: the asset graph it draws, the scoped
run it triggers, and the model source it edits. They keep the ``/api/pipelines``
prefix because the frontend and several callers already speak it, but they are
registered *before* the orchestrator router so the static paths win over
``/api/pipelines/{pipeline_id}``.

Everything scheduler-related that used to live here — cron jobs, Dagster engines,
custom DAG files, pipeline definitions — is gone: the pipeline orchestrator owns
scheduling now.
"""

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.warehouse_resolver import resolve_warehouse_postgres_params
from app.engines.dbt_runner import dbt_runner

router = APIRouter(prefix="/api/pipelines", tags=["dbt Pipelines"])


class TriggerPipelineRequest(BaseModel):
    postgres_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Target PostgreSQL warehouse connection parameters. Defaults to active environment.",
    )
    table_names: Optional[List[str]] = Field(
        default=None,
        description="Optional list of table stems to copy from the landing layer and transform.",
    )
    selector: Optional[str] = Field(
        default=None,
        description="Optional dbt model selector pattern (e.g. 'stg_orders+', 'models/staging', '*')",
    )
    pipeline_name: Optional[str] = Field(
        default=None, description="Optional human-readable pipeline name"
    )


class SaveModelRequest(BaseModel):
    layer: str = Field(..., description="Target layer path, e.g. 'raw' or 'marts.sales'")
    name: str = Field(..., description="Model name (e.g. stg_orders or fct_revenue)")
    sql: str = Field(..., description="SQL Jinja model code")


@router.get("/assets")
async def get_pipeline_asset_graph(selector: Optional[str] = None):
    """Returns the dbt lineage graph, optionally scoped to a selector."""
    return await asyncio.to_thread(dbt_runner.get_asset_graph, selector=selector)


@router.post("/run")
async def trigger_pipeline_run(req: Optional[TriggerPipelineRequest] = None):
    """Copies landing Parquet into the warehouse, runs dbt, tests it, updates the catalog.

    This is the dbt Studio's "run this selection now" action. Scheduled and
    dependency-aware execution is the pipeline orchestrator's job.
    """
    pg_params = req.postgres_params if req and req.postgres_params else None
    table_names = req.table_names if req else None
    selector = req.selector if req else None
    pipeline_name = req.pipeline_name if req else None

    if not pg_params:
        pg_params = await asyncio.to_thread(resolve_warehouse_postgres_params)

    try:
        return await asyncio.to_thread(
            dbt_runner.run_pipeline,
            postgres_params=pg_params,
            table_names=table_names,
            selector=selector,
            pipeline_name=pipeline_name,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


@router.get("/runs")
async def get_pipeline_runs():
    """Returns historical dbt run metadata and execution logs."""
    runs = await asyncio.to_thread(dbt_runner.get_runs_history)
    return {"runs": runs, "total": len(runs)}


@router.get("/model/{layer}/{model_name}")
async def get_pipeline_model_code(layer: str, model_name: str):
    """Returns the raw SQL/Jinja source code of a dbt model."""
    code = await asyncio.to_thread(dbt_runner.get_model_code, layer=layer, name=model_name)
    if code is None:
        raise HTTPException(
            status_code=404,
            detail=f"Model '{model_name}' not found in layer '{layer}'.",
        )
    return {"name": model_name, "layer": layer, "sql": code}


@router.post("/model")
async def save_or_create_pipeline_model(req: SaveModelRequest):
    """Saves or creates a dbt model in git_repo/dbt_project/models/."""
    try:
        return await asyncio.to_thread(
            dbt_runner.save_model_code, layer=req.layer, name=req.name, sql=req.sql
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save model: {str(e)}")
