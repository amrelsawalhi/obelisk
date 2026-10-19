import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.connection import Connection
from app.core.vault import decrypt
from app.engines.dagster_client import dagster_client

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

import json
import uuid
from datetime import datetime, timezone
from app.models.cron_job import CronJob
from app.engines.cron_engine import cron_engine

class IngestTriggerRequest(BaseModel):
    connection_id: str = Field(..., description="ID of the registered external connection")
    entity: str = Field(..., description="Table or collection name to extract")
    mode: str = Field("snapshot", description="Extraction mode: 'snapshot' or 'incremental'")
    watermark_column: Optional[str] = Field(None, description="Watermark column for incremental mode")
    target_name: Optional[str] = Field(None, description="Target parquet filename override")
    target_folder: Optional[str] = Field(None, description="Target lakehouse folder/directory")
    limit: Optional[int] = Field(None, description="Optional row limit for sample ingestion")
    write_strategy: Optional[str] = Field("append", description="'append' or 'upsert'")
    primary_key: Optional[str] = Field(None, description="Primary key for upsert deduplication")
    schedule: Optional[str] = Field(None, description="Optional cron expression for recurring schedule")
    schedule_engine: Optional[str] = Field("cron", description="'cron' for internal CronEngine or 'dagster' for Dagster Schedule")

class IngestTriggerResponse(BaseModel):
    success: bool
    run_id: str
    asset_name: str
    metadata: Dict[str, Any]
    logs: List[str]

class BatchIngestRequest(BaseModel):
    connection_id: str = Field(..., description="ID of the registered external connection")
    schema_name: Optional[str] = Field(None, description="Optional schema filter or label")
    tables: List[str] = Field(..., description="List of table or collection names to extract")
    mode: str = Field("snapshot", description="Extraction mode: 'snapshot' or 'incremental'")
    watermark_column: Optional[str] = Field(None, description="Watermark column for incremental mode")
    write_strategy: str = Field("append", description="'append' or 'upsert'")
    primary_key: Optional[str] = Field(None, description="Primary key for upsert deduplication")
    target_folder: Optional[str] = Field(None, description="Target lakehouse folder/directory")
    limit: Optional[int] = Field(None, description="Optional row limit for sample ingestion")
    schedule: Optional[str] = Field(None, description="Optional cron expression for recurring schedule")
    schedule_engine: Optional[str] = Field("cron", description="'cron' for internal CronEngine or 'dagster' for Dagster Schedule")

class BatchIngestResponse(BaseModel):
    success: bool
    total_tables: int
    succeeded: int
    failed: int
    results: List[Dict[str, Any]]
    errors: List[Dict[str, str]]
    logs: List[str]

@router.post("/trigger", response_model=IngestTriggerResponse)
async def trigger_ingestion(
    req: IngestTriggerRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Triggers a Dagster asset materialization to stream data from source into Bronze Parquet.
    Executes in background threadpool to maintain FastAPI async loop responsiveness.
    Optionally registers a recurring schedule via Obelisk Built-in Cron or Dagster.
    """
    stmt = select(Connection).where(Connection.id == req.connection_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{req.connection_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    connection_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username": conn.username,
        "password": plaintext_pw,
        **(conn.options or {}),
    }

    # If scheduling is requested, save the recurring schedule
    schedule_log = None
    if req.schedule and req.schedule.strip():
        cron_expr = req.schedule.strip()
        clean_target = (req.target_name or (req.entity.split(".", 1)[-1] if "." in req.entity else req.entity)).replace(".", "_")
        job_name = f"Ingest: {conn.name}.{req.entity}"

        if req.schedule_engine == "dagster":
            # Generate Dagster DAG file in git_repo/dags/
            dags_dir = Path(__file__).resolve().parent.parent.parent.parent / "git_repo" / "dags"
            dags_dir.mkdir(parents=True, exist_ok=True)
            dag_file = dags_dir / f"ingest_{clean_target}.py"
            dag_code = f'''# Auto-generated Dagster Ingestion Asset & Schedule
import dagster as dg
from dagster_workspace.repository import create_ingest_asset

connection_params = {json.dumps(connection_params, indent=4)}

ingest_{clean_target}_asset = create_ingest_asset(
    asset_name="bronze_{clean_target}",
    source_type="{conn.source_type}",
    connection_params=connection_params,
    entity="{req.entity}",
    mode="{req.mode}",
    watermark_column={json.dumps(req.watermark_column)},
    target_name={json.dumps(req.target_name)},
    target_folder={json.dumps(req.target_folder)},
    limit={json.dumps(req.limit)},
    write_strategy="{req.write_strategy or 'append'}",
    primary_key={json.dumps(req.primary_key)},
)

ingest_{clean_target}_job = dg.define_asset_job(
    name="ingest_{clean_target}_job",
    selection=[ingest_{clean_target}_asset],
)

ingest_{clean_target}_schedule = dg.ScheduleDefinition(
    job=ingest_{clean_target}_job,
    cron_schedule="{cron_expr}",
)
'''
            dag_file.write_text(dag_code, encoding="utf-8")
            schedule_log = f"Registered Dagster schedule '{cron_expr}' in git_repo/dags/{dag_file.name}"
        else:
            # Register in Obelisk Built-in Cron
            target_payload = json.dumps({
                "connection_id": req.connection_id,
                "entity": req.entity,
                "mode": req.mode,
                "watermark_column": req.watermark_column,
                "target_name": req.target_name,
                "target_folder": req.target_folder,
                "limit": req.limit,
                "write_strategy": req.write_strategy or "append",
                "primary_key": req.primary_key,
            })
            next_run = cron_engine.compute_next_run(cron_expr)

            stmt_existing = select(CronJob).where(CronJob.name == job_name)
            existing_job = (await db.execute(stmt_existing)).scalar_one_or_none()
            if existing_job:
                existing_job.cron_expression = cron_expr
                existing_job.target = target_payload
                existing_job.next_run_at = next_run
                existing_job.is_enabled = True
            else:
                new_job = CronJob(
                    id=str(uuid.uuid4()),
                    name=job_name,
                    description=f"Automated recurring ingestion for {conn.name}.{req.entity} to Lakehouse Parquet",
                    target_type="ingest",
                    target=target_payload,
                    cron_expression=cron_expr,
                    next_run_at=next_run,
                    is_enabled=True,
                )
                db.add(new_job)
            await db.commit()
            schedule_log = f"Registered Obelisk Cron schedule '{cron_expr}' (Next run: {next_run.strftime('%Y-%m-%d %H:%M:%S UTC') if next_run else 'N/A'})"

    try:
        # Offload synchronous Dagster materialization to worker thread
        run_res = await asyncio.to_thread(
            dagster_client.trigger_ingest,
            source_type=conn.source_type,
            connection_params=connection_params,
            entity=req.entity,
            mode=req.mode,
            watermark_column=req.watermark_column,
            target_name=req.target_name,
            target_folder=req.target_folder,
            limit=req.limit,
            write_strategy=req.write_strategy or "append",
            primary_key=req.primary_key,
        )

        logs = run_res["logs"]
        if schedule_log:
            logs.insert(0, f"[SCHEDULE] {schedule_log}")

        return IngestTriggerResponse(
            success=run_res["success"],
            run_id=run_res["run_id"],
            asset_name=run_res["asset_name"],
            metadata=run_res["metadata"],
            logs=logs,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dagster ingestion failed: {str(e)}"
        )

@router.post("/batch", response_model=BatchIngestResponse)
async def trigger_batch_ingestion(
    req: BatchIngestRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Executes batch ingestion across multiple tables/collections in a schema or database.
    Optionally schedules the entire batch recurringly via Dagster or Obelisk Cron.
    """
    from app.engines.ingest_engine import ingest_engine

    if not req.tables:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No tables specified for batch ingestion."
        )

    stmt = select(Connection).where(Connection.id == req.connection_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{req.connection_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    connection_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username": conn.username,
        "password": plaintext_pw,
        **(conn.options or {}),
    }

    logs: List[str] = [f"Starting batch ingestion for {len(req.tables)} tables from {conn.name} ({conn.source_type})"]
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    # Handle optional scheduling
    if req.schedule and req.schedule.strip():
        cron_expr = req.schedule.strip()
        job_name = f"Batch Ingest: {conn.name} ({len(req.tables)} tables)"

        if req.schedule_engine == "dagster":
            # Generate Dagster DAG in git_repo/dags/
            clean_db = conn.name.lower().replace(" ", "_").replace("-", "_")
            dags_dir = Path(__file__).resolve().parent.parent.parent.parent / "git_repo" / "dags"
            dags_dir.mkdir(parents=True, exist_ok=True)
            dag_file = dags_dir / f"batch_ingest_{clean_db}.py"

            asset_defs = []
            asset_names = []
            for tbl in req.tables:
                clean_t = tbl.replace(".", "_")
                var_name = f"asset_{clean_t}"
                asset_names.append(var_name)
                asset_defs.append(f"""
{var_name} = create_ingest_asset(
    asset_name="bronze_{clean_t}",
    source_type="{conn.source_type}",
    connection_params=connection_params,
    entity="{tbl}",
    mode="{req.mode}",
    watermark_column={json.dumps(req.watermark_column)},
    target_folder={json.dumps(req.target_folder)},
    limit={json.dumps(req.limit)},
    write_strategy="{req.write_strategy}",
    primary_key={json.dumps(req.primary_key)},
)
""")
            dag_code = f"""# Auto-generated Dagster Batch Ingestion Asset Group & Schedule
import dagster as dg
from dagster_workspace.repository import create_ingest_asset

connection_params = {json.dumps(connection_params, indent=4)}

{''.join(asset_defs)}

batch_job = dg.define_asset_job(
    name="batch_ingest_{clean_db}_job",
    selection=[{', '.join(asset_names)}],
)

batch_schedule = dg.ScheduleDefinition(
    job=batch_job,
    cron_schedule="{cron_expr}",
)
"""
            dag_file.write_text(dag_code, encoding="utf-8")
            logs.append(f"[SCHEDULE] Registered Dagster batch schedule '{cron_expr}' in git_repo/dags/{dag_file.name}")
        else:
            # Register in Obelisk Built-in Cron
            target_payload = json.dumps({
                "connection_id": req.connection_id,
                "tables": req.tables,
                "mode": req.mode,
                "watermark_column": req.watermark_column,
                "write_strategy": req.write_strategy,
                "primary_key": req.primary_key,
                "target_folder": req.target_folder,
                "limit": req.limit,
            })
            next_run = cron_engine.compute_next_run(cron_expr)

            stmt_existing = select(CronJob).where(CronJob.name == job_name)
            existing_job = (await db.execute(stmt_existing)).scalar_one_or_none()
            if existing_job:
                existing_job.cron_expression = cron_expr
                existing_job.target = target_payload
                existing_job.next_run_at = next_run
                existing_job.is_enabled = True
            else:
                new_job = CronJob(
                    id=str(uuid.uuid4()),
                    name=job_name,
                    description=f"Batch ingestion of {len(req.tables)} tables from {conn.name}",
                    target_type="batch_ingest",
                    target=target_payload,
                    cron_expression=cron_expr,
                    next_run_at=next_run,
                    is_enabled=True,
                )
                db.add(new_job)
            await db.commit()
            logs.append(f"[SCHEDULE] Registered Obelisk Cron batch schedule '{cron_expr}'")

    # Execute batch extraction
    for table_name in req.tables:
        try:
            res = await asyncio.to_thread(
                ingest_engine.ingest_entity,
                source_type=conn.source_type,
                connection_params=connection_params,
                entity=table_name,
                mode=req.mode,
                watermark_column=req.watermark_column,
                target_folder=req.target_folder,
                limit=req.limit,
                write_strategy=req.write_strategy,
                primary_key=req.primary_key,
            )
            results.append(res)
            logs.append(f"✓ Ingested '{table_name}': {res['num_rows']} total rows (extracted: {res['new_rows_extracted']})")
        except Exception as e:
            errors.append({"table": table_name, "error": str(e)})
            logs.append(f"✗ Failed '{table_name}': {str(e)}")

    succeeded = len(results)
    failed = len(errors)
    logs.append(f"Batch ingestion finished: {succeeded} succeeded, {failed} failed out of {len(req.tables)} total.")

    return BatchIngestResponse(
        success=(failed == 0),
        total_tables=len(req.tables),
        succeeded=succeeded,
        failed=failed,
        results=results,
        errors=errors,
        logs=logs,
    )


# ==============================================================================
# Decoupled Background Ingest Jobs & Real-Time SSE Event Streaming
# ==============================================================================

@router.post("/trigger/async")
async def trigger_ingest_async(
    req: IngestTriggerRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Launches a single-table Lakehouse ingestion job in the background and returns immediately.
    The client can stream live progress and logs via SSE (/jobs/{job_id}/stream).
    """
    from app.engines.ingest_job_manager import ingest_job_manager

    stmt = select(Connection).where(Connection.id == req.connection_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{req.connection_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    connection_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username": conn.username,
        "password": plaintext_pw,
        **(conn.options or {}),
    }

    job = ingest_job_manager.create_job(
        title=f"Ingest {req.entity}",
        job_type="single",
        connection_id=conn.id,
        source_name=conn.name,
        source_type=conn.source_type,
        mode=req.mode,
        total_tables=1,
    )

    # Spawn independent background worker task
    asyncio.create_task(
        ingest_job_manager.execute_single_job_async(
            job_id=job.id,
            connection_params=connection_params,
            entity=req.entity,
            mode=req.mode,
            watermark_column=req.watermark_column,
            target_name=req.target_name,
            target_folder=req.target_folder,
            limit=req.limit,
            write_strategy=req.write_strategy or "append",
            primary_key=req.primary_key,
        )
    )

    return {
        "job_id": job.id,
        "status": "running",
        "title": job.title,
        "source_name": conn.name,
    }


@router.post("/batch/async")
async def trigger_batch_ingest_async(
    req: BatchIngestRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Launches a multi-table batch Lakehouse ingestion job in the background and returns immediately.
    The client can stream live progress and logs via SSE (/jobs/{job_id}/stream).
    """
    from app.engines.ingest_job_manager import ingest_job_manager

    if not req.tables:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No tables specified for batch ingestion."
        )

    stmt = select(Connection).where(Connection.id == req.connection_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{req.connection_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    connection_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username": conn.username,
        "password": plaintext_pw,
        **(conn.options or {}),
    }

    schema_label = f" ({req.schema_name})" if req.schema_name else ""
    job = ingest_job_manager.create_job(
        title=f"Batch Ingest {conn.name}{schema_label} ({len(req.tables)} tables)",
        job_type="batch",
        connection_id=conn.id,
        source_name=conn.name,
        source_type=conn.source_type,
        mode=req.mode,
        total_tables=len(req.tables),
    )

    # Spawn independent background worker task
    asyncio.create_task(
        ingest_job_manager.execute_batch_job_async(
            job_id=job.id,
            connection_params=connection_params,
            tables=req.tables,
            mode=req.mode,
            watermark_column=req.watermark_column,
            target_folder=req.target_folder,
            limit=req.limit,
            write_strategy=req.write_strategy or "append",
            primary_key=req.primary_key,
        )
    )

    return {
        "job_id": job.id,
        "status": "running",
        "title": job.title,
        "source_name": conn.name,
        "total_tables": len(req.tables),
    }


@router.get("/jobs/active")
async def get_active_ingest_jobs():
    """Returns all currently active running ingestion jobs."""
    from app.engines.ingest_job_manager import ingest_job_manager
    return ingest_job_manager.get_active_jobs()


@router.get("/jobs/recent")
async def get_recent_ingest_jobs(limit: int = 20):
    """Returns recent ingestion jobs with completion status and metrics."""
    from app.engines.ingest_job_manager import ingest_job_manager
    return ingest_job_manager.get_recent_jobs(limit=limit)


@router.get("/jobs/{job_id}")
async def get_ingest_job_status(job_id: str):
    """Returns the current state, progress metrics, and log buffer of an ingestion job."""
    from app.engines.ingest_job_manager import ingest_job_manager
    job = ingest_job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found."
        )
    return job.to_dict()


@router.get("/jobs/{job_id}/stream")
async def stream_ingest_job_events(job_id: str):
    """
    Server-Sent Events (SSE) streaming endpoint.
    Streams progress updates and timestamped log lines in real-time.
    Flushes catch-up buffer on connect, making client reconnects and tab-switches completely seamless.
    """
    from app.engines.ingest_job_manager import ingest_job_manager
    job = ingest_job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found."
        )

    return StreamingResponse(
        ingest_job_manager.subscribe(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disables proxy buffering in Caddy / Nginx
        },
    )


@router.post("/jobs/{job_id}/cancel")
async def cancel_ingest_job(job_id: str):
    """Gracefully aborts a running ingestion job."""
    from app.engines.ingest_job_manager import ingest_job_manager
    success = ingest_job_manager.cancel_job(job_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not cancel job '{job_id}' (not found or not running)."
        )
    return {"status": "cancelled", "job_id": job_id}

