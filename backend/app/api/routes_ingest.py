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
from app.core.catalog import InvalidTablePath, validate_column
from app.engines.ingest_engine import IngestEngine
from app.engines.ingest_job_manager import ingest_job_manager
from app.engines.ingest_runner import trigger_ingest

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

from datetime import datetime, timezone

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
    schema_policy: Optional[str] = Field("evolve", description="'evolve' or 'strict'")
    
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
    schema_policy: Optional[str] = Field("evolve", description="'evolve' or 'strict'")
    target_folder: Optional[str] = Field(None, description="Target lakehouse folder/directory")
    limit: Optional[int] = Field(None, description="Optional row limit for sample ingestion")
    
class BatchIngestResponse(BaseModel):
    success: bool
    total_tables: int
    succeeded: int
    failed: int
    results: List[Dict[str, Any]]
    errors: List[Dict[str, str]]
    logs: List[str]

def _reject_bad_target(req, tables: Optional[List[str]] = None) -> None:
    """Refuse a target name before any work starts, so the reason is a 422.

    The engine validates the same way when it runs — this only moves the answer forward,
    out of a background job and into the request. A rejected name is never a 500.
    """
    entities = tables if tables is not None else [getattr(req, "entity", None)]
    try:
        for entity in entities:
            IngestEngine._address(
                getattr(req, "target_folder", None),
                entity or "",
                getattr(req, "target_name", None),
            )
        if getattr(req, "watermark_column", None):
            validate_column(req.watermark_column, what="watermark column")
        if getattr(req, "primary_key", None):
            validate_column(req.primary_key, what="primary key")
    except InvalidTablePath as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/trigger", response_model=IngestTriggerResponse)
async def trigger_ingestion(
    req: IngestTriggerRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Streams data from the source into the Bronze lakehouse layer.
    Executes in background threadpool to maintain FastAPI async loop responsiveness.
    """
    _reject_bad_target(req)

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


    try:
        run_res = await asyncio.to_thread(
            trigger_ingest,
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
            schema_policy=req.schema_policy or "evolve",
        )

        logs = run_res["logs"]
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
            detail=f"Ingestion failed: {str(e)}"
        )

@router.post("/batch", response_model=BatchIngestResponse)
async def trigger_batch_ingestion(
    req: BatchIngestRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Executes batch ingestion across multiple tables/collections in a schema or database.
    """
    from app.engines.ingest_engine import ingest_engine

    _reject_bad_target(req, req.tables)

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
                schema_policy=req.schema_policy or "evolve",
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
    _reject_bad_target(req)

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
            schema_policy=req.schema_policy or "evolve",
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
    _reject_bad_target(req, req.tables)

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
            schema_policy=req.schema_policy or "evolve",
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
    return ingest_job_manager.get_active_jobs()


@router.get("/jobs/recent")
async def get_recent_ingest_jobs(limit: int = 20):
    """Returns recent ingestion jobs with completion status and metrics."""
    return ingest_job_manager.get_recent_jobs(limit=limit)


@router.get("/jobs/{job_id}")
async def get_ingest_job_status(job_id: str):
    """Returns the current state, progress metrics, and log buffer of an ingestion job."""
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
    success = ingest_job_manager.cancel_job(job_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not cancel job '{job_id}' (not found or not running)."
        )
    return {"status": "cancelled", "job_id": job_id}

