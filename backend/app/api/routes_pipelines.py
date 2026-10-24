"""Pipelines API — the orchestrator's public surface.

Routes are split by noun: ``/api/pipelines`` for definitions (graph, schedule,
enable), ``/api/pipeline-runs`` for execution records and their control verbs, and
``/api/step-runs`` for per-step logs. Everything the UI needs to draw a pipeline and
watch it run comes from these three, so no screen has to stitch data together from
unrelated endpoints.

Validation is deliberately strict at the boundary: a graph with a cycle, an edge to
a step that does not exist, or a schedule that cannot parse is rejected before it
reaches the engine. An orchestrator that accepts an impossible graph only fails
later, at three in the morning.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select

from app.database import AsyncSessionLocal
from app.engines.pipeline_engine import (
    EDGE_CONDITIONS,
    OVERLAP_POLICIES,
    RUN_ACTIVE,
    RUN_TERMINAL,
    STEP_TERMINAL,
    STEP_TYPES,
    compute_next_run,
    pipeline_engine,
    preview_runs,
    validate_timezone,
)
from app.models.pipeline import (
    Pipeline,
    PipelineEdge,
    PipelineParam,
    PipelineRun,
    PipelineStep,
    StepRun,
)

router = APIRouter(prefix="/api", tags=["Pipeline Orchestrator"])


# --------------------------------------------------------------------- schemas


class ParamIn(BaseModel):
    name: str
    type: str = Field(default="string", description="string | int | float | bool")
    default_value: Optional[str] = None
    required: bool = False


class StepIn(BaseModel):
    # ``key`` is the designer's node id. Reusing an existing step id keeps run
    # history readable after an edit; anything else becomes a fresh id.
    key: str = Field(..., description="Stable node identifier used by edges.")
    name: str
    step_type: str
    target: str = ""
    args: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    retry_delay_seconds: Optional[int] = None
    position_x: float = 0.0
    position_y: float = 0.0


class EdgeIn(BaseModel):
    from_key: str
    to_key: str
    condition: str = "success"


class GraphIn(BaseModel):
    steps: List[StepIn] = Field(default_factory=list)
    edges: List[EdgeIn] = Field(default_factory=list)
    params: List[ParamIn] = Field(default_factory=list)


class PipelineIn(BaseModel):
    name: str
    description: Optional[str] = ""
    cron_expression: str = "0 2 * * *"
    timezone: str = "UTC"
    is_enabled: bool = True
    overlap_policy: str = "skip"
    max_parallel: int = 3
    keep_runs: int = 100
    timeout_seconds: int = 3600
    max_retries: int = 0
    retry_delay_seconds: int = 60
    auto_rerun_interrupted: bool = False
    graph: Optional[GraphIn] = None


class PipelinePatch(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    is_enabled: Optional[bool] = None
    overlap_policy: Optional[str] = None
    max_parallel: Optional[int] = None
    keep_runs: Optional[int] = None
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    retry_delay_seconds: Optional[int] = None
    auto_rerun_interrupted: Optional[bool] = None


class RunNowRequest(BaseModel):
    params: Optional[Dict[str, Any]] = None

# ------------------------------------------------------------------ validation


def _validate_schedule(cron_expression: str, timezone_name: str) -> None:
    try:
        validate_timezone(timezone_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        compute_next_run(cron_expression, tz=timezone_name)
    except Exception as exc:  # croniter raises several unrelated types
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cron expression {cron_expression!r}. Expected five fields, "
            "e.g. '0 2 * * *'.",
        ) from exc


def _validate_policy(overlap_policy: str) -> None:
    if overlap_policy not in OVERLAP_POLICIES:
        raise HTTPException(
            status_code=400,
            detail=f"overlap_policy must be one of {list(OVERLAP_POLICIES)}.",
        )


def _validate_graph(graph: GraphIn) -> None:
    """Rejects everything the engine could not execute, with a precise message."""
    if not graph.steps:
        raise HTTPException(status_code=400, detail="A pipeline needs at least one step.")

    keys: set = set()
    names: set = set()
    for step in graph.steps:
        if not step.key:
            raise HTTPException(status_code=400, detail="Every step needs a key.")
        if step.key in keys:
            raise HTTPException(status_code=400, detail=f"Duplicate step key '{step.key}'.")
        keys.add(step.key)
        clean_name = (step.name or "").strip()
        if not clean_name:
            raise HTTPException(status_code=400, detail="Every step needs a name.")
        if clean_name.lower() in names:
            raise HTTPException(status_code=400, detail=f"Duplicate step name '{clean_name}'.")
        names.add(clean_name.lower())
        if step.step_type not in STEP_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Step '{clean_name}' has unknown type '{step.step_type}'. "
                f"Expected one of {list(STEP_TYPES)}.",
            )
        if step.step_type in ("script", "notebook", "webhook", "raw_sql") and not (
            step.target or ""
        ).strip():
            raise HTTPException(
                status_code=400,
                detail=f"Step '{clean_name}' ({step.step_type}) requires a target.",
            )
        for limit_name, value in (
            ("timeout_seconds", step.timeout_seconds),
            ("max_retries", step.max_retries),
            ("retry_delay_seconds", step.retry_delay_seconds),
        ):
            if value is not None and value < 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Step '{clean_name}' has a negative {limit_name}.",
                )

    pairs: set = set()
    children: Dict[str, List[str]] = {key: [] for key in keys}
    for edge in graph.edges:
        if edge.from_key not in keys or edge.to_key not in keys:
            raise HTTPException(
                status_code=400,
                detail=f"Edge {edge.from_key!r} → {edge.to_key!r} references an unknown step.",
            )
        if edge.from_key == edge.to_key:
            raise HTTPException(
                status_code=400, detail=f"Step '{edge.from_key}' cannot depend on itself."
            )
        if edge.condition not in EDGE_CONDITIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Edge condition must be one of {list(EDGE_CONDITIONS)}.",
            )
        pair = (edge.from_key, edge.to_key)
        if pair in pairs:
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate edge {edge.from_key!r} → {edge.to_key!r}.",
            )
        pairs.add(pair)
        children[edge.from_key].append(edge.to_key)

    # DFS colouring: a cycle makes dependency resolution undefined.
    WHITE, GREY, BLACK = 0, 1, 2
    colour = {key: WHITE for key in keys}
    path: List[str] = []

    def visit(node: str) -> None:
        colour[node] = GREY
        path.append(node)
        for child in children.get(node, []):
            if colour[child] == GREY:
                cycle = " → ".join(path[path.index(child):] + [child])
                raise HTTPException(status_code=400, detail=f"Cycle detected: {cycle}.")
            if colour[child] == WHITE:
                visit(child)
        path.pop()
        colour[node] = BLACK

    for key in keys:
        if colour[key] == WHITE:
            visit(key)


# ---------------------------------------------------------------- serializing


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _step_out(step: PipelineStep) -> Dict[str, Any]:
    return {
        "id": step.id,
        "key": step.id,
        "name": step.name,
        "step_type": step.step_type,
        "target": step.target,
        "args": json.loads(step.args_json) if step.args_json else {},
        "timeout_seconds": step.timeout_seconds,
        "max_retries": step.max_retries,
        "retry_delay_seconds": step.retry_delay_seconds,
        "position_x": step.position_x,
        "position_y": step.position_y,
    }


def _edge_out(edge: PipelineEdge) -> Dict[str, Any]:
    return {
        "id": edge.id,
        "from_step": edge.from_step,
        "to_step": edge.to_step,
        "from_key": edge.from_step,
        "to_key": edge.to_step,
        "condition": edge.condition,
    }


def _pipeline_out(
    pipeline: Pipeline, step_count: int, is_running: bool
) -> Dict[str, Any]:
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "description": pipeline.description or "",
        "cron_expression": pipeline.cron_expression,
        "timezone": pipeline.timezone,
        "is_enabled": pipeline.is_enabled,
        "overlap_policy": pipeline.overlap_policy,
        "max_parallel": pipeline.max_parallel,
        "keep_runs": pipeline.keep_runs,
        "timeout_seconds": pipeline.timeout_seconds,
        "max_retries": pipeline.max_retries,
        "retry_delay_seconds": pipeline.retry_delay_seconds,
        "auto_rerun_interrupted": pipeline.auto_rerun_interrupted,
        "next_run_at": _iso(pipeline.next_run_at),
        "last_status": pipeline.last_status,
        "last_run_at": _iso(pipeline.last_run_at),
        "step_count": step_count,
        "is_running": is_running,
        "created_at": _iso(pipeline.created_at),
        "updated_at": _iso(pipeline.updated_at),
    }


def _run_out(
    run: PipelineRun,
    pipeline_name: Optional[str] = None,
    step_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    return {
        "id": run.id,
        "pipeline_id": run.pipeline_id,
        "pipeline_name": pipeline_name,
        "kind": run.kind,
        "triggered_by": run.triggered_by,
        "status": run.status,
        "params": json.loads(run.params_json) if run.params_json else {},
        "scheduled_for": _iso(run.scheduled_for),
        "started_at": _iso(run.started_at),
        "ended_at": _iso(run.ended_at),
        "duration_seconds": run.duration_seconds,
        "error": run.error,
        "step_counts": step_counts or {},
    }


def _step_run_out(
    step_run: StepRun,
    step_name: Optional[str] = None,
    include_logs: bool = False,
) -> Dict[str, Any]:
    payload = {
        "id": step_run.id,
        "run_id": step_run.run_id,
        "step_id": step_run.step_id,
        "step_name": step_name or "(deleted step)",
        "attempt": step_run.attempt,
        "status": step_run.status,
        "claimed_at": _iso(step_run.claimed_at),
        "started_at": _iso(step_run.started_at),
        "ended_at": _iso(step_run.ended_at),
        "duration_seconds": step_run.duration_seconds,
        "next_retry_at": _iso(step_run.next_retry_at),
        "error": step_run.error,
        "logs_size": len(step_run.logs or ""),
    }
    if include_logs:
        payload["logs"] = step_run.logs or ""
    return payload



# ------------------------------------------------------------- graph writing


async def _replace_graph(session, pipeline: Pipeline, graph: GraphIn) -> None:
    """Replaces steps, edges and params wholesale (caller commits).

    Step ids that the client sends back are preserved, so ``step_runs`` rows from
    earlier runs still resolve to a name. Steps that disappear leave their history
    behind as ``(deleted step)`` rather than cascading into old runs.
    """
    _validate_graph(graph)

    await session.execute(delete(PipelineEdge).where(PipelineEdge.pipeline_id == pipeline.id))
    old_steps = (
        await session.execute(
            select(PipelineStep).where(PipelineStep.pipeline_id == pipeline.id)
        )
    ).scalars().all()
    reusable = {step.id for step in old_steps}
    for step in old_steps:
        await session.delete(step)
    await session.flush()

    id_by_key: Dict[str, str] = {}
    for step in graph.steps:
        step_id = step.key if step.key in reusable else str(uuid.uuid4())
        id_by_key[step.key] = step_id
        session.add(
            PipelineStep(
                id=step_id,
                pipeline_id=pipeline.id,
                name=step.name.strip(),
                step_type=step.step_type,
                target=(step.target or "").strip(),
                args_json=json.dumps(step.args or {}) if step.args else None,
                timeout_seconds=step.timeout_seconds,
                max_retries=step.max_retries,
                retry_delay_seconds=step.retry_delay_seconds,
                position_x=step.position_x,
                position_y=step.position_y,
            )
        )

    await session.execute(delete(PipelineParam).where(PipelineParam.pipeline_id == pipeline.id))
    for param in graph.params:
        session.add(
            PipelineParam(
                pipeline_id=pipeline.id,
                name=param.name.strip(),
                type=param.type,
                default_value=param.default_value,
                required=param.required,
            )
        )

    await session.flush()
    for edge in graph.edges:
        session.add(
            PipelineEdge(
                pipeline_id=pipeline.id,
                from_step=id_by_key[edge.from_key],
                to_step=id_by_key[edge.to_key],
                condition=edge.condition,
            )
        )


async def _assert_idle(session, pipeline_id: str) -> None:
    active = (
        await session.execute(
            select(func.count())
            .select_from(PipelineRun)
            .where(
                PipelineRun.pipeline_id == pipeline_id,
                PipelineRun.status.in_(tuple(RUN_ACTIVE)),
            )
        )
    ).scalar_one()
    if active:
        raise HTTPException(
            status_code=409,
            detail="This pipeline has an active run. Cancel it (or wait) before editing its graph.",
        )


# ------------------------------------------------------------------- endpoints


@router.get("/pipelines/preview")
async def preview_schedule(
    cron_expression: str = Query("0 2 * * *"),
    timezone_name: str = Query("UTC", alias="timezone"),
    count: int = Query(5, ge=1, le=20),
):
    """Next occurrences of a schedule, for the editor's live preview."""
    _validate_schedule(cron_expression, timezone_name)
    return {
        "next_runs": [d.isoformat() for d in preview_runs(cron_expression, timezone_name, count)],
        "timezone": timezone_name,
    }


def _collect_targets() -> Dict[str, Any]:
    """Synchronous scan of the workspace (wrapped in a thread by the route)."""
    from app.engines.pipeline_engine import pipeline_engine as engine

    scripts = sorted(
        f.name for f in engine.scripts_dir.glob("*.py") if not f.name.startswith("_")
    )
    notebooks = sorted(
        f.name for f in engine.notebooks_dir.glob("*.py") if not f.name.startswith("_")
    )
    dbt_models: List[str] = []
    dbt_dirs: List[str] = []
    models_dir = engine.workspace_root / "git_repo" / "dbt_project" / "models"
    if models_dir.exists():
        for sub in sorted(models_dir.iterdir()):
            if sub.is_dir() and not sub.name.startswith(("_", ".")):
                dbt_dirs.append(f"models/{sub.name}")
        dbt_models = sorted(
            f.stem for f in models_dir.rglob("*.sql") if not f.name.startswith(("_", "."))
        )
    return {
        "scripts": scripts,
        "notebooks": notebooks,
        "dbt_models": dbt_models,
        "dbt_directories": dbt_dirs,
        "step_types": list(STEP_TYPES),
        "edge_conditions": list(EDGE_CONDITIONS),
    }


@router.get("/pipelines/targets")
async def list_targets():
    """Runnable targets for the step pickers (scripts, notebooks, dbt models)."""
    return await asyncio.to_thread(_collect_targets)



@router.get("/pipelines")
async def list_pipelines():
    async with AsyncSessionLocal() as session:
        pipelines = (
            await session.execute(select(Pipeline).order_by(Pipeline.name.asc()))
        ).scalars().all()
        counts = dict(
            (
                await session.execute(
                    select(PipelineStep.pipeline_id, func.count()).group_by(PipelineStep.pipeline_id)
                )
            ).all()
        )
        active = set(
            (
                await session.execute(
                    select(PipelineRun.pipeline_id)
                    .where(PipelineRun.status.in_(tuple(RUN_ACTIVE)))
                    .distinct()
                )
            ).scalars().all()
        )
        return [
            _pipeline_out(p, int(counts.get(p.id, 0)), p.id in active) for p in pipelines
        ]


@router.post("/pipelines", status_code=201)
async def create_pipeline(req: PipelineIn):
    _validate_schedule(req.cron_expression, req.timezone)
    _validate_policy(req.overlap_policy)
    async with AsyncSessionLocal() as session:
        existing = (
            await session.execute(select(Pipeline.id).where(Pipeline.name == req.name.strip()))
        ).scalars().first()
        if existing:
            raise HTTPException(
                status_code=400, detail=f"A pipeline named '{req.name.strip()}' already exists."
            )
        pipeline = Pipeline(
            name=req.name.strip(),
            description=req.description or "",
            cron_expression=req.cron_expression.strip(),
            timezone=req.timezone,
            is_enabled=req.is_enabled,
            overlap_policy=req.overlap_policy,
            max_parallel=max(1, req.max_parallel),
            keep_runs=max(1, req.keep_runs),
            timeout_seconds=max(1, req.timeout_seconds),
            max_retries=max(0, req.max_retries),
            retry_delay_seconds=max(0, req.retry_delay_seconds),
            auto_rerun_interrupted=req.auto_rerun_interrupted,
        )
        if req.is_enabled:
            pipeline.next_run_at = compute_next_run(
                pipeline.cron_expression, tz=pipeline.timezone
            )
        session.add(pipeline)
        await session.flush()
        if req.graph:
            await _replace_graph(session, pipeline, req.graph)
        await session.commit()
        await session.refresh(pipeline)
        return _pipeline_out(pipeline, len(req.graph.steps) if req.graph else 0, False)



@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
        steps = (
            await session.execute(
                select(PipelineStep)
                .where(PipelineStep.pipeline_id == pipeline_id)
                .order_by(PipelineStep.name.asc())
            )
        ).scalars().all()
        edges = (
            await session.execute(
                select(PipelineEdge).where(PipelineEdge.pipeline_id == pipeline_id)
            )
        ).scalars().all()
        params = (
            await session.execute(
                select(PipelineParam)
                .where(PipelineParam.pipeline_id == pipeline_id)
                .order_by(PipelineParam.name.asc())
            )
        ).scalars().all()
        last_run = (
            await session.execute(
                select(PipelineRun)
                .where(PipelineRun.pipeline_id == pipeline_id)
                .order_by(PipelineRun.id.desc())
                .limit(1)
            )
        ).scalars().first()
        active = (
            await session.execute(
                select(func.count())
                .select_from(PipelineRun)
                .where(
                    PipelineRun.pipeline_id == pipeline_id,
                    PipelineRun.status.in_(tuple(RUN_ACTIVE)),
                )
            )
        ).scalar_one()

        payload = _pipeline_out(pipeline, len(steps), bool(active))
        payload.update(
            {
                "steps": [_step_out(s) for s in steps],
                "edges": [_edge_out(e) for e in edges],
                "params": [
                    {
                        "name": p.name,
                        "type": p.type,
                        "default_value": p.default_value,
                        "required": p.required,
                    }
                    for p in params
                ],
                "last_run": _run_out(last_run, pipeline.name) if last_run else None,
            }
        )
        return payload


@router.patch("/pipelines/{pipeline_id}")
async def update_pipeline(pipeline_id: str, req: PipelinePatch):
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found.")

        data = req.model_dump(exclude_unset=True)
        if "cron_expression" in data or "timezone" in data:
            _validate_schedule(
                data.get("cron_expression", pipeline.cron_expression),
                data.get("timezone", pipeline.timezone),
            )
        if "overlap_policy" in data:
            _validate_policy(data["overlap_policy"])
        if "name" in data and data["name"]:
            clash = (
                await session.execute(
                    select(Pipeline.id).where(
                        Pipeline.name == data["name"].strip(), Pipeline.id != pipeline_id
                    )
                )
            ).scalars().first()
            if clash:
                raise HTTPException(
                    status_code=400, detail=f"A pipeline named '{data['name']}' already exists."
                )
            data["name"] = data["name"].strip()

        for field, value in data.items():
            setattr(pipeline, field, value)
        pipeline.max_parallel = max(1, pipeline.max_parallel)
        pipeline.keep_runs = max(1, pipeline.keep_runs)

        if pipeline.is_enabled:
            pipeline.next_run_at = compute_next_run(
                pipeline.cron_expression, tz=pipeline.timezone
            )
        else:
            pipeline.next_run_at = None
        await session.commit()
        await session.refresh(pipeline)

        step_count = (
            await session.execute(
                select(func.count())
                .select_from(PipelineStep)
                .where(PipelineStep.pipeline_id == pipeline_id)
            )
        ).scalar_one()

@router.put("/pipelines/{pipeline_id}/graph")
async def replace_graph(pipeline_id: str, graph: GraphIn):
    """Replaces the whole graph (steps, edges, params) in one shot.

    Refused while a run is active: swapping step ids under a running graph would
    orphan the rows that describe it.
    """
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
        await _assert_idle(session, pipeline_id)
        await _replace_graph(session, pipeline, graph)
        await session.commit()
        return {
            "status": "saved",
            "pipeline_id": pipeline_id,
            "step_count": len(graph.steps),
            "edge_count": len(graph.edges),
        }



        return _pipeline_out(pipeline, int(step_count), False)



@router.post("/pipelines/{pipeline_id}/run", status_code=202)
async def run_pipeline_now(pipeline_id: str, req: Optional[RunNowRequest] = None):
    """Queues an ad-hoc run. The engine picks it up on its next tick (or wake)."""
    try:
        run_id = await pipeline_engine.run_now(
            pipeline_id, params=req.params if req else None, triggered_by="manual"
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"run_id": run_id, "pipeline_id": pipeline_id, "status": "pending"}


@router.post("/pipelines/{pipeline_id}/toggle")
async def toggle_pipeline(pipeline_id: str):
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
        pipeline.is_enabled = not pipeline.is_enabled
        pipeline.next_run_at = (
            compute_next_run(pipeline.cron_expression, tz=pipeline.timezone)
            if pipeline.is_enabled
            else None
        )
        await session.commit()
        return {
            "id": pipeline.id,
            "is_enabled": pipeline.is_enabled,
            "next_run_at": _iso(pipeline.next_run_at),
        }


async def _cancel_active_runs(pipeline_id: str) -> None:
    """Cancels every active run of a pipeline before its rows are deleted."""
    async with AsyncSessionLocal() as session:
        run_ids = (
            await session.execute(
                select(PipelineRun.id).where(
                    PipelineRun.pipeline_id == pipeline_id,
                    PipelineRun.status.in_(tuple(RUN_ACTIVE)),
                )
            )
        ).scalars().all()
    for run_id in run_ids:
        await pipeline_engine.cancel_run(run_id)


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
    await _cancel_active_runs(pipeline_id)
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        if pipeline:
            await session.delete(pipeline)
            await session.commit()
    return {"status": "deleted", "id": pipeline_id}



# ------------------------------------------------------------------ runs API


@router.get("/pipeline-runs")
async def list_runs(
    pipeline_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    async with AsyncSessionLocal() as session:
        stmt = select(PipelineRun, Pipeline.name).join(
            Pipeline, Pipeline.id == PipelineRun.pipeline_id
        )
        if pipeline_id:
            stmt = stmt.where(PipelineRun.pipeline_id == pipeline_id)
        if status:
            stmt = stmt.where(PipelineRun.status == status)
        # Newest first. scheduled_for carries the anchor time for schedule-created
        # runs; started_at is null until they begin.
        stmt = stmt.order_by(
            func.coalesce(PipelineRun.started_at, PipelineRun.scheduled_for).desc(),
            PipelineRun.id.desc(),
        ).limit(limit)
        rows = (await session.execute(stmt)).all()

        run_ids = [run.id for run, _ in rows]
        counts: Dict[str, Dict[str, int]] = {}
        if run_ids:
            for run_id, step_status, count in (
                await session.execute(
                    select(StepRun.run_id, StepRun.status, func.count())
                    .where(StepRun.run_id.in_(run_ids))
                    .group_by(StepRun.run_id, StepRun.status)
                )
            ).all():
                counts.setdefault(run_id, {})[step_status] = int(count)

        return [_run_out(run, name, counts.get(run.id, {})) for run, name in rows]


@router.get("/pipeline-runs/{run_id}")
async def get_run(run_id: str):
    async with AsyncSessionLocal() as session:
        run = await session.get(PipelineRun, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found.")
        pipeline = await session.get(Pipeline, run.pipeline_id)
        step_runs = (
            await session.execute(
                select(StepRun)
                .where(StepRun.run_id == run_id)
                .order_by(StepRun.started_at.asc(), StepRun.id.asc())
            )
        ).scalars().all()
        step_names = dict(
            (
                await session.execute(
                    select(PipelineStep.id, PipelineStep.name).where(
                        PipelineStep.pipeline_id == run.pipeline_id
                    )
                )
            ).all()
        )
        counts: Dict[str, int] = {}
        for sr in step_runs:
            counts[sr.status] = counts.get(sr.status, 0) + 1
        payload = _run_out(run, pipeline.name if pipeline else None, counts)
        payload["step_runs"] = [
            _step_run_out(sr, step_names.get(sr.step_id), include_logs=False)
            for sr in step_runs
        ]
        return payload



@router.post("/pipeline-runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    cancelled = await pipeline_engine.cancel_run(run_id)
    if not cancelled:
        raise HTTPException(
            status_code=409, detail="Run is already finished or does not exist."
        )
    return {"status": "cancelling", "run_id": run_id}


@router.post("/pipeline-runs/{run_id}/rerun-from/{step_id}", status_code=202)
async def rerun_from(run_id: str, step_id: str):
    try:
        new_run_id = await pipeline_engine.rerun_from(run_id, step_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"run_id": new_run_id, "source_run_id": run_id, "from_step": step_id}


@router.get("/step-runs/{step_run_id}/logs")
async def step_logs(step_run_id: str, after: int = Query(0, ge=0)):
    """Incremental log tail: poll with ``after=<previous size>`` while running."""
    async with AsyncSessionLocal() as session:
        step_run = await session.get(StepRun, step_run_id)
        if not step_run:
            raise HTTPException(status_code=404, detail="Step run not found.")
        text = step_run.logs or ""
        return {
            "id": step_run.id,
            "status": step_run.status,
            "size": len(text),
            "offset": after,
            "logs": text[after:],
        }

