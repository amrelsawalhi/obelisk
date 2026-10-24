"""The Obelisk pipeline orchestrator.

One in-process engine owns every schedule, every dependency edge and every run.
It replaces the cron engine (one flat row per job, no dependencies, logs only on
completion) and Dagster (out-of-process, its own database, no conditional edges).

Design rule: **the database is the scheduler; this process is only a consumer.**

* Every decision is a row transition committed before or after the side effect it
  describes, so a crash leaves a truthful record rather than a lost one.
* Runs are claimed with ``UPDATE … WHERE status='pending'`` plus a rowcount check,
  which makes double execution impossible even if two engines share one file.
* ``next_run_at`` advances from the previous anchor (never from "now"), so a slow or
  missed tick can never shift a schedule.
* Logs are flushed to the step row every second while a child runs; a crash keeps
  everything up to the moment of death.

Terminal step states: ``success``, ``failed``, ``skipped``, ``cancelled``,
``interrupted``. A step is finished exactly when it holds one.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from sqlalchemy import delete, func, select, update

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.pipeline import (
    Pipeline,
    PipelineEdge,
    PipelineParam,
    PipelineRun,
    PipelineStep,
    StepRun,
)

logger = logging.getLogger(__name__)

TICK_SECONDS = 5
# A step waiting on upstreams for longer than this can never run: one of its
# producers is gone (deleted mid-run) and nothing will change its state again.
MAX_DEPENDENCY_WAIT_SECONDS = 24 * 3600
MAX_LOG_CHARS = 2_000_000

STEP_TYPES = ("dbt_run", "dbt_test", "script", "notebook", "webhook", "raw_sql")
EDGE_CONDITIONS = ("success", "failure", "completion", "skipped")
OVERLAP_POLICIES = ("skip", "queue", "kill")

STEP_TERMINAL = frozenset({"success", "failed", "skipped", "cancelled", "interrupted"})
RUN_ACTIVE = frozenset({"pending", "waiting", "running"})
RUN_TERMINAL = frozenset({"success", "failed", "cancelled", "interrupted", "skipped"})


class StepCancelled(Exception):
    """Raised inside a child runner when the operator cancelled the run."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Normalises a datetime read back from SQLite (always naive) into aware UTC.

    The columns are ``DateTime`` without a timezone, so values come back naive while
    everything the engine computes with is aware. Comparing the two raises
    ``TypeError``, and silently assuming local time would be worse.
    """
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _interpreter_path() -> str:
    """Returns the interpreter that owns this process's installed packages.

    ``sys.executable`` is not reliable here: uvicorn's reloader spawns its worker
    from the *base* interpreter (multiprocessing uses ``sys._base_executable``), so
    children launched with it lose the venv's site-packages and fail on
    ``import marimo``/``import pydantic_settings``. Locate the venv that provides the
    packages we are currently running with via its ``pyvenv.cfg``.
    """
    for entry in sys.path:
        if not entry:
            continue
        p = Path(entry)
        if not p.is_dir():
            continue
        for candidate_root in (p, *p.parents):
            if (candidate_root / "pyvenv.cfg").is_file():
                exe = candidate_root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
                if exe.is_file():
                    return str(exe)
                break
    return sys.executable


def validate_timezone(tz: str) -> str:
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Unknown timezone {tz!r}.") from exc
    return tz


def compute_next_run(
    cron_expr: str, base_time: Optional[datetime] = None, tz: str = "UTC"
) -> datetime:
    """Next occurrence after ``base_time`` in ``tz``, returned as an aware UTC datetime.

    Cron expressions are always interpreted in the pipeline's timezone; storing the
    result in UTC keeps every comparison in the database offset-free.
    """
    zone = ZoneInfo(tz or "UTC")
    base = base_time or utcnow()
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    local_base = base.astimezone(zone)
    nxt = croniter(cron_expr, local_base).get_next(datetime)
    if nxt.tzinfo is None:
        nxt = nxt.replace(tzinfo=zone)
    return nxt.astimezone(timezone.utc)


def preview_runs(cron_expr: str, tz: str = "UTC", count: int = 5) -> List[datetime]:
    out: List[datetime] = []
    base = utcnow()
    for _ in range(max(1, min(count, 20))):
        base = compute_next_run(cron_expr, base, tz)
        out.append(base)
    return out


def _cond_matches(condition: str, upstream_status: str) -> bool:
    if condition == "success":
        return upstream_status == "success"
    if condition == "failure":
        return upstream_status == "failed"
    if condition == "skipped":
        return upstream_status == "skipped"
    if condition == "completion":
        return upstream_status in STEP_TERMINAL


def _coerce_param(raw: Any, kind: str) -> Any:
    if raw is None:
        return None
    if kind == "int":
        return int(raw)
    if kind == "float":
        return float(raw)
    if kind == "bool":
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return str(raw)


def render_sql_params(sql: str, params: Dict[str, Any]) -> str:
    """Substitutes ``{{name}}`` placeholders with escaped SQL literals.

    DuckDB's ``conn.sql`` path has no bind-parameter API, so the literal is built
    here: numbers and booleans unquoted, everything else as a quoted string with
    embedded quotes doubled. Only pipeline parameters (operator-defined, typed) are
    ever substituted — never request bodies.
    """
    out = sql
    for name, value in params.items():
        if value is None:
            literal = "NULL"
        elif isinstance(value, bool):
            literal = "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            literal = "'" + str(value).replace("'", "''") + "'"
        out = out.replace("{{" + name + "}}", literal)
        out = out.replace("{{ " + name + " }}", literal)
    return out


class PipelineEngine:
    """Scheduling, dependency resolution and execution for every pipeline."""

    def __init__(self) -> None:
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        # In-flight executor tasks, so stop() can cancel them instead of leaving
        # un-awaited coroutines behind on shutdown.
        self._tasks: set = set()
        # step_run_id -> live child process, killed on timeout/cancel/shutdown.
        self._procs: Dict[str, subprocess.Popen] = {}
        # run ids the operator asked to cancel; executors consult it after exit.
        self._cancel_runs: set = set()
        # step_run_id -> length of the log last written, so an idle step does not
        # re-write identical text (and contend for the single SQLite writer).
        self._flushed: Dict[str, int] = {}

        workspace_root = Path(__file__).resolve().parent.parent.parent.parent
        self.workspace_root = workspace_root
        self.scripts_dir = (workspace_root / "git_repo" / "scripts").resolve()
        self.notebooks_dir = (workspace_root / "git_repo" / "notebooks").resolve()
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._tick_loop())
        logger.info("Pipeline engine started (tick=%ss).", TICK_SECONDS)

    async def stop(self, grace: float = 5.0) -> None:
        """Stops the loop, kills children and lets executors unwind cleanly.

        Executors are given a grace window before being cancelled: cancelling a task
        that is inside a database transaction can leave that write lock behind, and
        the next writer then stalls on ``busy_timeout``. Children are killed first, so
        the executors finish on their own almost immediately.
        """
        self._running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
        for proc in list(self._procs.values()):
            try:
                proc.kill()
            except OSError:
                pass
        self._procs.clear()

        pending = [task for task in self._tasks if not task.done()]
        if pending:
            await asyncio.wait(pending, timeout=grace)
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
        self._tasks.clear()

    # ------------------------------------------------------------- boot healing

    async def heal_on_start(self) -> None:
        """Reconciles rows a dead process left behind.

        Three cases, and they are deliberately distinguished:

        * ``claimed_at`` set, ``started_at`` null — the claim committed but the child
          never spawned, so nothing outside this database was touched. Safe to
          re-queue: the work still has to happen.
        * ``started_at`` set — the child was running and its fate is unknown. Marked
          ``interrupted``; the operator (or ``auto_rerun_interrupted``) decides.
        * runs still ``pending``/``waiting`` — untouched, and the tick loop resumes
          them on its first pass.
        """
        now = utcnow()
        requeued = 0
        interrupted = 0
        async with AsyncSessionLocal() as session:
            claimed_only = (
                await session.execute(
                    select(StepRun).where(
                        StepRun.status == "running", StepRun.started_at.is_(None)
                    )
                )
            ).scalars().all()
            for step_run in claimed_only:
                step_run.status = "pending"
                step_run.claimed_at = None
                step_run.logs = (step_run.logs or "") + (
                    "\nRe-queued: claimed but never started before restart.\n"
                )
                requeued += 1

            started = (
                await session.execute(
                    select(StepRun).where(
                        StepRun.status == "running", StepRun.started_at.is_not(None)
                    )
                )
            ).scalars().all()
            for step_run in started:
                step_run.status = "interrupted"
                step_run.ended_at = now
                step_run.error = "Interrupted by an Obelisk restart while executing."
                step_run.logs = (step_run.logs or "") + (
                    "\nInterrupted by an Obelisk restart while executing. "
                    "The step did not report a result.\n"
                )
                if step_run.started_at:
                    step_run.duration_seconds = round(
                        (now - as_utc(step_run.started_at)).total_seconds(), 2
                    )
                interrupted += 1

            # Runs left mid-flight are finalized by the next tick once their steps
            # are terminal; nothing to force here.
            await session.commit()

        if requeued or interrupted:
            logger.warning(
                "Pipeline healing: %s step(s) re-queued, %s marked interrupted.",
                requeued,
                interrupted,
            )


    async def _tick_loop(self) -> None:
        try:
            await self.heal_on_start()
        except Exception:  # healing must never take the scheduler down
            logger.exception("Pipeline boot healing failed.")
        while self._running:
            try:
                await self._create_scheduled_runs()
                await self._advance_active_runs()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Pipeline tick failed.")
            await asyncio.sleep(TICK_SECONDS)

    async def wake(self) -> None:
        """Runs one scheduler tick immediately (new run, finished step, cancel)."""
        if not self._running:
            return
        try:
            await self._create_scheduled_runs()
            await self._advance_active_runs()
        except Exception:
            logger.exception("Pipeline wake tick failed.")

    # -------------------------------------------------------------- scheduling

    async def _create_scheduled_runs(self) -> None:
        """Turns due schedule anchors into run rows, exactly once per occurrence."""
        now = utcnow()
        kill_after_commit: List[str] = []
        async with AsyncSessionLocal() as session:
            due = (
                await session.execute(
                    select(Pipeline).where(
                        Pipeline.is_enabled.is_(True),
                        Pipeline.next_run_at.is_not(None),
                        Pipeline.next_run_at <= now,
                    )
                )
            ).scalars().all()

            for pipeline in due:
                scheduled_for = pipeline.next_run_at
                # Advance the anchor from the occurrence itself, never from "now", and
                # skip every occurrence that was missed in between: one catch-up run,
                # not a backfill storm. (A 10-minute job on an hourly schedule still
                # fires at :00 because the anchor is the occurrence, not this tick.)
                try:
                    anchor = compute_next_run(
                        pipeline.cron_expression, scheduled_for, pipeline.timezone
                    )
                    skipped_ahead = 0
                    while as_utc(anchor) <= now and skipped_ahead < 5000:
                        anchor = compute_next_run(
                            pipeline.cron_expression, anchor, pipeline.timezone
                        )
                        skipped_ahead += 1
                except Exception:
                    logger.warning(
                        "Pipeline %s has an invalid schedule %r; disabling it.",
                        pipeline.name,
                        pipeline.cron_expression,
                    )
                    await session.execute(
                        update(Pipeline)
                        .where(
                            Pipeline.id == pipeline.id,
                            Pipeline.next_run_at == scheduled_for,
                        )
                        .values(next_run_at=None, is_enabled=False)
                    )
                    continue

                # Claim the occurrence atomically. Two schedulers (a second worker, or
                # a tick racing a manual wake) both see the same due anchor; only the
                # UPDATE that still matches the old value may create the run.
                claimed = await session.execute(
                    update(Pipeline)
                    .where(
                        Pipeline.id == pipeline.id,
                        Pipeline.next_run_at == scheduled_for,
                    )
                    .values(next_run_at=anchor)
                )
                if claimed.rowcount != 1:
                    continue
                pipeline.next_run_at = anchor

                active = (
                    await session.execute(
                        select(PipelineRun.id).where(
                            PipelineRun.pipeline_id == pipeline.id,
                            PipelineRun.status.in_(tuple(RUN_ACTIVE)),
                        )
                    )
                ).scalars().all()

                if active and pipeline.overlap_policy == "skip":
                    session.add(
                        PipelineRun(
                            pipeline_id=pipeline.id,
                            kind="scheduled",
                            triggered_by="schedule",
                            status="skipped",
                            scheduled_for=scheduled_for,
                            started_at=now,
                            ended_at=now,
                            duration_seconds=0.0,
                            error=(
                                "Skipped: a previous run was still active "
                                "(overlap policy 'skip')."
                            ),
                        )
                    )
                    continue
                if active and pipeline.overlap_policy == "queue":
                    session.add(
                        PipelineRun(
                            pipeline_id=pipeline.id,
                            kind="scheduled",
                            triggered_by="schedule",
                            status="waiting",
                            scheduled_for=scheduled_for,
                        )
                    )
                    continue
                if active and pipeline.overlap_policy == "kill":
                    kill_after_commit.extend(active)

                await self._insert_run_rows(
                    session,
                    pipeline,
                    kind="scheduled",
                    triggered_by="schedule",
                    scheduled_for=scheduled_for,
                )

            await session.commit()

        for run_id in kill_after_commit:
            await self.cancel_run(run_id)


    async def _insert_run_rows(
        self,
        session,
        pipeline: Pipeline,
        *,
        kind: str,
        triggered_by: Optional[str],
        scheduled_for: Optional[datetime] = None,
        overrides: Optional[Dict[str, Any]] = None,
        step_ids: Optional[List[str]] = None,
    ) -> PipelineRun:
        """Creates a run plus one pending ``step_runs`` row per step (caller commits).

        ``step_ids`` restricts the run to a subtree — that is how "re-run from step"
        works without inventing a second execution model.
        """
        params = await self._resolve_params(session, pipeline.id, overrides)
        run = PipelineRun(
            pipeline_id=pipeline.id,
            kind=kind,
            triggered_by=triggered_by,
            params_json=json.dumps(params),
            status="pending",
            scheduled_for=scheduled_for,
        )
        session.add(run)
        await session.flush()

        stmt = select(PipelineStep).where(PipelineStep.pipeline_id == pipeline.id)
        if step_ids is not None:
            stmt = stmt.where(PipelineStep.id.in_(step_ids))
        steps = (await session.execute(stmt)).scalars().all()
        for step in steps:
            session.add(StepRun(run_id=run.id, step_id=step.id, status="pending"))
        return run

    async def _resolve_params(
        self, session, pipeline_id: str, overrides: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        rows = (
            await session.execute(
                select(PipelineParam).where(PipelineParam.pipeline_id == pipeline_id)
            )
        ).scalars().all()
        resolved: Dict[str, Any] = {}
        for param in rows:
            raw = None
            if overrides and param.name in overrides:
                raw = overrides[param.name]
            elif param.default_value is not None:
                raw = param.default_value
            if raw is None:
                if param.required:
                    raise ValueError(f"Parameter '{param.name}' is required.")
                resolved[param.name] = None
                continue
            try:
                resolved[param.name] = _coerce_param(raw, param.type)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Parameter '{param.name}' is not a valid {param.type}."
                ) from exc
        # Overrides for undeclared names are still usable: ad-hoc runs stay flexible.
        for key, value in (overrides or {}).items():
            resolved.setdefault(key, value)
        return resolved

    # ---------------------------------------------------------- public actions

    async def run_now(
        self,
        pipeline_id: str,
        params: Optional[Dict[str, Any]] = None,
        triggered_by: str = "manual",
        kind: str = "manual",
    ) -> str:
        async with AsyncSessionLocal() as session:
            pipeline = await session.get(Pipeline, pipeline_id)
            if not pipeline:
                raise ValueError(f"Pipeline '{pipeline_id}' not found.")
            run = await self._insert_run_rows(
                session, pipeline, kind=kind, triggered_by=triggered_by, overrides=params
            )
            await session.commit()
            run_id = run.id
        await self.wake()
        return run_id


    async def rerun_from(self, run_id: str, step_id: str, triggered_by: str = "manual") -> str:
        """Starts a new run containing ``step_id`` and everything downstream of it."""
        async with AsyncSessionLocal() as session:
            source = await session.get(PipelineRun, run_id)
            if not source:
                raise ValueError(f"Run '{run_id}' not found.")
            pipeline = await session.get(Pipeline, source.pipeline_id)
            if not pipeline:
                raise ValueError("Pipeline no longer exists.")
            edges = (
                await session.execute(
                    select(PipelineEdge).where(PipelineEdge.pipeline_id == pipeline.id)
                )
            ).scalars().all()
            steps = (
                await session.execute(
                    select(PipelineStep).where(PipelineStep.pipeline_id == pipeline.id)
                )
            ).scalars().all()
            step_ids = self._subtree_ids(edges, [s.id for s in steps], step_id)
            if not step_ids:
                raise ValueError(f"Step '{step_id}' is not part of this pipeline.")
            overrides = json.loads(source.params_json) if source.params_json else None
            run = await self._insert_run_rows(
                session,
                pipeline,
                kind="rerun",
                triggered_by=triggered_by,
                overrides=overrides,
                step_ids=sorted(step_ids),
            )
            await session.commit()
            new_run_id = run.id
        await self.wake()
        return new_run_id

    @staticmethod
    def _subtree_ids(edges, all_step_ids: List[str], root: str) -> set:
        """Step ids reachable from ``root`` through any edge condition (inclusive)."""
        if root not in all_step_ids:
            return set()
        children: Dict[str, List[str]] = {}
        for edge in edges:
            children.setdefault(edge.from_step, []).append(edge.to_step)
        seen: set = set()
        stack = [root]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(children.get(current, []))
        return seen

    async def cancel_run(self, run_id: str) -> bool:
        """Requests cancellation: kills live children, retires steps that never started."""
        now = utcnow()
        self._cancel_runs.add(run_id)
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, run_id)
            if not run or run.status in RUN_TERMINAL:
                return False
            steps = (
                await session.execute(select(StepRun).where(StepRun.run_id == run_id))
            ).scalars().all()
            for step_run in steps:
                if step_run.status in ("pending", "waiting_deps"):
                    step_run.status = "cancelled"
                    step_run.ended_at = now
                    step_run.error = "Cancelled before it started."
                elif step_run.status == "running":
                    proc = self._procs.get(step_run.id)
                    if proc is not None:
                        try:
                            proc.kill()
                        except OSError:
                            pass
            await session.commit()
        # Finalize right away so the status flips even when no tick is running
        # (tests, or a cancel during shutdown).
        await self._maybe_finalize(run_id)
        await self.wake()
        return True


    # ------------------------------------------------------- dependency advance

    async def _advance_active_runs(self) -> None:
        async with AsyncSessionLocal() as session:
            run_ids = (
                await session.execute(
                    select(PipelineRun.id)
                    .where(PipelineRun.status.in_(tuple(RUN_ACTIVE)))
                    .order_by(PipelineRun.scheduled_for.asc(), PipelineRun.started_at.asc())
                    .limit(50)
                )
            ).scalars().all()
        for run_id in run_ids:
            try:
                await self._advance_run(run_id)
            except Exception:
                logger.exception("Advancing pipeline run %s failed.", run_id)

    async def _advance_run(self, run_id: str) -> None:
        """Promotes a run to ``running`` and claims every step whose edges are met."""
        spawned: List[str] = []
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, run_id)
            if not run or run.status in RUN_TERMINAL:
                return
            pipeline = await session.get(Pipeline, run.pipeline_id)
            if not pipeline:
                run.status = "cancelled"
                run.error = "Pipeline was deleted."
                await session.commit()
                return

            if run.status == "waiting":
                others = (
                    await session.execute(
                        select(func.count())
                        .select_from(PipelineRun)
                        .where(
                            PipelineRun.pipeline_id == run.pipeline_id,
                            PipelineRun.id != run.id,
                            PipelineRun.status.in_(tuple(RUN_ACTIVE)),
                        )
                    )
                ).scalar_one()
                if others:
                    return
                run.status = "pending"
                await session.commit()

            if run.status == "pending":
                run.status = "running"
                run.started_at = run.started_at or utcnow()
                await session.commit()

            edges = (
                await session.execute(
                    select(PipelineEdge).where(PipelineEdge.pipeline_id == run.pipeline_id)
                )
            ).scalars().all()
            step_runs = (
                await session.execute(select(StepRun).where(StepRun.run_id == run.id))
            ).scalars().all()

            if not step_runs:
                run.status = "failed"
                run.ended_at = utcnow()
                run.error = "Pipeline has no steps."
                await session.commit()
                return

            by_step = {sr.step_id: sr for sr in step_runs}
            now = utcnow()
            running = [sr for sr in step_runs if sr.status == "running"]

            for step_run in step_runs:
                if step_run.status not in ("pending", "waiting_deps"):
                    continue
                if step_run.next_retry_at and as_utc(step_run.next_retry_at) > now:
                    continue
                decision = self._decide(edges, by_step, step_run.step_id)
                if decision == "wait":
                    step_run.status = "waiting_deps"
                    continue
                if decision == "skip":
                    step_run.status = "skipped"
                    step_run.ended_at = now
                    step_run.error = "No incoming edge condition was met."
                    continue
                if len(running) >= max(1, pipeline.max_parallel):
                    continue

                # The claim is the lock. Two engines racing here produce one winner:
                # SQLite serialises the UPDATE and the loser sees rowcount 0.
                result = await session.execute(
                    update(StepRun)
                    .where(
                        StepRun.id == step_run.id,
                        StepRun.status.in_(("pending", "waiting_deps")),
                    )
                    .values(status="running", claimed_at=now)
                )
                if result.rowcount != 1:
                    continue
                step_run.status = "running"
                step_run.claimed_at = now
                running.append(step_run)
                spawned.append(step_run.id)
                # Commit the claim immediately. Holding one write transaction across
                # the whole loop would block every other writer (log flushes, run
                # finalization) for up to busy_timeout, which turns a fast pipeline
                # into a stalled one.
                await session.commit()

            await session.commit()

        for step_run_id in spawned:
            task = asyncio.create_task(self._execute_step(step_run_id))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        await self._maybe_finalize(run_id)


    @staticmethod
    def _decide(edges, by_step: Dict[str, StepRun], step_id: str) -> str:
        """``ready`` | ``wait`` | ``skip`` for one step, given its upstreams.

        A step is ready when at least one incoming edge condition is met and no
        upstream is still running. All edges dead (upstream finished but none
        matched) means the step can never run, so it is skipped rather than left
        waiting forever.
        """
        incoming = [
            edge for edge in edges if edge.to_step == step_id and edge.from_step in by_step
        ]
        if not incoming:
            return "ready"
        states: List[str] = []
        for edge in incoming:
            upstream = by_step[edge.from_step]
            if upstream.status not in STEP_TERMINAL:
                states.append("pending")
            elif _cond_matches(edge.condition, upstream.status):
                states.append("match")
            else:
                states.append("dead")
        if "pending" in states:
            return "wait"
        if "match" in states:
            return "ready"
        return "skip"

    # ------------------------------------------------------------- finalizing

    async def _maybe_finalize(self, run_id: str) -> None:
        """Closes a run once every step is terminal, then records and prunes it."""
        status: Optional[str] = None
        pipeline_id: Optional[str] = None
        started_at: Optional[datetime] = None
        auto_rerun = False
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, run_id)
            if not run or run.status in RUN_TERMINAL:
                return
            pipeline = await session.get(Pipeline, run.pipeline_id)
            step_runs = (
                await session.execute(select(StepRun).where(StepRun.run_id == run_id))
            ).scalars().all()
            if any(sr.status not in STEP_TERMINAL for sr in step_runs):
                return

            counts: Dict[str, int] = {}
            for sr in step_runs:
                counts[sr.status] = counts.get(sr.status, 0) + 1

            if counts.get("failed"):
                status = "failed"
            elif counts.get("interrupted"):
                status = "interrupted"
            elif counts.get("cancelled"):
                status = "cancelled"
            else:
                status = "success"

            now = utcnow()
            run.status = status
            run.ended_at = now
            if run.started_at:
                run.duration_seconds = round((now - as_utc(run.started_at)).total_seconds(), 2)
            if status != "success":
                run.error = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()) if k != "success")
            if pipeline:
                pipeline.last_status = status
                pipeline.last_run_at = run.started_at or now
                auto_rerun = bool(pipeline.auto_rerun_interrupted) and status == "interrupted"
            pipeline_id = run.pipeline_id
            started_at = run.started_at or now
            await session.commit()

        if status is None or pipeline_id is None:
            return

        # Durable history: the same table ingest/dbt/deploy use, so the Overview
        # activity feed keeps one timeline. Written off-loop: this is a synchronous
        # sqlite3 call that retries on lock contention, and it must never freeze the
        # event loop (the UI is polling this same process).
        try:
            from app.services.state import task_history

            await asyncio.to_thread(
                task_history.record,
                "pipeline",
                run_id,
                status,
                started_at,
                {
                    "id": run_id,
                    "pipeline_id": pipeline_id,
                    "status": status,
                    "started_at": started_at.isoformat(),
                },
            )
        except Exception:  # history must never break orchestration
            logger.debug("Could not record pipeline run %s in task history.", run_id)

        await self._prune_runs(pipeline_id)
        self._cancel_runs.discard(run_id)

        if auto_rerun:
            try:
                await self.run_now(pipeline_id, triggered_by="auto-rerun", kind="rerun")
            except Exception:
                logger.exception("Auto-rerun of interrupted pipeline %s failed.", pipeline_id)

    async def _prune_runs(self, pipeline_id: str) -> None:
        """Keeps the newest ``keep_runs`` finished runs per pipeline."""
        async with AsyncSessionLocal() as session:
            pipeline = await session.get(Pipeline, pipeline_id)
            if not pipeline:
                return
            keep = max(1, pipeline.keep_runs)
            finished = (
                await session.execute(
                    select(PipelineRun.id)
                    .where(
                        PipelineRun.pipeline_id == pipeline_id,
                        PipelineRun.status.in_(tuple(RUN_TERMINAL)),
                    )
                    .order_by(PipelineRun.id)
                )
            ).scalars().all()
            if len(finished) <= keep:
                return
            # Order by time, not id: id is a uuid. Re-select with the time key.
            ordered = (
                await session.execute(
                    select(PipelineRun.id)
                    .where(
                        PipelineRun.pipeline_id == pipeline_id,
                        PipelineRun.status.in_(tuple(RUN_TERMINAL)),
                    )
                    .order_by(
                        func.coalesce(
                            PipelineRun.ended_at, PipelineRun.started_at, PipelineRun.scheduled_for
                        ).desc()
                    )
                    .limit(keep)
                )
            ).scalars().all()
            stale = [rid for rid in finished if rid not in set(ordered)]
            if stale:
                await session.execute(
                    delete(PipelineRun).where(PipelineRun.id.in_(stale))
                )
                await session.commit()


    # -------------------------------------------------------------- execution

    async def _execute_step(self, step_run_id: str) -> None:
        """Runs one claimed step to a terminal state, honouring timeout and retries."""
        async with AsyncSessionLocal() as session:
            step_run = await session.get(StepRun, step_run_id)
            if not step_run or step_run.status != "running":
                return
            run = await session.get(PipelineRun, step_run.run_id)
            if not run:
                return
            pipeline = await session.get(Pipeline, run.pipeline_id)
            step = await session.get(PipelineStep, step_run.step_id)
            if not pipeline or not step:
                step_run.status = "failed"
                step_run.error = "Step or pipeline was deleted before it ran."
                await session.commit()
                await self._advance_run(run.id)
                return
            # started_at is written here — after the claim is ours — so boot healing
            # can tell "never spawned" from "was executing".
            step_run.started_at = utcnow()
            params = json.loads(run.params_json or "{}")
            prior_logs = step_run.logs or ""
            await session.commit()

        # Attempts append: the log of attempt 1 survives attempt 2, which is the only
        # way a retry can be diagnosed after the fact.
        log: List[str] = [prior_logs] if prior_logs else []
        timeout = int(step.timeout_seconds or pipeline.timeout_seconds or 3600)
        log.append(
            f"[{utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] Step '{step.name}' "
            f"({step.step_type}) attempt {step_run.attempt} — target: {step.target or '(none)'}"
        )
        status = "success"
        error: Optional[str] = None
        started = time.time()

        try:
            if run.id in self._cancel_runs:
                # Cancelled between the claim and this task starting: the child must
                # never be spawned at all.
                raise StepCancelled()
            await self._dispatch(step, params, pipeline, step_run.id, run.id, log, timeout)
        except StepCancelled:
            status = "cancelled"
            log.append("Cancelled before the child produced a result.")
        except asyncio.TimeoutError:
            status = "failed"
            error = f"Timed out after {timeout}s."
            log.append(f"ERROR: {error}")
        except Exception as exc:
            status = "failed"
            error = str(exc) or exc.__class__.__name__
            log.append(f"ERROR: {error}")
            log.append(traceback.format_exc())
        finally:
            self._procs.pop(step_run_id, None)

        duration = round(time.time() - started, 2)
        cancelled = run.id in self._cancel_runs
        if cancelled and status != "success":
            status = "cancelled"
            error = None
            log.append("Cancelled by operator.")
        log.append(f"Step finished: {status} ({duration}s)")

        retry_cap = step.max_retries if step.max_retries is not None else pipeline.max_retries
        retry_delay = (
            step.retry_delay_seconds
            if step.retry_delay_seconds is not None
            else pipeline.retry_delay_seconds
        )

        async with AsyncSessionLocal() as session:
            row = await session.get(StepRun, step_run_id)
            if not row:
                return
            row.logs = self._cap_logs("\n".join(log))
            if status == "failed" and row.attempt <= retry_cap and not cancelled:
                row.status = "pending"
                row.attempt = row.attempt + 1
                row.claimed_at = None
                row.started_at = None
                row.ended_at = None
                row.duration_seconds = None
                row.next_retry_at = utcnow() + timedelta(seconds=max(0, retry_delay))
                row.error = f"{error} (retrying in {retry_delay}s)"
                row.logs = self._cap_logs(
                    row.logs + f"\nRetry scheduled in {retry_delay}s (attempt {row.attempt})."
                )
            else:
                row.status = status
                row.ended_at = utcnow()
                row.duration_seconds = duration
                row.error = error
                row.next_retry_at = None
            await session.commit()
            run_id = row.run_id

        self._flushed.pop(step_run_id, None)
        # Promote dependents immediately instead of waiting for the next tick.
        await self._advance_run(run_id)


    async def _dispatch(
        self,
        step: PipelineStep,
        params: Dict[str, Any],
        pipeline: Pipeline,
        step_run_id: str,
        run_id: str,
        log: List[str],
        timeout: int,
    ) -> None:
        args = json.loads(step.args_json) if step.args_json else {}
        if step.step_type in ("dbt_run", "dbt_test"):
            await self._run_dbt(step, params, log, timeout)
        elif step.step_type == "script":
            await self._run_python_file(
                self.scripts_dir, step.target, args, params, step_run_id, run_id,
                log, timeout, "script"
            )
        elif step.step_type == "notebook":
            await self._run_python_file(
                self.notebooks_dir, step.target, args, params, step_run_id, run_id,
                log, timeout, "notebook"
            )
        elif step.step_type == "webhook":
            await self._run_webhook(step, args, params, log, timeout)
        elif step.step_type == "raw_sql":
            await self._run_raw_sql(step, params, log, timeout)
        else:
            raise ValueError(f"Unsupported step type '{step.step_type}'.")

    async def _run_dbt(
        self, step: PipelineStep, params: Dict[str, Any], log: List[str], timeout: int
    ) -> None:
        from app.engines.dbt_runner import dbt_runner

        command = "test" if step.step_type == "dbt_test" else "run"
        # "*" means "the whole project", and dbt does not read it that way: a bare
        # asterisk is treated as a path glob and matches project files instead of
        # models, so the run reports "Nothing to do" and exits 0. Omit --select for
        # the wildcard, exactly like the Studio's scoped runs do.
        selector = (step.target or "").strip()
        if selector in ("", "*"):
            selector = None
        log.append(f"$ dbt {command}" + (f" --select {selector}" if selector else " (whole project)"))
        dbt_vars = {**dbt_runner._dbt_vars_payload(), **params}
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    dbt_runner.run_dbt,
                    command=command,
                    select=selector,
                    dbt_vars=dbt_vars,
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            # A thread cannot be killed; dbt's own subprocess finishes on its own.
            # The step is still terminal and honest about the timeout.
            log.append(f"dbt exceeded {timeout}s; abandoning the wait.")
            raise
        output = (result or {}).get("output", "")
        if output:
            log.append(output)
        if not (result or {}).get("success"):
            raise RuntimeError(
                f"dbt {command} failed with exit code {(result or {}).get('exit_code')}."
            )

    async def _run_python_file(
        self,
        root: Path,
        target: str,
        args: Dict[str, Any],
        params: Dict[str, Any],
        step_run_id: str,
        run_id: str,
        log: List[str],
        timeout: int,
        label: str,
    ) -> None:
        if not target:
            raise ValueError(f"{label.capitalize()} step has no target file.")
        path = root / target
        if not path.exists():
            path = root / f"{target}.py"
        if not path.exists():
            raise FileNotFoundError(f"{label.capitalize()} '{target}' not found in {root}.")

        env = self._child_env(params)
        cmd = [_interpreter_path(), "-u", str(path), *[str(a) for a in args.get("args", [])]]
        log.append(f"$ {' '.join(cmd)}")
        code = await self._run_child(cmd, env, step_run_id, run_id, log, timeout)
        if code != 0:
            raise RuntimeError(f"{label.capitalize()} exited with code {code}.")


    async def _run_webhook(
        self,
        step: PipelineStep,
        args: Dict[str, Any],
        params: Dict[str, Any],
        log: List[str],
        timeout: int,
    ) -> None:
        import httpx

        if not step.target:
            raise ValueError("Webhook step has no URL.")
        method = str(args.get("method", "POST")).upper()

        def render(value: Any) -> Any:
            if isinstance(value, str):
                for key, val in params.items():
                    value = value.replace("{" + key + "}", "" if val is None else str(val))
                return value
            if isinstance(value, dict):
                return {k: render(v) for k, v in value.items()}
            if isinstance(value, list):
                return [render(v) for v in value]
            return value

        body = render(args.get("body"))
        headers = render(args.get("headers") or {})
        log.append(f"$ {method} {step.target}")
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, step.target, json=body, headers=headers)
        log.append(f"HTTP {response.status_code}: {response.text[:2000]}")
        if response.status_code >= 400:
            raise RuntimeError(f"Webhook returned HTTP {response.status_code}.")

    async def _run_raw_sql(
        self, step: PipelineStep, params: Dict[str, Any], log: List[str], timeout: int
    ) -> None:
        from app.engines.duckdb_engine import duckdb_engine

        sql = render_sql_params(step.target or "", params).strip()
        if not sql:
            raise ValueError("Raw SQL step has no SQL.")
        log.append(f"$ {sql[:500]}")
        result = await asyncio.wait_for(
            asyncio.to_thread(duckdb_engine.execute_query, sql), timeout=timeout
        )
        rows = (result or {}).get("row_count", 0)
        columns = (result or {}).get("columns", [])
        log.append(f"Query OK: {rows} row(s), columns={columns}")
        for row in (result or {}).get("rows", [])[:25]:
            log.append(str(row))

    # ---------------------------------------------------------- child process

    async def _run_child(
        self,
        cmd: List[str],
        env: Dict[str, str],
        step_run_id: str,
        run_id: str,
        log: List[str],
        timeout: int,
    ) -> int:
        """Streams a child's output into ``log`` and flushes it every second.

        Output is drained by a daemon thread into a queue rather than by awaiting
        ``readline``: a child that prints nothing for ten minutes must still hit its
        timeout, and blocking reads cannot deliver that on Windows.
        """
        import queue
        import threading

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            # Decode as UTF-8 regardless of the Windows console code page; the child
            # is also told to write UTF-8 (see _child_env), so what a script prints is
            # what the log shows instead of a charmap error.
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
            cwd=str(self.workspace_root),
        )
        self._procs[step_run_id] = proc
        lines: "queue.Queue[Optional[str]]" = queue.Queue()

        def _pump() -> None:
            try:
                assert proc.stdout is not None
                for line in proc.stdout:
                    lines.put(line)
            finally:
                lines.put(None)

        threading.Thread(target=_pump, daemon=True).start()

        deadline = time.monotonic() + max(1, timeout)
        last_flush = time.monotonic()
        finished = False
        try:
            while not finished:
                try:
                    while True:
                        item = lines.get_nowait()
                        if item is None:
                            finished = True
                            break
                        log.append(item.rstrip("\n"))
                except queue.Empty:
                    pass

                if time.monotonic() > deadline and not finished:
                    try:
                        proc.kill()
                    except OSError:
                        pass
                    raise asyncio.TimeoutError

                if run_id in self._cancel_runs and not finished:
                    # Kill the child as soon as the operator asks, instead of letting
                    # it run to completion and only then reporting "cancelled".
                    try:
                        proc.kill()
                    except OSError:
                        pass
                    raise StepCancelled()

                if time.monotonic() - last_flush >= 1.0:
                    await self._flush_logs(step_run_id, log)
                    last_flush = time.monotonic()

                if not finished:
                    await asyncio.sleep(0.2)

            # No Popen.wait(): on Windows it can block on a pipe whose reader
            # already saw EOF, freezing the event loop. EOF means the child closed
            # its output, so poll() — which never blocks — is the right question.
            for _ in range(100):
                if proc.poll() is not None:
                    break
                await asyncio.sleep(0.05)
            return int(proc.returncode or 0)
        finally:
            self._procs.pop(step_run_id, None)
            await self._flush_logs(step_run_id, log)

    async def _flush_logs(self, step_run_id: str, log: List[str]) -> None:
        """Writes the accumulated log to the step row (tail-capped) mid-run.

        Skips the write when nothing changed: an idle step must not compete for the
        single SQLite writer every second.
        """
        text = self._cap_logs("\n".join(log))
        if self._flushed.get(step_run_id) == len(text):
            return
        try:
            async with AsyncSessionLocal() as session:
                row = await session.get(StepRun, step_run_id)
                if not row or row.status != "running":
                    return
                row.logs = text
                await session.commit()
            self._flushed[step_run_id] = len(text)
        except Exception:
            # A failed flush must never kill the step; the final write will retry.
            logger.debug("Log flush failed for step_run %s", step_run_id)

    @staticmethod
    def _cap_logs(text: str) -> str:
        if len(text) <= MAX_LOG_CHARS:
            return text
        return "…[earlier output truncated]…\n" + text[-MAX_LOG_CHARS:]

    def _child_env(self, params: Dict[str, Any]) -> Dict[str, str]:
        env = os.environ.copy()
        backend_dir = str(self.workspace_root / "backend")
        # The notebooks directory carries the `obelisk` SDK shim that notebooks and
        # scripts import; without it `import obelisk` only works from inside that
        # folder, and a script step fails with ModuleNotFoundError.
        notebooks_dir = str(self.notebooks_dir)
        existing = env.get("PYTHONPATH", "")
        parts = [backend_dir, str(self.workspace_root), notebooks_dir]
        if existing:
            parts.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(parts)
        # Children inherit the Windows console code page by default, so any non-ASCII
        # output (a Polars table, a ✓ in a progress line) dies with UnicodeEncodeError.
        # Force UTF-8 end to end; the reader decodes UTF-8 to match.
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        for name, value in params.items():
            env[f"OBELISK_PARAM_{name.upper()}"] = "" if value is None else str(value)
        return env


pipeline_engine = PipelineEngine()

