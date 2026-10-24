"""The pipeline orchestrator, exercised for real.

No mocks: every test drives the engine against a throwaway SQLite brain and real
child processes from ``git_repo/scripts``. That is the only way the claims that
matter here can be believed — claims like "a failure edge fires", "a claimed row
cannot be claimed twice", and "healing marks what was running as interrupted".

The engine is started by hand (``pipeline_engine.start()``) instead of through the
app lifespan, so a test can advance the scheduler deterministically with ``wake()``
rather than waiting for the five-second tick.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.engines.pipeline_engine import pipeline_engine
from app.models.pipeline import (
    Pipeline,
    PipelineEdge,
    PipelineParam,
    PipelineRun,
    PipelineStep,
    StepRun,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "git_repo" / "scripts"
NOTEBOOKS_DIR = PROJECT_ROOT / "git_repo" / "notebooks"


@pytest.fixture(autouse=True)
async def engine_running():
    """Starts the real engine for each test (cheap) and stops it afterwards.

    Function scope, not module: a module-scoped fixture is created outside the
    event loop, and ``start()`` needs a running loop to create its tick task.
    """
    pipeline_engine.start()
    yield
    await pipeline_engine.stop()


@pytest.fixture
def demo_scripts():
    """Writes the demo scripts the tests execute, and removes them after."""
    bodies = {
        "zz_test_echo.py": (
            "import os\n"
            "print('echo:start')\n"
            "for key in sorted(k for k in os.environ if k.startswith('OBELISK_PARAM_')):\n"
            "    print(f'{key}={os.environ[key]}')\n"
            "print('echo:done')\n"
        ),
        "zz_test_fail.py": (
            "import sys\n"
            "print('fail:start')\n"
            "print('fail:about to exit 3', file=sys.stderr)\n"
            "sys.exit(3)\n"
        ),
        "zz_test_slow.py": (
            "import os, time\n"
            "seconds = float(os.environ.get('OBELISK_PARAM_SECONDS', '30'))\n"
            "print('slow:start', flush=True)\n"
            "for i in range(int(seconds * 2)):\n"
            "    time.sleep(0.5)\n"
            "    print(f'slow:tick {i}', flush=True)\n"
            "print('slow:done', flush=True)\n"
        ),
    }
    paths = []
    for name, body in bodies.items():
        path = SCRIPTS_DIR / name
        path.write_text(body, encoding="utf-8")
        paths.append(path)
    yield
    for path in paths:
        path.unlink(missing_ok=True)


def _uuid() -> str:
    return str(uuid.uuid4())


async def _create_pipeline(
    steps: list,
    edges: list = None,
    *,
    params: list = None,
    **settings,
) -> dict:
    """Creates a pipeline + graph directly in the brain, returning ids by name."""
    pipeline_id = _uuid()
    ids = {}
    async with AsyncSessionLocal() as session:
        pipeline = Pipeline(id=pipeline_id, name=f"test-{uuid.uuid4().hex[:8]}", **settings)
        session.add(pipeline)
        await session.flush()
        for step in steps:
            step_id = _uuid()
            ids[step["name"]] = step_id
            session.add(
                PipelineStep(
                    id=step_id,
                    pipeline_id=pipeline_id,
                    name=step["name"],
                    step_type=step["step_type"],
                    target=step.get("target", ""),
                    args_json=json.dumps(step["args"]) if step.get("args") else None,
                    timeout_seconds=step.get("timeout_seconds"),
                    max_retries=step.get("max_retries"),
                    retry_delay_seconds=step.get("retry_delay_seconds"),
                )
            )
        await session.flush()
        for edge in edges or []:
            session.add(
                PipelineEdge(
                    pipeline_id=pipeline_id,
                    from_step=ids[edge["from"]],
                    to_step=ids[edge["to"]],
                    condition=edge.get("condition", "success"),
                )
            )
        for param in params or []:
            session.add(
                PipelineParam(
                    pipeline_id=pipeline_id,
                    name=param["name"],
                    type=param.get("type", "string"),
                    default_value=param.get("default_value"),
                    required=param.get("required", False),
                )
            )
        await session.commit()
    return {"pipeline_id": pipeline_id, "steps": ids}


async def _run_and_wait(pipeline_id: str, params: dict = None, timeout: float = 90) -> dict:
    """Runs a pipeline and drives the engine until the run reaches a terminal state."""
    run_id = await pipeline_engine.run_now(pipeline_id, params=params)
    terminal = ("success", "failed", "cancelled", "interrupted", "skipped")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await pipeline_engine.wake()
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, run_id)
            if run and run.status in terminal:
                step_runs = (
                    await session.execute(select(StepRun).where(StepRun.run_id == run_id))
                ).scalars().all()
                steps = (
                    await session.execute(
                        select(PipelineStep).where(PipelineStep.pipeline_id == pipeline_id)
                    )
                ).scalars().all()
                names = {s.id: s.name for s in steps}
                return {
                    "id": run.id,
                    "status": run.status,
                    "error": run.error,
                    "steps": {
                        names.get(sr.step_id, sr.step_id): {
                            "id": sr.id,
                            "status": sr.status,
                            "attempt": sr.attempt,
                            "logs": sr.logs or "",
                            "error": sr.error,
                        }
                        for sr in step_runs
                    },
                }
        await asyncio.sleep(0.25)
    raise AssertionError(f"run {run_id} did not finish within {timeout}s")


# ------------------------------------------------------------------- execution


@pytest.mark.asyncio
async def test_success_chain_runs_in_dependency_order(demo_scripts):
    """A → B (success) executes both, and each keeps its own log."""
    graph = await _create_pipeline(
        [
            {"name": "echo", "step_type": "script", "target": "zz_test_echo"},
            {"name": "slow", "step_type": "script", "target": "zz_test_slow"},
        ],
        [{"from": "echo", "to": "slow"}],
        params=[{"name": "seconds", "type": "float", "default_value": "1"}],
    )

    result = await _run_and_wait(graph["pipeline_id"])

    assert result["status"] == "success"
    assert result["steps"]["echo"]["status"] == "success"
    assert result["steps"]["slow"]["status"] == "success"
    assert "echo:done" in result["steps"]["echo"]["logs"]
    assert "slow:done" in result["steps"]["slow"]["logs"]
    # Parameters reach the child as env vars, uppercased.
    assert "OBELISK_PARAM_SECONDS=1.0" in result["steps"]["echo"]["logs"]


@pytest.mark.asyncio
async def test_failure_edge_fires_and_success_edge_is_skipped(demo_scripts):
    """The conditional matrix, through a real failing process."""
    graph = await _create_pipeline(
        [
            {"name": "fails", "step_type": "script", "target": "zz_test_fail"},
            {"name": "on_success", "step_type": "script", "target": "zz_test_echo"},
            {"name": "on_failure", "step_type": "script", "target": "zz_test_echo"},
        ],
        [
            {"from": "fails", "to": "on_success", "condition": "success"},
            {"from": "fails", "to": "on_failure", "condition": "failure"},
        ],
    )

    result = await _run_and_wait(graph["pipeline_id"])

    assert result["status"] == "failed"
    assert result["steps"]["fails"]["status"] == "failed"
    assert result["steps"]["on_success"]["status"] == "skipped"
    assert result["steps"]["on_failure"]["status"] == "success"
    assert "exit 3" in result["steps"]["fails"]["logs"]


@pytest.mark.asyncio
async def test_completion_edge_runs_after_failure(demo_scripts):
    graph = await _create_pipeline(
        [
            {"name": "fails", "step_type": "script", "target": "zz_test_fail"},
            {"name": "always", "step_type": "script", "target": "zz_test_echo"},
        ],
        [{"from": "fails", "to": "always", "condition": "completion"}],
    )

    result = await _run_and_wait(graph["pipeline_id"])

    assert result["steps"]["always"]["status"] == "success"



@pytest.mark.asyncio
async def test_retry_reuses_the_same_step_run(demo_scripts):
    """A retried step keeps one row: attempts grow, history does not fork."""
    graph = await _create_pipeline(
        [
            {
                "name": "flaky",
                "step_type": "script",
                "target": "zz_test_fail",
                "max_retries": 1,
                "retry_delay_seconds": 0,
            }
        ]
    )

    result = await _run_and_wait(graph["pipeline_id"])

    assert result["status"] == "failed"
    assert result["steps"]["flaky"]["attempt"] == 2
    assert "Retry scheduled" in result["steps"]["flaky"]["logs"]


@pytest.mark.asyncio
async def test_raw_sql_step_executes_in_duckdb():
    graph = await _create_pipeline(
        [
            {
                "name": "sql",
                "step_type": "raw_sql",
                "target": "SELECT {{n}} AS n, 'ok' AS status",
            }
        ],
        params=[{"name": "n", "type": "int", "default_value": "7"}],
    )

    result = await _run_and_wait(graph["pipeline_id"])

    assert result["status"] == "success"
    assert "Query OK" in result["steps"]["sql"]["logs"]
    assert "'n': 7" in result["steps"]["sql"]["logs"]


@pytest.mark.asyncio
async def test_webhook_step_hits_a_real_server():
    """A real localhost HTTP server, not a mock — the body is rendered from params."""
    import http.server
    import threading

    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 - stdlib naming
            length = int(self.headers.get("Content-Length", "0"))
            received.append(self.rfile.read(length))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_args):
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        graph = await _create_pipeline(
            [
                {
                    "name": "hook",
                    "step_type": "webhook",
                    "target": f"http://127.0.0.1:{port}/notify",
                    "args": {"body": {"message": "run {label}"}},
                }
            ],
            params=[{"name": "label", "type": "string", "default_value": "nightly"}],
        )
        result = await _run_and_wait(graph["pipeline_id"])
    finally:
        server.shutdown()
        server.server_close()

    assert result["status"] == "success"
    assert received and b"run nightly" in received[0]


@pytest.mark.asyncio
async def test_notebook_step_runs_headlessly():
    """Notebooks are plain Python; the step runs the file, not a marimo server."""
    path = NOTEBOOKS_DIR / "zz_test_notebook.py"
    path.write_text("print('notebook:hello')\n", encoding="utf-8")
    try:
        graph = await _create_pipeline(
            [{"name": "nb", "step_type": "notebook", "target": "zz_test_notebook"}]
        )
        result = await _run_and_wait(graph["pipeline_id"])
    finally:
        path.unlink(missing_ok=True)

    assert result["status"] == "success"
    assert "notebook:hello" in result["steps"]["nb"]["logs"]



# ------------------------------------------------------- scheduling & recovery


async def _set_schedule(pipeline_id: str, **fields) -> None:
    async with AsyncSessionLocal() as session:
        pipeline = await session.get(Pipeline, pipeline_id)
        for key, value in fields.items():
            setattr(pipeline, key, value)
        await session.commit()


@pytest.mark.asyncio
async def test_schedule_creates_exactly_one_catchup_run(demo_scripts):
    """A missed occurrence runs once, and the anchor advances from the occurrence."""
    from datetime import datetime, timedelta, timezone

    graph = await _create_pipeline(
        [{"name": "echo", "step_type": "script", "target": "zz_test_echo"}],
        is_enabled=True,
        cron_expression="* * * * *",
    )
    missed = datetime.now(timezone.utc) - timedelta(hours=3)
    await _set_schedule(graph["pipeline_id"], next_run_at=missed)

    await pipeline_engine._create_scheduled_runs()
    await pipeline_engine._create_scheduled_runs()

    async with AsyncSessionLocal() as session:
        runs = (
            await session.execute(
                select(PipelineRun).where(PipelineRun.pipeline_id == graph["pipeline_id"])
            )
        ).scalars().all()
        pipeline = await session.get(Pipeline, graph["pipeline_id"])

    assert len(runs) == 1, "a missed schedule must produce one run, not a backfill storm"
    assert runs[0].kind == "scheduled"
    assert runs[0].scheduled_for is not None
    # Anchor moved forward from the missed occurrence, so it is now in the future.
    assert pipeline.next_run_at is not None
    assert pipeline.next_run_at.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_overlap_skip_policy_records_the_skipped_occurrence(demo_scripts):
    from datetime import datetime, timedelta, timezone

    graph = await _create_pipeline(
        [{"name": "slow", "step_type": "script", "target": "zz_test_slow"}],
        is_enabled=True,
        cron_expression="* * * * *",
        overlap_policy="skip",
    )
    # An unfinished run blocks the occurrence. It carries a running step so the
    # scheduler treats it as genuinely active rather than "a run with no steps".
    async with AsyncSessionLocal() as session:
        blocking = PipelineRun(
            pipeline_id=graph["pipeline_id"],
            kind="manual",
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        session.add(blocking)
        await session.flush()
        session.add(
            StepRun(
                run_id=blocking.id,
                step_id=graph["steps"]["slow"],
                status="running",
                claimed_at=datetime.now(timezone.utc),
                started_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()
    await _set_schedule(
        graph["pipeline_id"],
        next_run_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )

    await pipeline_engine._create_scheduled_runs()

    async with AsyncSessionLocal() as session:
        runs = (
            await session.execute(
                select(PipelineRun).where(PipelineRun.pipeline_id == graph["pipeline_id"])
            )
        ).scalars().all()
    statuses = sorted(r.status for r in runs)
    assert "skipped" in statuses
    assert "pending" not in statuses



@pytest.mark.asyncio
async def test_boot_healing_distinguishes_claimed_from_started(demo_scripts):
    """A child that never spawned is re-queued; one that did is marked interrupted."""
    from datetime import datetime, timezone

    graph = await _create_pipeline(
        [
            {"name": "never_started", "step_type": "script", "target": "zz_test_echo"},
            {"name": "was_running", "step_type": "script", "target": "zz_test_echo"},
        ]
    )
    async with AsyncSessionLocal() as session:
        run = PipelineRun(
            pipeline_id=graph["pipeline_id"],
            kind="manual",
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        session.add(run)
        await session.flush()
        session.add(
            StepRun(
                run_id=run.id,
                step_id=graph["steps"]["never_started"],
                status="running",
                claimed_at=datetime.now(timezone.utc),
                started_at=None,
            )
        )
        session.add(
            StepRun(
                run_id=run.id,
                step_id=graph["steps"]["was_running"],
                status="running",
                claimed_at=datetime.now(timezone.utc),
                started_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()
        run_id = run.id

    await pipeline_engine.heal_on_start()

    async with AsyncSessionLocal() as session:
        step_runs = (
            await session.execute(select(StepRun).where(StepRun.run_id == run_id))
        ).scalars().all()
    by_step = {sr.step_id: sr for sr in step_runs}
    requeued = by_step[graph["steps"]["never_started"]]
    interrupted = by_step[graph["steps"]["was_running"]]
    assert requeued.status == "pending"
    assert interrupted.status == "interrupted"
    assert "Interrupted by an Obelisk restart" in (interrupted.logs or "")


@pytest.mark.asyncio
async def test_cancel_stops_a_running_step_quickly(demo_scripts):
    graph = await _create_pipeline(
        [{"name": "slow", "step_type": "script", "target": "zz_test_slow"}],
        params=[{"name": "seconds", "type": "float", "default_value": "30"}],
    )
    run_id = await pipeline_engine.run_now(graph["pipeline_id"])

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        await pipeline_engine.wake()
        async with AsyncSessionLocal() as session:
            step_runs = (
                await session.execute(select(StepRun).where(StepRun.run_id == run_id))
            ).scalars().all()
            if step_runs and step_runs[0].status == "running":
                break
        await asyncio.sleep(0.2)

    assert await pipeline_engine.cancel_run(run_id)
    cancelled_at = time.monotonic()
    status = None
    while time.monotonic() - cancelled_at < 15:
        await pipeline_engine.wake()
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, run_id)
            status = run.status
            if status == "cancelled":
                break
        await asyncio.sleep(0.2)

    assert status == "cancelled", f"run ended as {status}, not cancelled"
    # The child is killed, not waited out: a 30-second script must stop in seconds.
    assert time.monotonic() - cancelled_at < 15


@pytest.mark.asyncio
async def test_rerun_from_step_contains_only_the_subtree(demo_scripts):
    graph = await _create_pipeline(
        [
            {"name": "root", "step_type": "script", "target": "zz_test_echo"},
            {"name": "middle", "step_type": "script", "target": "zz_test_echo"},
            {"name": "leaf", "step_type": "script", "target": "zz_test_echo"},
        ],
        [
            {"from": "root", "to": "middle"},
            {"from": "middle", "to": "leaf"},
        ],
    )
    first = await _run_and_wait(graph["pipeline_id"])
    assert first["status"] == "success"

    new_run_id = await pipeline_engine.rerun_from(first["id"], graph["steps"]["middle"])
    deadline = time.monotonic() + 60
    step_runs = []
    status = None
    while time.monotonic() < deadline:
        await pipeline_engine.wake()
        async with AsyncSessionLocal() as session:
            run = await session.get(PipelineRun, new_run_id)
            status = run.status
            if status in ("success", "failed", "cancelled", "interrupted"):
                step_runs = (
                    await session.execute(
                        select(StepRun).where(StepRun.run_id == new_run_id)
                    )
                ).scalars().all()
                break
        await asyncio.sleep(0.25)

    assert status == "success"
    assert {sr.step_id for sr in step_runs} == {
        graph["steps"]["middle"],
        graph["steps"]["leaf"],
    }

