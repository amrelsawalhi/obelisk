"""Crash recovery, verified by actually killing the backend.

The durability contract in the plan says: kill the process mid-run, start it again,
and the run must come back as ``interrupted`` with its logs intact. That cannot be
tested with an in-process engine — the process is what dies — so this test spawns
uvicorn as a real subprocess, hard-kills it, and boots a second one against the same
SQLite file.

``kill()`` on the Popen maps to TerminateProcess on Windows and SIGKILL on POSIX:
the harshest case, where no shutdown hook runs.
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.pipeline import Pipeline, PipelineStep

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BACKEND_DIR.parent
SCRIPTS_DIR = PROJECT_ROOT / "git_repo" / "scripts"
SCRIPT_NAME = "zz_crash_slow.py"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _backend_env() -> dict:
    import os

    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{settings.sqlite_db_path}"
    env["STORAGE_URI"] = settings.STORAGE_URI
    env["PYTHONPATH"] = str(BACKEND_DIR)
    return env


def _start_backend(port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=str(BACKEND_DIR),
        env=_backend_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_healthy(port: int, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"http://127.0.0.1:{port}/health", timeout=2.0)
            if response.status_code == 200:
                return
        except Exception as exc:  # not up yet
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"backend did not become healthy on port {port}: {last_error}")


def _stop(proc: subprocess.Popen) -> None:
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.wait(timeout=20)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_hard_kill_marks_running_step_interrupted_and_keeps_logs():
    script = SCRIPTS_DIR / SCRIPT_NAME
    script.write_text(
        "import time\nprint('crash:start', flush=True)\n"
        "for i in range(240):\n    time.sleep(0.25)\n"
        "    print(f'crash:tick {i}', flush=True)\n",
        encoding="utf-8",
    )

    pipeline_id = str(uuid.uuid4())
    step_id = str(uuid.uuid4())
    async with AsyncSessionLocal() as session:
        session.add(Pipeline(id=pipeline_id, name=f"crash-{uuid.uuid4().hex[:8]}"))
        await session.flush()
        session.add(
            PipelineStep(
                id=step_id,
                pipeline_id=pipeline_id,
                name="slow",
                step_type="script",
                target=SCRIPT_NAME[:-3],
                timeout_seconds=600,
            )
        )
        await session.commit()

    port = _free_port()
    first = _start_backend(port)
    try:
        _wait_healthy(port)
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0) as client:
            queued = client.post(f"/api/pipelines/{pipeline_id}/run", json={})
            assert queued.status_code == 202, queued.text
            run_id = queued.json()["run_id"]

            # Wait until the child is genuinely running and has flushed some output.
            deadline = time.monotonic() + 60
            logs = ""
            while time.monotonic() < deadline:
                detail = client.get(f"/api/pipeline-runs/{run_id}").json()
                if detail["step_runs"]:
                    step_run = detail["step_runs"][0]
                    if step_run["status"] == "running":
                        tail = client.get(f"/api/step-runs/{step_run['id']}/logs").json()
                        logs = tail["logs"]
                        if "crash:tick" in logs:
                            break
                time.sleep(0.5)
            assert "crash:tick" in logs, "incremental log flushing did not happen"

        # Hard kill: no shutdown hook runs.
        first.kill()
        first.wait(timeout=20)

        # Boot a second backend against the same brain.
        second = _start_backend(port)
        try:
            _wait_healthy(port)
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0) as client:
                status = None
                detail = {}
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    detail = client.get(f"/api/pipeline-runs/{run_id}").json()
                    status = detail["status"]
                    if status in ("interrupted", "failed", "success", "cancelled"):
                        break
                    time.sleep(0.5)

                assert status == "interrupted", f"run came back as {status}"
                step_run = detail["step_runs"][0]
                assert step_run["status"] == "interrupted"

                # The tail survived the kill.
                tail = client.get(f"/api/step-runs/{step_run['id']}/logs").json()
                assert "crash:start" in tail["logs"]
                assert "Interrupted by an Obelisk restart" in tail["logs"]
        finally:
            _stop(second)
    finally:
        _stop(first)
        script.unlink(missing_ok=True)
        async with AsyncSessionLocal() as session:
            pipeline = await session.get(Pipeline, pipeline_id)
            if pipeline:
                await session.delete(pipeline)
                await session.commit()

