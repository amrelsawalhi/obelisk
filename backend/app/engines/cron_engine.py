"""
Obelisk Cron Engine:
Lightweight, in-process, async cron scheduler.
Executes dbt models, standalone Python scripts (git_repo/scripts/), and Marimo notebooks on schedule.
Persists job definitions and execution history directly in SQLite (data/obelisk.db).
"""

import os
import sys
import time
import asyncio
import subprocess
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List
from croniter import croniter
from sqlalchemy import select, update
from app.database import AsyncSessionLocal
from app.models.cron_job import CronJob
from app.engines.dbt_runner import dbt_runner

# Hard cap for a single job's child process. A wedged child used to leave its job
# stuck in 'running' forever, and the poll loop permanently skips such jobs.
JOB_TIMEOUT_SECONDS = 3600


def _interpreter_path() -> str:
    """Returns the interpreter that owns this process's installed packages.

    `sys.executable` is not reliable here: uvicorn's reloader spawns its worker
    from the *base* interpreter (multiprocessing uses `sys._base_executable`), so
    children launched with `sys.executable` lose the venv's site-packages and fail
    on `import marimo`/`import pydantic_settings`. Locate the venv that provides
    the packages we are currently running with via its `pyvenv.cfg`.
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



class CronEngine:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # In-flight job tasks and their live children, so stop() can cancel the
        # tasks and kill the subprocesses instead of orphaning them.
        self._tasks: set = set()
        self._active_procs: set = set()
        root = Path(__file__).resolve().parent.parent.parent.parent
        self.workspace_root = root
        self.scripts_dir = (root / "git_repo" / "scripts").resolve()
        self.notebooks_dir = (root / "git_repo" / "notebooks").resolve()
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def compute_next_run(cron_expr: str, base_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculates next execution datetime from standard cron expression."""
        try:
            base = base_time or datetime.now(timezone.utc)
            iter_val = croniter(cron_expr, base)
            next_ts = iter_val.get_next(float)
            return datetime.fromtimestamp(next_ts, tz=timezone.utc)
        except Exception as e:
            print(f"[CronEngine] Invalid cron expression '{cron_expr}': {e}")
            return None

    def start(self):
        """Starts the background cron monitoring loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        print("[CronEngine] In-process cron scheduler started.")

    def stop(self):
        """Stops the background cron monitoring loop and any in-flight jobs."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

        # Cancel in-flight jobs and kill their children; otherwise a job running
        # at shutdown keeps its subprocess alive after the backend is gone.
        for task in list(self._tasks):
            task.cancel()
        self._tasks.clear()
        for proc in list(self._active_procs):
            try:
                proc.kill()
            except OSError:
                pass
        self._active_procs.clear()

        print("[CronEngine] In-process cron scheduler stopped.")

    async def _poll_loop(self):
        """Polls SQLite every 15 seconds for due jobs and executes them."""
        # Heal jobs left in 'running' by a previous process (crash, hard kill, or
        # shutdown during a run). A job cannot outlive the backend, and the query
        # below skips 'running' jobs, so without this they never run again.
        await self._reset_stale_running_jobs()

        while self._running:
            try:
                now = datetime.now(timezone.utc)
                due_jobs: List[CronJob] = []
                async with AsyncSessionLocal() as session:
                    stmt = select(CronJob).where(
                        CronJob.is_enabled == True,
                        CronJob.next_run_at != None,
                        CronJob.next_run_at <= now,
                        CronJob.last_status != "running",
                    )
                    res = await session.execute(stmt)
                    due_jobs = list(res.scalars().all())

                for job in due_jobs:
                    # Run job in background to prevent blocking poll loop.
                    # Keep a reference: an unreferenced task can be garbage
                    # collected mid-flight and cannot be cancelled on shutdown.
                    task = asyncio.create_task(self.run_job(job.id))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[CronEngine] Polling error: {e}")

            await asyncio.sleep(15)

    async def _reset_stale_running_jobs(self):
        """Clears jobs left in 'running' by a previous process.

        A job cannot outlive the backend, so any row still marked 'running' at
        startup was interrupted. The poll query skips 'running' jobs, so without
        this recovery the job would never be scheduled again.
        """
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    update(CronJob)
                    .where(CronJob.last_status == "running")
                    .values(
                        last_status="failed",
                        last_logs="Interrupted by an Obelisk restart before completion.",
                    )
                )
                res = await session.execute(stmt)
                await session.commit()
                recovered = getattr(res, "rowcount", 0) or 0
                if recovered:
                    print(f"[CronEngine] Recovered {recovered} interrupted job(s) stuck in 'running'.")
        except Exception as e:
            print(f"[CronEngine] Stale job recovery failed: {e}")

    async def _run_and_capture(self, args: List[str], env: Dict[str, str], label: str):
        """Runs a child process to completion, returning (combined output, returncode).

        The child is driven by `subprocess.Popen` from a worker thread because the
        backend may run on an asyncio SelectorEventLoop (uvicorn on Windows with
        --reload/--workers forces one), where `asyncio.create_subprocess_exec`
        raises NotImplementedError and no job could ever start.

        The hard timeout is what stops a wedged child from leaving its job stuck in
        'running' forever: the poll loop permanently skips 'running' jobs.
        """
        proc = subprocess.Popen(
            args,
            cwd=str(self.workspace_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._active_procs.add(proc)
        try:
            stdout, stderr = await asyncio.wait_for(
                asyncio.to_thread(proc.communicate), timeout=JOB_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)
            raise TimeoutError(
                f"{label} exceeded the {JOB_TIMEOUT_SECONDS}s execution limit and was terminated."
            ) from None
        finally:
            self._active_procs.discard(proc)

        chunks = []
        if stdout:
            chunks.append(stdout.decode(errors="replace"))
        if stderr:
            chunks.append(stderr.decode(errors="replace"))
        return "\n".join(chunks), proc.returncode if proc.returncode is not None else -1

    async def run_job(self, job_id: str) -> Dict[str, Any]:
        """Executes a target cron job (dbt, python script, or notebook) and records results."""
        job_data: Optional[Dict[str, Any]] = None
        async with AsyncSessionLocal() as session:
            stmt = select(CronJob).where(CronJob.id == job_id)
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()
            if not job:
                raise ValueError(f"CronJob with ID '{job_id}' not found.")

            job_data = {
                "id": job.id,
                "name": job.name,
                "target_type": job.target_type,
                "target": job.target,
                "cron_expression": job.cron_expression,
            }

            # Mark as running
            job.last_status = "running"
            job.last_run_at = datetime.now(timezone.utc)
            await session.commit()

        start_time = time.time()
        status = "success"
        logs = []

        try:
            target_type = job_data["target_type"]
            target = job_data["target"]
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            logs.append(f"[{now_str}] Starting Obelisk Cron Job: '{job_data['name']}'")
            logs.append(f"Target Type: {target_type} | Target: {target}")
            logs.append("-" * 60)

            sub_env = os.environ.copy()
            backend_dir = str(self.workspace_root / "backend")
            existing_pypath = sub_env.get("PYTHONPATH", "")
            sub_env["PYTHONPATH"] = f"{backend_dir}{os.pathsep}{str(self.workspace_root)}" + (f"{os.pathsep}{existing_pypath}" if existing_pypath else "")

            if target_type == "dbt":
                # Run dbt model
                select_param = target if target and target != "*" else None
                run_res = await asyncio.to_thread(
                    dbt_runner.run_dbt,
                    command="run",
                    select=select_param
                )
                logs.append(run_res.get("output", ""))
                if not run_res.get("success", False):
                    status = "failed"

            elif target_type == "script":
                # Execute Python script in git_repo/scripts/
                script_path = self.scripts_dir / target
                if not script_path.exists():
                    script_path = self.scripts_dir / f"{target}.py"

                if not script_path.exists():
                    raise FileNotFoundError(f"Python script '{target}' not found in {self.scripts_dir}")

                output, returncode = await self._run_and_capture(
                    [_interpreter_path(), "-u", str(script_path)],
                    sub_env,
                    f"Python script '{target}'",
                )
                if output:
                    logs.append(output)

                if returncode != 0:
                    status = "failed"
                    logs.append(f"\nProcess exited with non-zero return code: {returncode}")

            elif target_type == "notebook":
                # Execute the Marimo notebook headlessly.
                #
                # Never use `marimo run <notebook>` here: `run` starts an app server
                # that serves until killed, so awaiting communicate() blocked forever.
                # The job stayed "running" (and the poll loop skips "running" jobs, so
                # it never ran again) while every execution orphaned a process holding
                # the notebook port. Marimo notebooks are plain Python -- `app.run()`
                # executes the cells and returns -- so run the file directly.
                nb_path = self.notebooks_dir / target
                if not nb_path.exists():
                    nb_path = self.notebooks_dir / f"{target}.py"

                if not nb_path.exists():
                    raise FileNotFoundError(f"Notebook '{target}' not found in {self.notebooks_dir}")

                output, returncode = await self._run_and_capture(
                    [_interpreter_path(), "-u", str(nb_path)],
                    sub_env,
                    f"Notebook '{target}'",
                )
                if output:
                    logs.append(output)

                if returncode != 0:
                    status = "failed"
                    logs.append(f"\nNotebook execution failed with code: {returncode}")

            elif target_type == "ingest":
                # Execute automated Lakehouse table ingestion
                import json
                from app.engines.ingest_engine import ingest_engine
                from app.models.connection import Connection
                from app.core.vault import decrypt

                params = json.loads(target)
                conn_id = params["connection_id"]
                entity_name = params["entity"]
                mode_val = params.get("mode", "snapshot")
                watermark_col = params.get("watermark_column")
                t_name = params.get("target_name")
                t_folder = params.get("target_folder")
                row_limit = params.get("limit")

                async with AsyncSessionLocal() as db_session:
                    stmt = select(Connection).where(Connection.id == conn_id)
                    res = await db_session.execute(stmt)
                    conn_obj = res.scalar_one_or_none()

                if not conn_obj:
                    raise ValueError(f"Connection '{conn_id}' not found for scheduled ingestion.")

                pw = decrypt(conn_obj.encrypted_credentials)
                c_params = {
                    "host": conn_obj.host,
                    "port": conn_obj.port,
                    "database": conn_obj.database,
                    "username": conn_obj.username,
                    "password": pw,
                    **(conn_obj.options or {}),
                }

                res = await asyncio.to_thread(
                    ingest_engine.ingest_entity,
                    source_type=conn_obj.source_type,
                    connection_params=c_params,
                    entity=entity_name,
                    mode=mode_val,
                    watermark_column=watermark_col,
                    target_name=t_name,
                    target_folder=t_folder,
                    limit=row_limit,
                )
                logs.append(f"Successfully ingested {res['num_rows']} rows to {res['file_path']} (Extracted: {res['new_rows_extracted']})")

            elif target_type == "batch_ingest":
                # Execute automated Lakehouse batch schema/database ingestion
                import json
                from app.engines.ingest_engine import ingest_engine
                from app.models.connection import Connection
                from app.core.vault import decrypt

                params = json.loads(target)
                conn_id = params["connection_id"]
                tables = params.get("tables", [])
                mode_val = params.get("mode", "snapshot")
                watermark_col = params.get("watermark_column")
                w_strategy = params.get("write_strategy", "append")
                pk = params.get("primary_key")
                t_folder = params.get("target_folder")
                row_limit = params.get("limit")

                async with AsyncSessionLocal() as db_session:
                    stmt = select(Connection).where(Connection.id == conn_id)
                    res = await db_session.execute(stmt)
                    conn_obj = res.scalar_one_or_none()

                if not conn_obj:
                    raise ValueError(f"Connection '{conn_id}' not found for scheduled batch ingestion.")

                pw = decrypt(conn_obj.encrypted_credentials)
                c_params = {
                    "host": conn_obj.host,
                    "port": conn_obj.port,
                    "database": conn_obj.database,
                    "username": conn_obj.username,
                    "password": pw,
                    **(conn_obj.options or {}),
                }

                logs.append(f"Starting batch cron ingestion for {len(tables)} tables...")
                for t in tables:
                    try:
                        res = await asyncio.to_thread(
                            ingest_engine.ingest_entity,
                            source_type=conn_obj.source_type,
                            connection_params=c_params,
                            entity=t,
                            mode=mode_val,
                            watermark_column=watermark_col,
                            target_folder=t_folder,
                            limit=row_limit,
                            write_strategy=w_strategy,
                            primary_key=pk,
                        )
                        logs.append(f"✓ Ingested '{t}': {res['num_rows']} rows (extracted: {res['new_rows_extracted']})")
                    except Exception as err:
                        logs.append(f"✗ Failed '{t}': {str(err)}")

            else:
                status = "failed"
                logs.append(f"Unsupported target_type: '{target_type}'")

        except Exception as e:
            status = "failed"
            # str(e) is empty for some exceptions (e.g. NotImplementedError), so log
            # the full traceback -- otherwise a scheduling failure is undiagnosable.
            logs.append(f"\nERROR during job execution: {str(e)}\n{traceback.format_exc()}")

        duration = round(time.time() - start_time, 2)
        end_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logs.append("-" * 60)
        logs.append(f"[{end_time_str}] Job finished with status: {status.upper()} ({duration}s)")
        full_log_text = "\n".join(logs)

        # Compute next run time
        next_run = self.compute_next_run(job_data["cron_expression"])

        # Update job record in SQLite
        async with AsyncSessionLocal() as session:
            stmt = (
                update(CronJob)
                .where(CronJob.id == job_id)
                .values(
                    last_status=status,
                    last_duration_seconds=duration,
                    last_logs=full_log_text,
                    next_run_at=next_run,
                )
            )
            await session.execute(stmt)
            await session.commit()

        return {
            "job_id": job_id,
            "status": status,
            "duration_seconds": duration,
            "logs": full_log_text,
            "next_run_at": next_run.isoformat() if next_run else None,
        }

    def list_available_targets(self) -> Dict[str, List[Dict[str, Any]]]:
        """Returns all runnable targets (scripts, notebooks, dbt models) for the job creation picker."""
        scripts = []
        if self.scripts_dir.exists():
            for f in sorted(self.scripts_dir.glob("*.py")):
                if not f.name.startswith("_"):
                    stat = f.stat()
                    scripts.append({
                        "filename": f.name,
                        "name": f.stem,
                        "size_bytes": stat.st_size,
                    })

        notebooks = []
        if self.notebooks_dir.exists():
            for f in sorted(self.notebooks_dir.glob("*.py")):
                if not f.name.startswith("_"):
                    stat = f.stat()
                    notebooks.append({
                        "filename": f.name,
                        "name": f.stem,
                        "size_bytes": stat.st_size,
                    })

        dbt_selectors = [
            {"label": "All Models (*)", "value": "*", "description": "Compiles & runs all staging and mart models"},
        ]

        dbt_models_dir = self.workspace_root / "git_repo" / "dbt_project" / "models"
        if dbt_models_dir.exists():
            # 1. Dynamically scan top-level model folders (e.g., staging, marts)
            for sub_dir in sorted(dbt_models_dir.iterdir()):
                if sub_dir.is_dir() and not sub_dir.name.startswith(("_", ".")):
                    dbt_selectors.append({
                        "label": f"Directory: models/{sub_dir.name}/*",
                        "value": f"models/{sub_dir.name}",
                        "description": f"Runs all models inside models/{sub_dir.name}",
                    })

            # 2. Dynamically scan all individual .sql models in the project
            for sql_file in sorted(dbt_models_dir.rglob("*.sql")):
                if not sql_file.name.startswith(("_", ".")):
                    model_name = sql_file.stem
                    rel_dir = sql_file.parent.relative_to(dbt_models_dir).as_posix()
                    dir_tag = f" ({rel_dir})" if rel_dir != "." else ""
                    dbt_selectors.append({
                        "label": f"Model: {model_name}{dir_tag}",
                        "value": model_name,
                        "description": f"Compiles & runs {model_name}.sql",
                    })
                    dbt_selectors.append({
                        "label": f"Lineage: {model_name}+ (and downstream)",
                        "value": f"{model_name}+",
                        "description": f"Runs {model_name} and all downstream dependencies",
                    })

        return {
            "scripts": scripts,
            "notebooks": notebooks,
            "dbt_selectors": dbt_selectors,
        }


cron_engine = CronEngine()
