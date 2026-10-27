import asyncio
import functools
import json
import threading
import uuid
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Any, List, Optional, AsyncGenerator
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from app.engines.ingest_engine import ingest_engine
from app.services.state import task_history


@dataclass
class IngestJob:
    id: str
    title: str
    job_type: str  # "single" | "batch"
    connection_id: str
    source_name: str
    source_type: str
    status: str  # "running" | "completed" | "failed" | "cancelled"
    mode: str
    started_at: str
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    total_tables: int = 1
    completed_tables: int = 0
    failed_tables: int = 0
    current_table: Optional[str] = None
    current_phase: Optional[str] = None
    percent: int = 0
    total_rows: int = 0
    # Rows staged for the table in flight. Kept apart from total_rows so a failed write
    # cannot leave the job claiming rows that were never committed.
    staged_rows: int = 0
    logs: List[str] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Dict[str, str]] = field(default_factory=list)
    is_cancelled: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class IngestJobManager:
    """
    Decoupled Ingestion Job Runner & Real-Time SSE Broadcaster.
    Allows jobs to run safely in background worker threads independently of client connections.
    Supports in-flight log buffering, live SSE streaming, minimization, and graceful aborts.
    """

    def __init__(self):
        self._jobs: Dict[str, IngestJob] = {}
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._last_persist: Dict[str, float] = {}
        # The DuckDB connection each running job is using. A load that is one long
        # statement (the whole Postgres path) has no batch boundary at which to notice a
        # cancel flag, so cancelling has to interrupt the statement itself.
        self._live_connections: Dict[str, Any] = {}
        self._connection_lock = threading.Lock()

    def _attach_connection(self, job_id: str, conn: Any) -> None:
        """Remembers a job's live connection. Called from the ingestion thread."""
        with self._connection_lock:
            self._live_connections[job_id] = conn

    def _detach_connection(self, job_id: str) -> None:
        with self._connection_lock:
            self._live_connections.pop(job_id, None)

    def _persist(self, job: IngestJob) -> bool:
        """Mirrors the job into task history (M3).

        Throttled to once a second while running: progress arrives per log line, and
        the point of this write is that a restart can reconstruct the job, not that it
        captures every tick. Terminal states always land.
        """
        now = time.monotonic()
        terminal = job.status != "running"
        if not terminal and (now - self._last_persist.get(job.id, 0.0)) < 1.0:
            return False
        self._last_persist[job.id] = now
        return task_history.record(
            "ingest", job.id, job.status, job.started_at, job.to_dict()
        )

    def hydrate(self, limit: int = 50) -> int:
        """Reloads recent jobs so history survives a restart.

        Must run after ``mark_interrupted`` so nothing returns as still running.
        """
        restored = 0
        for payload in task_history.recent("ingest", limit=limit):
            job_id = payload.get("id")
            if not job_id or job_id in self._jobs:
                continue
            try:
                self._jobs[job_id] = IngestJob(**payload)
            except TypeError:
                continue
            restored += 1
        return restored

    def get_job(self, job_id: str) -> Optional[IngestJob]:
        return self._jobs.get(job_id)

    def get_active_jobs(self) -> List[Dict[str, Any]]:
        return [
            job.to_dict()
            for job in self._jobs.values()
            if job.status == "running"
        ]

    def get_recent_jobs(self, limit: int = 20) -> List[Dict[str, Any]]:
        sorted_jobs = sorted(
            self._jobs.values(),
            key=lambda j: j.started_at,
            reverse=True,
        )
        return [j.to_dict() for j in sorted_jobs[:limit]]

    def create_job(
        self,
        title: str,
        job_type: str,
        connection_id: str,
        source_name: str,
        source_type: str,
        mode: str,
        total_tables: int = 1,
    ) -> IngestJob:
        job_id = str(uuid.uuid4())
        now_str = datetime.now(timezone.utc).isoformat()
        job = IngestJob(
            id=job_id,
            title=title,
            job_type=job_type,
            connection_id=connection_id,
            source_name=source_name,
            source_type=source_type,
            status="running",
            mode=mode,
            started_at=now_str,
            total_tables=total_tables,
            percent=0,
        )
        self._jobs[job_id] = job
        self._persist(job)
        return job

    def append_log(self, job_id: str, message: str):
        job = self._jobs.get(job_id)
        if not job:
            return

        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        formatted = f"[{ts}] {message}"
        job.logs.append(formatted)

        # Broadcast to all live subscribers
        event_payload = json.dumps({"type": "log", "text": formatted})
        for q in list(self._subscribers.get(job_id, [])):
            try:
                q.put_nowait(event_payload)
            except Exception:
                pass

    def set_schedule_log(self, job_id: str, message: str):
        """Append a schedule-registration log line, prefixed with [SCHEDULE]."""
        self.append_log(job_id, f"[SCHEDULE] {message}")

    def update_progress(
        self,
        job_id: str,
        current_table: Optional[str] = None,
        current_phase: Optional[str] = None,
        completed_tables: Optional[int] = None,
        failed_tables: Optional[int] = None,
        total_rows: Optional[int] = None,
        staged_rows: Optional[int] = None,
    ):
        job = self._jobs.get(job_id)
        if not job:
            return

        if current_table is not None:
            job.current_table = current_table
        if current_phase is not None:
            job.current_phase = current_phase
        if completed_tables is not None:
            job.completed_tables = completed_tables
        if failed_tables is not None:
            job.failed_tables = failed_tables
        if total_rows is not None:
            job.total_rows = total_rows
        if staged_rows is not None:
            job.staged_rows = staged_rows

        if job.total_tables > 0:
            done = job.completed_tables + job.failed_tables
            job.percent = min(100, int((done / job.total_tables) * 100))

        self._persist(job)

        event_payload = json.dumps({
            "type": "progress",
            "percent": job.percent,
            "completed_tables": job.completed_tables,
            "failed_tables": job.failed_tables,
            "total_tables": job.total_tables,
            "current_table": job.current_table,
            "current_phase": job.current_phase,
            "total_rows": job.total_rows,
            "staged_rows": job.staged_rows,
            "status": job.status,
        })
        for q in list(self._subscribers.get(job_id, [])):
            try:
                q.put_nowait(event_payload)
            except Exception:
                pass

    def _batch_progress(self, job_id: str, label: Optional[str] = None) -> Callable[[int], None]:
        """A per-batch progress hook that is safe to call from the worker thread.

        ``update_progress`` writes to the SSE queues, which belong to the event loop, so
        the call is marshalled back instead of mutating them from the ingestion thread.
        """
        loop = asyncio.get_running_loop()

        def report(staged: int) -> None:
            phase = f"{label} - {staged:,} rows staged" if label else f"Staged {staged:,} rows"
            loop.call_soon_threadsafe(
                functools.partial(
                    self.update_progress,
                    job_id,
                    current_phase=phase,
                    # Deliberately not total_rows: these rows are staged, not committed — a
                    # load that fails must not leave them counted as ingested.
                    staged_rows=staged,
                )
            )

        return report

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != "running":
            return False

        job.is_cancelled = True
        job.status = "cancelled"
        job.completed_at = datetime.now(timezone.utc).isoformat()
        self._persist(job)

        # Break the statement that is running right now, if any. Without this a cancel
        # during a single-statement load (the Postgres path) waited for the whole table.
        with self._connection_lock:
            live = self._live_connections.get(job_id)
        if live is not None:
            try:
                live.interrupt()
            except Exception as exc:
                self.append_log(job_id, f"Could not interrupt the running statement: {exc}")

        self.append_log(job_id, "⚠️ Ingestion cancelled by user. Terminating remaining operations...")

        event_payload = json.dumps({
            "type": "cancelled",
            "job": job.to_dict(),
        })
        for q in list(self._subscribers.get(job_id, [])):
            try:
                q.put_nowait(event_payload)
            except Exception:
                pass
        return True

    async def subscribe(self, job_id: str) -> AsyncGenerator[str, None]:
        """
        Yields Server-Sent Events (SSE).
        First flushes all existing accumulated logs & status to catch the client up,
        then streams new events as they occur.
        """
        job = self._jobs.get(job_id)
        if not job:
            yield f"event: error\ndata: {json.dumps({'error': 'Job not found'})}\n\n"
            return

        # 1. Flush historical catch-up state
        initial_progress = json.dumps({
            "type": "progress",
            "percent": job.percent,
            "completed_tables": job.completed_tables,
            "failed_tables": job.failed_tables,
            "total_tables": job.total_tables,
            "current_table": job.current_table,
            "current_phase": job.current_phase,
            "total_rows": job.total_rows,
            "status": job.status,
            "title": job.title,
            "results": job.results,
            "errors": job.errors,
        })
        yield f"event: progress\ndata: {initial_progress}\n\n"

        for log_line in job.logs:
            log_event = json.dumps({"type": "log", "text": log_line})
            yield f"event: log\ndata: {log_event}\n\n"

        # If already done, yield final complete event and exit
        if job.status in ("completed", "failed", "cancelled"):
            yield f"event: complete\ndata: {json.dumps(job.to_dict())}\n\n"
            return

        # 2. Register live queue
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers[job_id].append(q)

        try:
            while True:
                # Wait for next event or heartbeat
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    data_obj = json.loads(payload)
                    ev_type = data_obj.get("type", "message")
                    yield f"event: {ev_type}\ndata: {payload}\n\n"

                    if ev_type in ("complete", "cancelled"):
                        break
                except asyncio.TimeoutError:
                    # Keep-alive heartbeat comment for proxies (Caddy/Nginx)
                    yield ": ping\n\n"

                # Check if job transitioned while waiting
                if job.status in ("completed", "failed", "cancelled") and q.empty():
                    yield f"event: complete\ndata: {json.dumps(job.to_dict())}\n\n"
                    break
        finally:
            if q in self._subscribers.get(job_id, []):
                self._subscribers[job_id].remove(q)

    async def execute_single_job_async(
        self,
        job_id: str,
        connection_params: Dict[str, Any],
        entity: str,
        mode: str,
        watermark_column: Optional[str] = None,
        target_name: Optional[str] = None,
        target_folder: Optional[str] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ):
        """Asynchronously executes single-table lakehouse ingestion."""
        job = self._jobs[job_id]
        start_time = time.time()
        self.append_log(job_id, f"Starting extraction for table: '{entity}'")
        self.update_progress(job_id, current_table=entity, current_phase="Connecting to source")

        try:
            if job.is_cancelled:
                return

            self.update_progress(job_id, current_phase="Staging rows into the lakehouse")

            # Run in worker thread so DuckDB C++ / PyArrow doesn't block async loop
            res = await asyncio.to_thread(
                ingest_engine.ingest_entity,
                source_type=job.source_type,
                connection_params=connection_params,
                entity=entity,
                mode=mode,
                watermark_column=watermark_column,
                target_name=target_name,
                target_folder=target_folder,
                limit=limit,
                write_strategy=write_strategy,
                primary_key=primary_key,
                on_batch=self._batch_progress(job_id),
                # Polled between batches, and the connection is published so cancel_job can
                # interrupt a single-statement load.
                should_cancel=lambda: job.is_cancelled,
                on_connection=lambda conn: self._attach_connection(job_id, conn),
            )

            job.results.append(res)
            job.total_rows += res.get("num_rows", 0)
            job.staged_rows = 0
            job.completed_tables = 1
            job.percent = 100
            job.status = "completed"
            job.completed_at = datetime.now(timezone.utc).isoformat()
            job.duration_ms = int((time.time() - start_time) * 1000)

            self.append_log(
                job_id,
                f"✓ Successfully ingested {res.get('num_rows', 0)} rows to {res.get('file_path')} "
                f"(New rows extracted: {res.get('new_rows_extracted', 0)}) [{job.duration_ms}ms]"
            )
            self.update_progress(job_id, current_phase="Finished", completed_tables=1)

        except Exception as e:
            # A cancel surfaces here as IngestCancelled (raised between batches) or as
            # DuckDB's interrupt exception (raised inside a single statement). Either way
            # nothing was committed, so it is not a failure and is not counted as one.
            if job.is_cancelled:
                job.status = "cancelled"
                job.failed_tables = 0
                job.staged_rows = 0
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.duration_ms = int((time.time() - start_time) * 1000)
                self.append_log(
                    job_id,
                    f"Ingestion of '{entity}' stopped before it was committed; "
                    "the table was left as it was.",
                )
                self.update_progress(job_id, current_phase="Cancelled")
            else:
                job.status = "failed"
                job.failed_tables = 1
                # Whatever was staged was never committed: nothing is written until the final
                # statement runs, and that is the statement that just failed.
                job.staged_rows = 0
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.duration_ms = int((time.time() - start_time) * 1000)
                err_msg = str(e)
                job.errors.append({"table": entity, "error": err_msg})
                self.append_log(job_id, f"✗ Ingestion failed for '{entity}': {err_msg}")
                self.update_progress(job_id, current_phase="Failed")

        finally:
            self._detach_connection(job_id)
            complete_payload = json.dumps({"type": "complete", "job": job.to_dict()})
            for q in list(self._subscribers.get(job_id, [])):
                try:
                    q.put_nowait(complete_payload)
                except Exception:
                    pass

    async def execute_batch_job_async(
        self,
        job_id: str,
        connection_params: Dict[str, Any],
        tables: List[str],
        mode: str,
        watermark_column: Optional[str] = None,
        target_folder: Optional[str] = None,
        limit: Optional[int] = None,
        write_strategy: str = "append",
        primary_key: Optional[str] = None,
    ):
        """Asynchronously executes batch schema/database lakehouse ingestion."""
        job = self._jobs[job_id]
        start_time = time.time()
        self.append_log(job_id, f"Starting batch extraction for {len(tables)} tables from {job.source_name}")

        try:
            for idx, table_name in enumerate(tables):
                if job.is_cancelled:
                    self.append_log(job_id, "Batch job stopped due to user cancellation.")
                    break

                self.update_progress(
                    job_id,
                    current_table=table_name,
                    current_phase=f"Extracting ({idx + 1}/{len(tables)})",
                )
                self.append_log(job_id, f"[{idx + 1}/{len(tables)}] Processing '{table_name}'...")

                table_start = time.time()
                try:
                    res = await asyncio.to_thread(
                        ingest_engine.ingest_entity,
                        source_type=job.source_type,
                        connection_params=connection_params,
                        entity=table_name,
                        mode=mode,
                        watermark_column=watermark_column,
                        target_folder=target_folder,
                        limit=limit,
                        write_strategy=write_strategy,
                        primary_key=primary_key,
                        on_batch=self._batch_progress(
                            job_id, label=f"Extracting ({idx + 1}/{len(tables)})"
                        ),
                        should_cancel=lambda: job.is_cancelled,
                        on_connection=lambda conn: self._attach_connection(job_id, conn),
                    )
                    t_duration = int((time.time() - table_start) * 1000)
                    job.results.append(res)
                    job.completed_tables += 1
                    job.total_rows += res.get("num_rows", 0)
                    job.staged_rows = 0

                    self.append_log(
                        job_id,
                        f"  ✓ '{table_name}': {res.get('num_rows', 0)} rows "
                        f"(+{res.get('new_rows_extracted', 0)} new) [{t_duration}ms] → {res.get('file_path')}"
                    )
                except Exception as table_err:
                    if job.is_cancelled:
                        # Not a failure: the operator stopped it, and nothing was committed.
                        job.staged_rows = 0
                        self.append_log(
                            job_id,
                            f"  ⏹ '{table_name}' stopped before it was committed.",
                        )
                        break
                    err_msg = str(table_err)
                    job.failed_tables += 1
                    job.staged_rows = 0
                    job.errors.append({"table": table_name, "error": err_msg})
                    self.append_log(job_id, f"  ✗ Failed '{table_name}': {err_msg}")
                finally:
                    self._detach_connection(job_id)

                self.update_progress(
                    job_id,
                    completed_tables=job.completed_tables,
                    failed_tables=job.failed_tables,
                    total_rows=job.total_rows,
                )

            if not job.is_cancelled:
                job.status = "completed" if job.failed_tables == 0 else ("completed" if job.completed_tables > 0 else "failed")
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.duration_ms = int((time.time() - start_time) * 1000)
                self.append_log(
                    job_id,
                    f"Batch ingestion finished: {job.completed_tables} succeeded, "
                    f"{job.failed_tables} failed ({job.total_rows:,} total rows in {job.duration_ms / 1000:.1f}s)"
                )
                self.update_progress(job_id, current_phase="Finished", current_table=None)

        except Exception as e:
            job.status = "failed"
            job.completed_at = datetime.now(timezone.utc).isoformat()
            job.duration_ms = int((time.time() - start_time) * 1000)
            self.append_log(job_id, f"Fatal batch error: {str(e)}")
            self.update_progress(job_id, current_phase="Failed")

        finally:
            complete_payload = json.dumps({"type": "complete", "job": job.to_dict()})
            for q in list(self._subscribers.get(job_id, [])):
                try:
                    q.put_nowait(complete_payload)
                except Exception:
                    pass


# Singleton instance
ingest_job_manager = IngestJobManager()
