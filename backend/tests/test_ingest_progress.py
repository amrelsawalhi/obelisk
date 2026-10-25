"""Ingest progress: per-batch counts, and the latency a preview reports.

A single large table used to show one static phase until it finished, because progress
was only reported per table. The engine now calls back after every staged batch; these
tests pin that the count climbs and that the callback survives the trip back to the
event loop (the SSE queues belong to it, not to the ingestion thread).

The preview tests cover the other half of the same gap: live-source previews carried no
``duration_ms``, so the UI rendered "Latency:  ms" for every external table.
"""
from __future__ import annotations

import asyncio
import sqlite3

import pyarrow as pa

from app.config import settings
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine
from app.engines.ingest_job_manager import IngestJobManager
from app.engines.introspector import _preview_payload


def _cleanup(*pairs: tuple[str, str]) -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        for kind, task_id in pairs:
            con.execute("DELETE FROM task_history WHERE kind = ? AND id = ?", (kind, task_id))
        con.commit()


def test_stage_batches_reports_a_running_total() -> None:
    """Three batches produce three callbacks, each the total so far — not the batch size."""
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    seen: list[int] = []
    batches = [
        pa.table({"n": list(range(5))}),
        pa.table({"n": list(range(5, 9))}),
        pa.table({"n": list(range(9, 15))}),
    ]
    try:
        total = engine._stage_batches(conn, batches, on_batch=seen.append)
    finally:
        conn.close()

    assert seen == [5, 9, 15]
    assert total == 15


def test_stage_batches_without_a_callback_still_works() -> None:
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    try:
        assert engine._stage_batches(conn, [pa.table({"n": [1, 2, 3]})]) == 3
    finally:
        conn.close()


def test_batch_progress_lands_when_called_from_a_worker_thread() -> None:
    """The engine calls the hook from a thread; the update must land on the loop.

    ``update_progress`` touches the SSE queues, which belong to the event loop, so the
    hook marshals itself with ``call_soon_threadsafe``. This runs it the way ingestion
    does — off-loop — and asserts the job actually moved.
    """
    manager = IngestJobManager()
    job = manager.create_job(
        title="progress probe",
        job_type="single",
        connection_id="conn-1",
        source_name="source",
        source_type="postgres",
        mode="snapshot",
    )

    async def run() -> None:
        hook = manager._batch_progress(job.id)
        await asyncio.to_thread(hook, 150_000)
        await asyncio.sleep(0.05)

    try:
        asyncio.run(run())

        # Staged, not written: the rows sit in the temp table until the write statement runs,
        # so they must not be added to the rows this job claims to have ingested.
        assert job.staged_rows == 150_000
        assert job.total_rows == 0
        assert job.current_phase == "Staged 150,000 rows"
    finally:
        _cleanup(("ingest", job.id))


def test_a_failed_write_does_not_leave_staged_rows_counted() -> None:
    """A table that fails after staging reports the rows it actually wrote: none."""
    manager = IngestJobManager()
    job = manager.create_job(
        title="failure probe",
        job_type="single",
        connection_id="conn-1",
        source_name="source",
        source_type="not-a-real-source",
        mode="snapshot",
    )

    async def run() -> None:
        hook = manager._batch_progress(job.id)
        await asyncio.to_thread(hook, 250_000)
        await asyncio.sleep(0.05)
        assert job.staged_rows == 250_000

        # The driver rejects the unknown source, so the write never happens.
        await manager.execute_single_job_async(
            job_id=job.id,
            connection_params={},
            entity="orders",
            mode="snapshot",
        )
        await asyncio.sleep(0.05)

    try:
        asyncio.run(run())

        assert job.status == "failed"
        assert job.staged_rows == 0
        assert job.total_rows == 0
    finally:
        _cleanup(("ingest", job.id))


def test_batch_progress_keeps_the_table_position() -> None:
    """A batch job's phase keeps "(2/5)": the row count must not erase where it is."""
    manager = IngestJobManager()
    job = manager.create_job(
        title="batch probe",
        job_type="batch",
        connection_id="conn-1",
        source_name="source",
        source_type="postgres",
        mode="snapshot",
        total_tables=5,
    )

    async def run() -> None:
        hook = manager._batch_progress(job.id, label="Extracting (2/5)")
        await asyncio.to_thread(hook, 1_500)
        await asyncio.sleep(0.05)

    try:
        asyncio.run(run())

        assert job.current_phase == "Extracting (2/5) - 1,500 rows staged"
    finally:
        _cleanup(("ingest", job.id))


def test_preview_payload_carries_a_latency() -> None:
    """Every live-source preview returns a timed payload, so the UI has something to show."""
    import time

    start = time.perf_counter()
    payload = _preview_payload(["id"], [{"id": 1}, {"id": 2}], start)

    assert payload["row_count"] == 2
    assert payload["columns"] == ["id"]
    assert isinstance(payload["duration_ms"], int)
    assert payload["duration_ms"] >= 0
