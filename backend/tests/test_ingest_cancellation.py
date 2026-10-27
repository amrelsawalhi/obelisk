"""Cancelling an ingestion has to actually stop it.

Cancellation used to be checked between *tables*, so a batch job stopped promptly but a
single large table — one ``ingest_entity`` call, or a Postgres load that is one DuckDB
statement — could not be stopped at all: "cancel" flipped a flag and the operator waited
for the whole table anyway.

Two mechanisms close that: the staging loop polls a callback per batch, and the live
DuckDB connection is published so ``cancel_job`` can interrupt a running statement. Both
are exercised here against a real lakehouse and a real DuckDB connection.
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid

import duckdb
import pyarrow as pa
import pytest

from app.config import settings
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestCancelled, IngestEngine
from app.engines.ingest_job_manager import IngestJobManager


def _forget(job_id: str) -> None:
    """Drops the job this test created from the durable task history."""
    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute("DELETE FROM task_history WHERE kind = 'ingest' AND id = ?", (job_id,))
        con.commit()


@pytest.fixture()
def sink():
    """A throwaway load table to stream into, in a schema of its own."""
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    schema = f"zzcancel{uuid.uuid4().hex[:6]}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    try:
        yield engine, conn, engine._target_sql(schema, "streamload")
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def test_stage_batches_stops_between_batches(sink) -> None:
    """The flag is polled per batch, so one huge table stops mid-load.

    Three batches, the second of which flips the flag: the rows already committed stay
    (they are real Parquet, and a rerun resumes from the watermark), and nothing after the
    cancel is read.
    """
    engine, conn, target = sink
    polls: list[int] = []

    def should_cancel() -> bool:
        polls.append(len(polls))
        return len(polls) > 1  # False for the first batch, True from then on

    batches = [pa.table({"n": list(range(5))}) for _ in range(3)]

    with pytest.raises(IngestCancelled):
        engine._stage_batches(
            conn, batches, sink=target, create=True, should_cancel=should_cancel
        )

    assert len(polls) == 2, "staging must stop polling once it has cancelled"
    assert engine._row_count(conn, target) == 5, "only the first batch was committed"


def test_stage_batches_without_a_cancel_hook_never_stops(sink) -> None:
    """No hook means no cancellation point — the pre-change behaviour, unchanged."""
    engine, conn, target = sink
    batches = [pa.table({"n": [1, 2]}), pa.table({"n": [3]})]

    assert engine._stage_batches(conn, batches, sink=target, create=True) == 3


def test_a_cancelled_job_is_not_reported_as_a_failure() -> None:
    """Cancel before the first batch: the job ends ``cancelled``, not ``failed``.

    Without the cancellation check in ``ingest_entity`` the unknown source would raise
    instead, and the job would be recorded as a broken source rather than a decision.
    """
    manager = IngestJobManager()
    job = manager.create_job(
        title="cancel probe",
        job_type="single",
        connection_id="conn-1",
        source_name="source",
        source_type="not-a-real-source",
        mode="snapshot",
    )

    async def run() -> None:
        assert manager.cancel_job(job.id) is True
        await manager.execute_single_job_async(
            job_id=job.id,
            connection_params={},
            entity="orders",
            mode="snapshot",
        )

    try:
        asyncio.run(run())

        assert job.status == "cancelled"
        assert job.is_cancelled is True
        assert job.failed_tables == 0
        assert job.errors == [], "a cancellation must not be recorded as a source error"
    finally:
        _forget(job.id)


def test_cancel_job_interrupts_the_statement_that_is_running() -> None:
    """A load with no batch boundary is stopped by interrupting its DuckDB statement.

    This is the Postgres path: one statement moves the whole table, so there is nothing to
    poll between batches. ``cancel_job`` interrupts the connection the engine published.
    """
    manager = IngestJobManager()
    job = manager.create_job(
        title="interrupt probe",
        job_type="single",
        connection_id="conn-1",
        source_name="source",
        source_type="postgres",
        mode="snapshot",
    )
    conn = duckdb.connect()
    failure: list[BaseException] = []

    def long_query() -> None:
        try:
            conn.execute("SELECT count(*) FROM range(1000000000000);").fetchall()
        except BaseException as exc:  # an interrupted statement raises a DuckDB error
            failure.append(exc)

    try:
        manager._attach_connection(job.id, conn)
        assert manager._live_connections[job.id] is conn

        worker = threading.Thread(target=long_query, daemon=True)
        worker.start()
        threading.Event().wait(0.4)  # let the statement start before interrupting it

        assert manager.cancel_job(job.id) is True
        worker.join(timeout=15.0)

        assert not worker.is_alive(), "the statement outlived the cancel"
        assert failure, "an interrupted statement must not come back with a result"
        assert job.status == "cancelled"
    finally:
        manager._detach_connection(job.id)
        conn.close()
        _forget(job.id)

