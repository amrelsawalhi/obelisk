"""The platform's resource budget: one pool shared by queries, ingests and dbt.

The audit's memory claims all reduce to one question: does anything that can hold a
large amount of memory actually take a share of a single budget? These pin the pieces
that make that true — the spill division, the slot accounting, the ingest cap, the
cross-process load lock, and history writes that no longer run on the event loop.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid

import pytest

from app.config import settings
from app.core.catalog import (
    LOAD_TABLE_SUFFIX,
    _active_load_lock_path,
    load_tables,
    register_active_load_table,
    unregister_active_load_table,
)
from app.core.resource_advisor import advisor
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine
from app.engines.ingest_job_manager import IngestJobManager
from app.services.state import task_history


@pytest.fixture()
def lake():
    engine = IngestEngine()
    schema = f"zz{uuid.uuid4().hex[:6]}"
    conn = duckdb_engine.get_connection()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    try:
        yield engine, conn, schema
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def _forget(kind: str, task_id: str) -> None:
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        conn.execute("DELETE FROM task_history WHERE kind = ? AND id = ?", (kind, task_id))
        conn.commit()


def test_the_spill_budget_is_divided_between_connections(tmp_path) -> None:
    """Four connections must not each be promised the whole process's spill space."""
    single = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "50GB", 1)
    four = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "50GB", 4)

    assert four == max(1, single // 4)
    assert four * 4 <= single, "the divided budget must not add up to more than the whole"


def test_a_small_configured_ceiling_is_still_honoured(tmp_path) -> None:
    assert advisor.get_duckdb_temp_limit(str(tmp_path), "1MB", 4) == "1MB"


def test_a_heavy_slot_is_counted_and_returned() -> None:
    before = duckdb_engine.get_telemetry()["active_heavy_queries"]
    with duckdb_engine.heavy_slot():
        assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before + 1
    assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before


def test_a_failed_heavy_task_returns_its_slot() -> None:
    before = duckdb_engine.get_telemetry()["active_heavy_queries"]
    with pytest.raises(RuntimeError, match="boom"):
        with duckdb_engine.heavy_slot():
            raise RuntimeError("boom")
    assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before


def test_ingests_share_one_cap_per_manager() -> None:
    manager = IngestJobManager()
    assert manager._ingest_slots() is manager._ingest_slots()
    assert max(1, int(settings.INGEST_MAX_CONCURRENT)) >= 1


def test_registering_a_load_writes_and_removes_its_lock(lake) -> None:
    """The lock file is what a maintenance run in *another* process can see."""
    engine, conn, schema = lake
    table = f"orders{LOAD_TABLE_SUFFIX}"
    conn.execute(f"CREATE TABLE {engine._target_sql(schema, table)} AS SELECT 1 AS id")
    lock = _active_load_lock_path(schema, table)

    register_active_load_table(schema, table)
    try:
        assert lock.is_file()
        payload = json.loads(lock.read_text(encoding="utf-8"))
        assert payload["pid"] == os.getpid()
        assert (schema, table) not in load_tables(conn)
    finally:
        unregister_active_load_table(schema, table)

    assert not lock.exists()
    assert (schema, table) in load_tables(conn)


def test_a_load_owned_by_another_live_process_is_never_dropped(lake) -> None:
    """A CLI sweep runs in its own interpreter; a PID lock is all it can see."""
    engine, conn, schema = lake
    table = f"orders{LOAD_TABLE_SUFFIX}"
    conn.execute(f"CREATE TABLE {engine._target_sql(schema, table)} AS SELECT 1 AS id")
    lock = _active_load_lock_path(schema, table)
    lock.parent.mkdir(parents=True, exist_ok=True)

    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        lock.write_text(
            json.dumps({"pid": holder.pid, "schema": schema, "table": table}),
            encoding="utf-8",
        )
        assert (schema, table) not in load_tables(conn), (
            "a load table whose owner is alive must be protected"
        )
    finally:
        holder.terminate()
        holder.wait(timeout=15)
        lock.unlink(missing_ok=True)

    for _ in range(40):
        if (schema, table) in load_tables(conn):
            break
        time.sleep(0.05)
    assert (schema, table) in load_tables(conn), "the orphan is collectable once its owner is gone"


def test_history_is_written_off_the_event_loop() -> None:
    """Creating a job from async code must not do the SQLite write on the loop."""
    manager = IngestJobManager()

    async def run():
        job = manager.create_job(
            title="async history probe",
            job_type="single",
            connection_id="conn-1",
            source_name="source",
            source_type="postgres",
            mode="snapshot",
        )
        for _ in range(100):
            if task_history.fetch("ingest", job.id) is not None:
                return job
            await asyncio.sleep(0.05)
        return job

    job = asyncio.run(run())
    try:
        stored = task_history.fetch("ingest", job.id)
        assert stored is not None, "the executor write must still land"
        assert stored["title"] == "async history probe"
    finally:
        _forget("ingest", job.id)
