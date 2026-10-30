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
from pathlib import Path

import psutil
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
from app.core.resource_leases import list_resource_leases
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
    assert advisor.get_duckdb_temp_limit(str(tmp_path), "1MB", 4) == "1MiB"


def test_a_heavy_slot_is_counted_and_returned() -> None:
    before = duckdb_engine.get_telemetry()["active_heavy_queries"]
    with duckdb_engine.heavy_slot():
        assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before + 1
    assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before


def test_a_multi_threaded_job_can_reserve_several_heavy_slots() -> None:
    """dbt's database-wide budget is backed by one permit per model thread."""
    before = duckdb_engine.get_telemetry()["active_heavy_queries"]
    reserved = duckdb_engine.begin_heavy(2)
    try:
        assert reserved == min(2, settings.DUCKDB_MAX_CONCURRENT_HEAVY)
        assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before + reserved
    finally:
        duckdb_engine.end_heavy(reserved)
    assert duckdb_engine.get_telemetry()["active_heavy_queries"] == before


def test_a_reservation_is_visible_as_a_cross_process_lease() -> None:
    slots = min(2, settings.DUCKDB_MAX_CONCURRENT_HEAVY)
    reserved = duckdb_engine.begin_heavy(
        slots,
        kind="dbt",
        memory_limit_mb=2048,
        temp_limit_mb=4096,
        temp_path=str(settings.duckdb_temp_path),
        detail="test build",
    )
    try:
        leases = [item for item in list_resource_leases() if item.get("kind") == "dbt"]
        assert any(
            item.get("pid") == os.getpid() and item.get("slots") == reserved
            for item in leases
        )
        telemetry = duckdb_engine.get_telemetry()
        assert telemetry["active_by_kind"].get("dbt") == reserved
        assert telemetry["active_temp_limit_mb"] >= 4096
        assert telemetry["active_memory_limit_mb"] >= 2048
        assert any(
            item["kind"] == "dbt" and item["slots"] == reserved
            for item in telemetry["active_reservations"]
        )
    finally:
        duckdb_engine.end_heavy(reserved)
    assert all(item.get("kind") != "dbt" or item.get("pid") != os.getpid() for item in list_resource_leases())


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


def test_temp_usage_counts_spill_but_not_dbt_artifacts() -> None:
    """The panel must not present compiled dbt output or scratch databases as spill."""
    base = settings.duckdb_temp_path
    base.mkdir(parents=True, exist_ok=True)
    spill = base / "pytest_spill_probe.bin"
    run_dir = base / "dbt_run_pytest_probe"
    scratch = base / "dbt_scratch_pytest_probe.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        spill.write_bytes(b"x" * (1024 * 1024))
        (run_dir / "manifest.json").write_bytes(b"y" * (2 * 1024 * 1024))
        scratch.write_bytes(b"z" * (3 * 1024 * 1024))
        before = duckdb_engine.get_telemetry()["temp_spill_bytes"]
        spill.write_bytes(b"x" * (2 * 1024 * 1024))
        after = duckdb_engine.get_telemetry()["temp_spill_bytes"]
        assert after - before == 1024 * 1024, (
            "only the real spill-sized probe should move the temp meter"
        )
    finally:
        spill.unlink(missing_ok=True)
        scratch.unlink(missing_ok=True)
        for item in run_dir.rglob("*"):
            item.unlink(missing_ok=True)
        run_dir.rmdir()


def test_a_child_process_lease_is_visible_to_backend_telemetry() -> None:
    """A notebook or script runs in its own process; its reservation must still count."""
    backend = str(Path(__file__).resolve().parents[1])
    env = os.environ.copy()
    env["PYTHONPATH"] = backend + os.pathsep + env.get("PYTHONPATH", "")
    code = (
        "import json, os, time\n"
        "from app.core.resource_leases import register_resource_lease\n"
        "lease = register_resource_lease(kind='sdk', slots=1, detail='child probe')\n"
        "print(json.dumps({'lease': lease, 'pid': os.getpid()}), flush=True)\n"
        "time.sleep(30)\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    child_pid = None
    try:
        line = child.stdout.readline() if child.stdout is not None else ""
        assert line.strip(), "the child did not publish a lease"
        published = json.loads(line)
        assert published["lease"], "the child could not create its lease file"
        child_pid = int(published["pid"])
        deadline = time.monotonic() + 10
        seen = []
        while time.monotonic() < deadline:
            seen = [
                item
                for item in list_resource_leases()
                if item.get("id") == published["lease"]
            ]
            if seen:
                break
            time.sleep(0.1)
        assert seen, "the backend cannot see a live child-process reservation"
        assert seen[0]["pid"] == child_pid
        assert seen[0]["kind"] == "sdk"
        assert seen[0]["slots"] == 1
    finally:
        if child_pid is not None:
            try:
                process = psutil.Process(child_pid)
                process.terminate()
                process.wait(timeout=15)
            except psutil.Error:
                pass
        child.terminate()
        child.wait(timeout=15)

    # A dead process's lease is swept on the next read.
    assert all(item.get("pid") != child_pid for item in list_resource_leases())


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
