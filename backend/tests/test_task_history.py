"""Phase 1b — task history survives a restart (M3/H10).

Before this, ``IngestJobManager._jobs``, ``DbtRunner._runs_history`` and
``DeployTracker._jobs`` lived only in process memory: every restart erased the run
history, and anything mid-flight stayed "running" forever because its thread was
gone. These tests pin the durable behaviour, not the in-memory cache.
"""
import sqlite3
import uuid

from app.config import settings
from app.core.deploy_tracker import DeployTracker
from app.engines.dbt_runner import DbtRunner
from app.engines.ingest_job_manager import IngestJobManager
from app.services.state import task_history


def _cleanup(*pairs: tuple[str, str]) -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        for kind, task_id in pairs:
            con.execute("DELETE FROM task_history WHERE kind = ? AND id = ?", (kind, task_id))
        con.commit()


def _stamp(day: int) -> str:
    return f"2026-03-{day:02d}T00:00:00+00:00"


def test_records_round_trip_newest_first() -> None:
    ids = [str(uuid.uuid4()) for _ in range(3)]
    try:
        for index, task_id in enumerate(ids):
            assert task_history.record(
                "ingest", task_id, "completed", _stamp(index + 1), {"id": task_id, "n": index}
            )

        newest = task_history.recent("ingest", limit=500)
        mine = [row["id"] for row in newest if row["id"] in set(ids)]
        assert mine == [ids[2], ids[1], ids[0]]

        assert task_history.fetch("ingest", ids[0]) == {"id": ids[0], "n": 0}
    finally:
        _cleanup(*[("ingest", task_id) for task_id in ids])


def test_mark_interrupted_closes_out_orphaned_runs() -> None:
    orphan, finished = str(uuid.uuid4()), str(uuid.uuid4())
    try:
        task_history.record(
            "ingest", orphan, "running", _stamp(1), {"id": orphan, "status": "running"}
        )
        task_history.record(
            "ingest", finished, "completed", _stamp(2), {"id": finished, "status": "completed"}
        )

        assert task_history.mark_interrupted(["ingest"]) >= 1

        assert task_history.fetch("ingest", orphan)["status"] == "interrupted"
        assert task_history.fetch("ingest", finished)["status"] == "completed"
    finally:
        _cleanup(("ingest", orphan), ("ingest", finished))


def test_ingest_job_survives_a_restart() -> None:
    manager = IngestJobManager()
    job = manager.create_job(
        title="probe job",
        job_type="single",
        connection_id="conn-1",
        source_name="source",
        source_type="postgres",
        mode="snapshot",
    )
    try:
        stored = task_history.fetch("ingest", job.id)
        assert stored is not None and stored["status"] == "running"
        assert stored["title"] == "probe job"

        job.status = "completed"
        job.completed_at = _stamp(3)
        job.total_rows = 7
        manager.update_progress(job.id, current_phase="Finished")

        stored = task_history.fetch("ingest", job.id)
        assert stored["status"] == "completed"
        assert stored["total_rows"] == 7

        # A brand-new manager stands in for the process after a restart.
        revived = IngestJobManager()
        assert revived.hydrate(limit=50) >= 1
        restored = revived.get_job(job.id)
        assert restored is not None
        assert restored.status == "completed"
        assert job.id not in {row["id"] for row in revived.get_active_jobs()}
    finally:
        _cleanup(("ingest", job.id))


def test_cancelled_job_is_recorded_immediately() -> None:
    manager = IngestJobManager()
    job = manager.create_job(
        title="cancel me",
        job_type="batch",
        connection_id="conn-2",
        source_name="source",
        source_type="mysql",
        mode="snapshot",
        total_tables=3,
    )
    try:
        assert manager.cancel_job(job.id) is True
        stored = task_history.fetch("ingest", job.id)
        assert stored["status"] == "cancelled"
        assert stored["is_cancelled"] is True
    finally:
        _cleanup(("ingest", job.id))


def test_dbt_run_history_survives_a_restart() -> None:
    run_id = str(uuid.uuid4())
    run = {
        "run_id": run_id,
        "pipeline_name": "probe pipeline",
        "selector": "marts.*",
        "status": "success",
        "started_at": _stamp(4),
        "completed_at": _stamp(5),
        "logs": "ok",
    }
    try:
        DbtRunner()._record_run(dict(run))

        stored = task_history.fetch("dbt", run_id)
        assert stored is not None
        assert stored["pipeline_name"] == "probe pipeline"
        assert stored["status"] == "success"

        # A fresh runner has an empty in-memory list and must read the table.
        revived = DbtRunner()
        assert revived._runs_history == []
        history = revived.get_runs_history()
        assert any(entry.get("run_id") == run_id for entry in history)

        stamps = [
            entry["started_at"]
            for entry in history
            if isinstance(entry.get("started_at"), str)
        ]
        assert stamps == sorted(stamps, reverse=True)
    finally:
        _cleanup(("dbt", run_id))


def test_deploy_progress_is_recorded() -> None:
    tracker = DeployTracker()
    service = f"zzprobe{uuid.uuid4().hex[:6]}"
    try:
        tracker.start(service)
        assert task_history.fetch("deploy", service)["active"] is True

        tracker.update(service, phase="pulling", pull_percent=42.5)
        tracker.finish(service, "ready", "service is up")

        stored = task_history.fetch("deploy", service)
        assert stored["phase"] == "ready"
        assert stored["active"] is False
        assert stored["detail"] == "service is up"
    finally:
        _cleanup(("deploy", service))


def test_failed_deploy_records_the_error() -> None:
    tracker = DeployTracker()
    service = f"zzprobe{uuid.uuid4().hex[:6]}"
    try:
        tracker.start(service)
        tracker.fail(service, "image pull failed")
        stored = task_history.fetch("deploy", service)
        assert stored["phase"] == "failed"
        assert stored["error"] == "image pull failed"
    finally:
        _cleanup(("deploy", service))
