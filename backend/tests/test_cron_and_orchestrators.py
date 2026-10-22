import asyncio

import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_orchestration_engines_registry():
    """
    Verifies the pluggable Orchestrator Hub discovers:
    - Built-in Cron (always active, in-process)
    - Dagster Engine (running or offline)
    - Apache Airflow (running or offline)
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/pipelines/engines")
        assert res.status_code == 200
        data = res.json()
        assert "engines" in data
        engine_ids = [e["id"] for e in data["engines"]]
        assert "cron" in engine_ids
        assert "dagster" in engine_ids
        assert "airflow" in engine_ids

        cron_engine = next(e for e in data["engines"] if e["id"] == "cron")
        assert cron_engine["status"] == "running"
        assert cron_engine["is_builtin"] is True


@pytest.mark.asyncio
async def test_cron_targets_discovery():
    """
    Verifies available targets (scripts, notebooks, dbt models) for the job creation picker.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/pipelines/cron/targets")
        assert res.status_code == 200
        targets = res.json()
        assert "scripts" in targets
        assert "notebooks" in targets
        assert "dbt_selectors" in targets

        # Verify daily_kpi_summary.py is discoverable in git_repo/scripts/
        script_names = [s["filename"] for s in targets["scripts"]]
        assert "daily_kpi_summary.py" in script_names

        # Verify dynamic dbt model discovery from git_repo/dbt_project/models/
        dbt_values = [sel["value"] for sel in targets["dbt_selectors"]]
        assert "*" in dbt_values
        assert "models/staging" in dbt_values or "models/marts" in dbt_values
        assert "stg_orders" in dbt_values
        assert "fct_orders_enriched" in dbt_values
        assert "stg_orders+" in dbt_values


@pytest.mark.asyncio
async def test_cron_job_lifecycle_and_execution():
    """
    Tests complete lifecycle of an Obelisk Cron job:
    1. Create a scheduled job targeting daily_kpi_summary.py
    2. Trigger immediate on-demand execution
    3. Verify execution logs, duration, status in SQLite
    4. Toggle enabled/disabled
    5. Delete job and verify cleanup
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        job_name = "Unit Test Daily KPI Cron"
        create_payload = {
            "name": job_name,
            "description": "Integration test scheduled job",
            "target_type": "script",
            "target": "daily_kpi_summary.py",
            "cron_expression": "0 4 * * *",
        }

        # 1. Create Job
        create_res = await client.post("/api/pipelines/cron/jobs", json=create_payload)
        assert create_res.status_code == 200
        job_data = create_res.json()
        job_id = job_data["id"]
        assert job_data["name"] == job_name
        assert job_data["next_run_at"] is not None

        try:
            # 2. List jobs
            list_res = await client.get("/api/pipelines/cron/jobs")
            assert list_res.status_code == 200
            jobs = list_res.json()
            assert any(j["id"] == job_id for j in jobs)

            # 3. Trigger immediate execution
            run_res = await client.post(f"/api/pipelines/cron/jobs/{job_id}/run")
            assert run_res.status_code == 200, run_res.text
            run_result = run_res.json()
            assert run_result["status"] == "success"
            assert run_result["duration_seconds"] >= 0.0
            assert "Daily KPI Summary execution" in run_result["logs"]

            # 4. Verify updated state in SQLite
            verify_res = await client.get("/api/pipelines/cron/jobs")
            updated_job = next(j for j in verify_res.json() if j["id"] == job_id)
            assert updated_job["last_status"] == "success"
            assert updated_job["last_duration_seconds"] is not None
            assert updated_job["last_logs"] is not None

            # 5. Toggle enable/disable
            toggle_res = await client.patch(f"/api/pipelines/cron/jobs/{job_id}/toggle")
            assert toggle_res.status_code == 200
            assert toggle_res.json()["is_enabled"] is False

        finally:
            # 6. Delete job
            del_res = await client.delete(f"/api/pipelines/cron/jobs/{job_id}")
            assert del_res.status_code == 200

            # Verify deleted from list
            list_after = await client.get("/api/pipelines/cron/jobs")
            assert not any(j["id"] == job_id for j in list_after.json())


@pytest.mark.asyncio
async def test_cron_notebook_job_executes_and_exits():
    """
    Regression guard for scheduled Marimo notebooks.

    `marimo run <notebook>` starts an app server that serves until killed, so
    awaiting communicate() blocked forever: the job stayed "running" (and the poll
    loop permanently skips "running" jobs, so it never ran again) and every
    execution orphaned a process holding the notebook port. Notebook jobs must
    execute-and-exit like script jobs do.
    """
    from app.engines.cron_engine import cron_engine

    # Underscore prefix keeps the probe out of list_available_targets().
    probe_path = cron_engine.notebooks_dir / "_cron_notebook_probe.py"
    probe_path.write_text(
        "import marimo\n"
        "\n"
        "app = marimo.App()\n"
        "\n"
        "\n"
        "@app.cell\n"
        "def _():\n"
        '    print("CRON_NOTEBOOK_PROBE_OUTPUT")\n'
        "    return\n"
        "\n"
        "\n"
        'if __name__ == "__main__":\n'
        "    app.run()\n",
        encoding="utf-8",
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_res = await client.post(
            "/api/pipelines/cron/jobs",
            json={
                "name": "Unit Test Notebook Cron",
                "description": "Verifies notebook jobs execute and exit",
                "target_type": "notebook",
                "target": "_cron_notebook_probe.py",
                "cron_expression": "0 4 * * *",
            },
        )
        assert create_res.status_code == 200, create_res.text
        job_id = create_res.json()["id"]

        try:
            # A regression would hang here forever; fail fast instead of stalling
            # the whole suite.
            run_res = await asyncio.wait_for(
                client.post(f"/api/pipelines/cron/jobs/{job_id}/run"), timeout=120
            )
            assert run_res.status_code == 200, run_res.text
            result = run_res.json()

            assert result["status"] == "success", result["logs"]
            assert "CRON_NOTEBOOK_PROBE_OUTPUT" in result["logs"]
            assert result["next_run_at"] is not None
            # The child must have exited, not been left behind as an orphan.
            assert cron_engine._active_procs == set()

            verify_res = await client.get("/api/pipelines/cron/jobs")
            updated_job = next(j for j in verify_res.json() if j["id"] == job_id)
            assert updated_job["last_status"] == "success"
            assert updated_job["last_duration_seconds"] is not None
        finally:
            await client.delete(f"/api/pipelines/cron/jobs/{job_id}")
            probe_path.unlink(missing_ok=True)
