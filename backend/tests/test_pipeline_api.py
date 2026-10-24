"""The pipelines API: validation, CRUD and the run/step-log surface.

These tests go through the real ASGI app (no mocks, no monkeypatching) and assert
the boundary behaviour the UI depends on: an impossible graph is refused with a
readable message, a run can be queued, its step logs can be tailed, and toggling a
schedule clears its anchor.
"""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _graph(steps, edges=None, params=None):
    return {
        "steps": steps,
        "edges": edges or [],
        "params": params or [],
    }


def _step(key, name, step_type="script", target="zz_test_echo", **extra):
    payload = {"key": key, "name": name, "step_type": step_type, "target": target}
    payload.update(extra)
    return payload


@pytest.mark.asyncio
async def test_create_pipeline_with_graph_and_read_it_back():
    name = f"api-{uuid.uuid4().hex[:8]}"
    async with _client() as client:
        res = await client.post(
            "/api/pipelines",
            json={
                "name": name,
                "cron_expression": "*/15 * * * *",
                "timezone": "Europe/Berlin",
                "graph": _graph(
                    [_step("a", "extract"), _step("b", "load")],
                    [{"from_key": "a", "to_key": "b", "condition": "success"}],
                    [{"name": "batch", "type": "int", "default_value": "10"}],
                ),
            },
        )
        assert res.status_code == 201, res.text
        pipeline = res.json()
        assert pipeline["step_count"] == 2
        assert pipeline["next_run_at"] is not None

        detail = (await client.get(f"/api/pipelines/{pipeline['id']}")).json()
        assert {s["name"] for s in detail["steps"]} == {"extract", "load"}
        assert detail["edges"][0]["condition"] == "success"
        assert detail["params"][0]["name"] == "batch"

        listed = (await client.get("/api/pipelines")).json()
        assert any(p["id"] == pipeline["id"] for p in listed)

        # A second pipeline with the same name is refused.
        again = await client.post("/api/pipelines", json={"name": name})
        assert again.status_code == 400

        deleted = await client.delete(f"/api/pipelines/{pipeline['id']}")
        assert deleted.json()["status"] == "deleted"
        assert (await client.get(f"/api/pipelines/{pipeline['id']}")).status_code == 404


@pytest.mark.asyncio
async def test_graph_validation_rejects_impossible_pipelines():
    async with _client() as client:
        cycle = await client.post(
            "/api/pipelines",
            json={
                "name": f"cycle-{uuid.uuid4().hex[:6]}",
                "graph": _graph(
                    [_step("a", "a"), _step("b", "b")],
                    [
                        {"from_key": "a", "to_key": "b"},
                        {"from_key": "b", "to_key": "a"},
                    ],
                ),
            },
        )
        assert cycle.status_code == 400
        assert "Cycle detected" in cycle.json()["detail"]

        dangling = await client.post(
            "/api/pipelines",
            json={
                "name": f"dangling-{uuid.uuid4().hex[:6]}",
                "graph": _graph([_step("a", "a")], [{"from_key": "a", "to_key": "ghost"}]),
            },
        )
        assert dangling.status_code == 400
        assert "unknown step" in dangling.json()["detail"]

        no_target = await client.post(
            "/api/pipelines",
            json={
                "name": f"target-{uuid.uuid4().hex[:6]}",
                "graph": _graph([_step("a", "a", target="")]),
            },
        )
        assert no_target.status_code == 400
        assert "requires a target" in no_target.json()["detail"]

        bad_cron = await client.post(
            "/api/pipelines",
            json={"name": f"cron-{uuid.uuid4().hex[:6]}", "cron_expression": "not a cron"},
        )
        assert bad_cron.status_code == 400

        bad_tz = await client.post(
            "/api/pipelines",
            json={"name": f"tz-{uuid.uuid4().hex[:6]}", "timezone": "Mars/Olympus"},
        )
        assert bad_tz.status_code == 400

        unknown_type = await client.post(
            "/api/pipelines",
            json={
                "name": f"type-{uuid.uuid4().hex[:6]}",
                "graph": _graph([_step("a", "a", step_type="quantum")]),
            },
        )
        assert unknown_type.status_code == 400


@pytest.mark.asyncio
async def test_schedule_preview_toggle_and_graph_replacement():
    async with _client() as client:
        created = await client.post(
            "/api/pipelines",
            json={
                "name": f"schedule-{uuid.uuid4().hex[:6]}",
                "cron_expression": "0 3 * * *",
                "timezone": "UTC",
                "graph": _graph([_step("a", "only")]),
            },
        )
        pipeline = created.json()
        pipeline_id = pipeline["id"]

        preview = await client.get(
            "/api/pipelines/preview",
            params={"cron_expression": "0 3 * * *", "timezone": "UTC", "count": 3},
        )
        assert preview.status_code == 200
        assert len(preview.json()["next_runs"]) == 3

        toggled = await client.post(f"/api/pipelines/{pipeline_id}/toggle")
        assert toggled.json()["is_enabled"] is False
        assert toggled.json()["next_run_at"] is None
        toggled_back = await client.post(f"/api/pipelines/{pipeline_id}/toggle")
        assert toggled_back.json()["is_enabled"] is True
        assert toggled_back.json()["next_run_at"] is not None

        # Replacing the graph keeps the id of a step whose key is that id.
        original_id = (await client.get(f"/api/pipelines/{pipeline_id}")).json()["steps"][0]["id"]
        replaced = await client.put(
            f"/api/pipelines/{pipeline_id}/graph",
            json=_graph(
                [_step(original_id, "only"), _step("new", "extra")],
                [{"from_key": original_id, "to_key": "new", "condition": "completion"}],
            ),
        )
        assert replaced.status_code == 200
        detail = (await client.get(f"/api/pipelines/{pipeline_id}")).json()
        assert len(detail["steps"]) == 2
        assert detail["edges"][0]["condition"] == "completion"

        await client.delete(f"/api/pipelines/{pipeline_id}")


@pytest.mark.asyncio
async def test_run_now_appears_in_history_with_step_logs():
    async with _client() as client:
        pipeline = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"run-{uuid.uuid4().hex[:6]}",
                    "graph": _graph([_step("a", "solo")]),
                },
            )
        ).json()

        queued = await client.post(f"/api/pipelines/{pipeline['id']}/run", json={})
        assert queued.status_code == 202
        run_id = queued.json()["run_id"]

        runs = (await client.get("/api/pipeline-runs", params={"limit": 50})).json()
        match = next(r for r in runs if r["id"] == run_id)
        assert match["pipeline_name"] == pipeline["name"]
        assert match["status"] in ("pending", "running")

        detail = (await client.get(f"/api/pipeline-runs/{run_id}")).json()
        assert len(detail["step_runs"]) == 1
        step_run = detail["step_runs"][0]
        assert step_run["step_name"] == "solo"

        # A run that has not executed has an empty log tail, not an error.
        logs = (await client.get(f"/api/step-runs/{step_run['id']}/logs")).json()
        assert logs["size"] == 0
        assert logs["logs"] == ""

        # Cancelling it is allowed once; the second attempt is a 409.
        cancelled = await client.post(f"/api/pipeline-runs/{run_id}/cancel")
        assert cancelled.status_code == 200
        again = await client.post(f"/api/pipeline-runs/{run_id}/cancel")
        assert again.status_code == 409

        await client.delete(f"/api/pipelines/{pipeline['id']}")
        assert (await client.get(f"/api/pipeline-runs/{run_id}")).status_code == 404


@pytest.mark.asyncio
async def test_run_log_concatenates_every_step():
    """The whole-run log keeps step boundaries so it can be read as a narrative."""
    async with _client() as client:
        pipeline = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"logs-{uuid.uuid4().hex[:6]}",
                    "graph": _graph(
                        [_step("a", "first", target="zz_missing_a"), _step("b", "second")],
                        [{"from_key": "a", "to_key": "b"}],
                    ),
                },
            )
        ).json()

        run_id = (await client.post(f"/api/pipelines/{pipeline['id']}/run", json={})).json()[
            "run_id"
        ]
        payload = (await client.get(f"/api/pipeline-runs/{run_id}/logs")).json()

        assert payload["run_id"] == run_id
        assert payload["size"] == len(payload["logs"])
        assert "Pipeline :" in payload["logs"]
        # Both steps appear with their status, even before anything executed.
        assert "STEP first" in payload["logs"]
        assert "STEP second" in payload["logs"]

        missing = await client.get("/api/pipeline-runs/does-not-exist/logs")
        assert missing.status_code == 404

        await client.delete(f"/api/pipelines/{pipeline['id']}")


@pytest.mark.asyncio
async def test_targets_lists_real_workspace_files():
    async with _client() as client:
        targets = (await client.get("/api/pipelines/targets")).json()
        assert "daily_kpi_summary.py" in targets["scripts"]
        assert targets["notebooks"], "shipped notebooks must be offered as targets"
        assert set(targets["step_types"]) == {
            "dbt_run",
            "dbt_test",
            "script",
            "notebook",
            "webhook",
            "raw_sql",
        }
        assert set(targets["edge_conditions"]) == {
            "success",
            "failure",
            "completion",
            "skipped",
        }

