"""Pipelines that depend on pipelines.

A ``pipeline`` step runs another pipeline and optionally waits for it. These tests drive
the real engine: the child is a real run, its status is real, and lineage is read back
through the API the UI uses.
"""
from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.engines.pipeline_engine import pipeline_engine
from app.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _graph(steps, edges=None):
    return {"steps": steps, "edges": edges or [], "params": []}


def _step(key, name, step_type="raw_sql", target="select 1 as n", **extra):
    payload = {
        "key": key,
        "name": name,
        "step_type": step_type,
        "target": target,
        "position_x": 0,
        "position_y": 0,
    }
    payload.update(extra)
    return payload


async def _wait(client: AsyncClient, run_id: str, timeout: float = 90) -> dict:
    deadline = time.monotonic() + timeout
    detail: dict = {}
    while time.monotonic() < deadline:
        await pipeline_engine.wake()
        detail = (await client.get(f"/api/pipeline-runs/{run_id}")).json()
        if detail.get("status") in ("success", "failed", "cancelled", "interrupted", "skipped"):
            return detail
        await asyncio.sleep(0.3)
    raise AssertionError(f"run {run_id} did not finish: {detail.get('status')}")


@pytest.fixture
async def engine():
    pipeline_engine.start()
    yield
    await pipeline_engine.stop()


@pytest.mark.asyncio
async def test_a_pipeline_step_runs_and_waits_for_another_pipeline(engine):
    async with _client() as client:
        child = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"child-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph([_step("s", "one", target="select 41 + 1 as n")]),
                },
            )
        ).json()
        parent = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"parent-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph([_step("run", "run child", step_type="pipeline", target=child["id"])]),
                },
            )
        ).json()

        queued = await client.post(f"/api/pipelines/{parent['id']}/run", json={})
        assert queued.status_code == 202, queued.text
        detail = await _wait(client, queued.json()["run_id"])

        assert detail["status"] == "success", detail
        step = detail["step_runs"][0]
        assert step["status"] == "success"
        assert step["outputs"]["status"]["value"] == "success"
        # The child run is linked to the step that started it.
        child_run = (await client.get(f"/api/pipeline-runs/{step['outputs']['run_id']['value']}")).json()
        assert child_run["pipeline_id"] == child["id"]
        assert child_run["parent_run_id"] == detail["id"]
        assert child_run["parent_step_run_id"] == step["id"]
        assert child_run["parent"]["pipeline_name"] == parent["name"]
        assert child_run["parent"]["step_name"] == "run child"

        await client.delete(f"/api/pipelines/{parent['id']}")
        await client.delete(f"/api/pipelines/{child['id']}")


@pytest.mark.asyncio
async def test_a_failing_child_fails_the_step(engine):
    async with _client() as client:
        child = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"bad-child-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph([_step("s", "one", target="select * from missing_table")]),
                },
            )
        ).json()
        parent = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"bad-parent-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph([_step("run", "run child", step_type="pipeline", target=child["name"])]),
                },
            )
        ).json()

        queued = await client.post(f"/api/pipelines/{parent['id']}/run", json={})
        detail = await _wait(client, queued.json()["run_id"])

        assert detail["status"] == "failed"
        step = detail["step_runs"][0]
        assert step["status"] == "failed"
        assert "finished as failed" in (step["error"] or "")

        await client.delete(f"/api/pipelines/{parent['id']}")
        await client.delete(f"/api/pipelines/{child['id']}")


@pytest.mark.asyncio
async def test_impossible_dependencies_are_refused_at_save_time():
    async with _client() as client:
        first = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"dep-a-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph([_step("s", "one")]),
                },
            )
        ).json()
        second = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"dep-b-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph(
                        [_step("run", "run a", step_type="pipeline", target=first["id"])]
                    ),
                },
            )
        ).json()

        # A -> B would close the loop A -> B -> A.
        cycle = await client.put(
            f"/api/pipelines/{first['id']}/graph",
            json=_graph(
                [_step("run", "run b", step_type="pipeline", target=second["id"])]
            ),
        )
        assert cycle.status_code == 400
        assert "cycle" in cycle.json()["detail"]["message"].lower()
        assert cycle.json()["detail"]["steps"] == ["run"]

        # Running yourself.
        self_run = await client.put(
            f"/api/pipelines/{first['id']}/graph",
            json=_graph([_step("run", "me", step_type="pipeline", target=first["id"])]),
        )
        assert self_run.status_code == 400
        assert "itself" in self_run.json()["detail"]["message"]

        # A target that does not exist.
        ghost = await client.put(
            f"/api/pipelines/{first['id']}/graph",
            json=_graph([_step("run", "ghost", step_type="pipeline", target="no-such-pipeline")]),
        )
        assert ghost.status_code == 400
        assert "does not exist" in ghost.json()["detail"]["message"]

        # Parameters the child never declared.
        invented = await client.put(
            f"/api/pipelines/{first['id']}/graph",
            json=_graph(
                [
                    _step(
                        "run",
                        "run b",
                        step_type="pipeline",
                        target=second["id"],
                        args={"params": {"made_up": 1}},
                    )
                ]
            ),
        )
        assert invented.status_code == 400
        assert "made_up" in invented.json()["detail"]["message"]

        await client.delete(f"/api/pipelines/{second['id']}")
        await client.delete(f"/api/pipelines/{first['id']}")


@pytest.mark.asyncio
async def test_parameters_flow_into_the_child_run(engine):
    async with _client() as client:
        child = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"p-child-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph(
                        [_step("s", "one", target="select {{batch}} as n")],
                        [],
                    ),
                },
            )
        ).json()
        # Declare the parameter on the child so the parent may pass it.
        await client.put(
            f"/api/pipelines/{child['id']}/graph",
            json={
                "steps": [_step("s", "one", target="select {{batch}} as n")],
                "edges": [],
                "params": [
                    {"name": "batch", "type": "int", "default_value": "1", "required": False}
                ],
            },
        )
        parent = (
            await client.post(
                "/api/pipelines",
                json={
                    "name": f"p-parent-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph(
                        [
                            _step(
                                "run",
                                "run child",
                                step_type="pipeline",
                                target=child["id"],
                                args={"params": {"batch": 7}},
                            )
                        ]
                    ),
                },
            )
        ).json()

        queued = await client.post(f"/api/pipelines/{parent['id']}/run", json={})
        detail = await _wait(client, queued.json()["run_id"])
        assert detail["status"] == "success", detail
        child_run_id = detail["step_runs"][0]["outputs"]["run_id"]["value"]
        child_run = (await client.get(f"/api/pipeline-runs/{child_run_id}")).json()
        assert child_run["params"]["batch"] == 7

        await client.delete(f"/api/pipelines/{parent['id']}")
        await client.delete(f"/api/pipelines/{child['id']}")
