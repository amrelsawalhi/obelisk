"""dbt targets come from the project, not from the application.

The Studio used to hardcode two adapters. These tests pin the replacement contract:
`profiles.yml` is the list, the profile's own default is the default, and a pipeline
step that names a target which does not exist is refused at save time.
"""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.engines.dbt_runner import dbt_runner
from app.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def test_targets_are_read_from_the_profile():
    info = dbt_runner.list_targets()
    names = [target["name"] for target in info["targets"]]
    assert names, "profiles.yml must define at least one output"
    assert info["default"] == dbt_runner.default_target()
    for target in info["targets"]:
        assert target["adapter"], "every output declares an adapter type"
        assert isinstance(target["available"], bool)


def test_the_target_name_is_passed_through_to_dbt():
    env = dbt_runner._get_target_env("postgres")
    # Not rewritten to 'dev': dbt must receive the target the operator picked.
    assert env["DBT_TARGET"] == "postgres"


@pytest.mark.asyncio
async def test_targets_endpoint_matches_the_profile():
    async with _client() as client:
        res = await client.get("/api/dbt/targets")
        assert res.status_code == 200, res.text
        payload = res.json()
    local = dbt_runner.list_targets()
    assert [t["name"] for t in payload["targets"]] == [t["name"] for t in local["targets"]]
    assert payload["default"] == local["default"]


@pytest.mark.asyncio
async def test_a_pipeline_step_cannot_name_an_unknown_dbt_target():
    async with _client() as client:
        step = {
            "key": "a",
            "name": "build",
            "step_type": "dbt_run",
            "target": "*",
            "args": {"target": "warehouse-that-does-not-exist"},
            "position_x": 0,
            "position_y": 0,
        }
        rejected = await client.post(
            "/api/pipelines",
            json={
                "name": f"target-{uuid.uuid4().hex[:6]}",
                "graph": {"steps": [step], "edges": [], "params": []},
            },
        )
        assert rejected.status_code == 400
        assert "profiles.yml does not define" in rejected.json()["detail"]["message"]

        step["args"] = {"target": dbt_runner.list_targets()["targets"][0]["name"]}
        accepted = await client.post(
            "/api/pipelines",
            json={
                "name": f"target-{uuid.uuid4().hex[:6]}",
                "graph": {"steps": [step], "edges": [], "params": []},
            },
        )
        assert accepted.status_code == 201, accepted.text
        await client.delete(f"/api/pipelines/{accepted.json()['id']}")
