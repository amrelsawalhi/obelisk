"""Step outputs, table binding and materialisation.

Three promises are tested here, because the UI and the docs both lean on them:

* a graph that references an output it cannot get is refused at save time;
* a child only sees what its direct parent declared, and a table arrives as a name;
* materialising a raw SQL step produces a real lakehouse dataset.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.engines.pipeline_engine import (
    declared_outputs,
    output_env_name,
    render_plain_step_refs,
    render_step_refs,
)
from app.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _step(key, name, step_type="script", target="zz_test_echo", **extra):
    payload = {"key": key, "name": name, "step_type": step_type, "target": target}
    payload.update(extra)
    return payload


def _graph(steps, edges=None):
    return {"steps": steps, "edges": edges or [], "params": []}


def test_declared_outputs_includes_derived_ones():
    assert declared_outputs("dbt_run", {}) == {"dbt_summary": "scalar"}
    assert declared_outputs("ingest", {}) == {"table": "table", "rows": "scalar"}
    # raw SQL always knows its row count; materialising adds the table name.
    assert declared_outputs(
        "raw_sql", {"materialize": {"folder": "bronze", "name": "orders"}}
    ) == {"rows": "scalar", "table": "table"}
    assert declared_outputs(
        "script", {"outputs": {"rows": {"type": "scalar"}, "sneaky": "table"}}
    ) == {"rows": "scalar", "sneaky": "table"}


def test_render_replaces_scalars_as_literals_and_tables_as_names():
    parents = {
        "extract": {
            "max_id": {"type": "scalar", "value": "O'Brien"},
            "rows": {"type": "scalar", "value": 42},
            "table": {"type": "table", "value": "bronze.orders_delta"},
        }
    }
    sql = (
        "SELECT * FROM {{steps.extract.table}} "
        "WHERE id > {{ steps.extract.rows }} AND name <> {{steps.extract.max_id}}"
    )
    rendered = render_step_refs(sql, parents)
    assert "FROM bronze.orders_delta" in rendered
    assert "id > 42" in rendered
    assert "name <> 'O''Brien'" in rendered
    assert render_plain_step_refs("count={steps.extract.rows}", parents) == "count=42"
    # Unknown references stay visible instead of silently becoming empty.
    assert render_plain_step_refs("{steps.extract.nope}", parents) == "{steps.extract.nope}"
    assert output_env_name("Load Orders", "max_id") == "OBELISK_STEP_LOAD_ORDERS_MAX_ID"


@pytest.mark.asyncio
async def test_undeclared_or_non_parent_output_references_are_rejected():
    async with _client() as client:
        undeclared = await client.post(
            "/api/pipelines",
            json={
                "name": f"refs-{uuid.uuid4().hex[:6]}",
                "graph": _graph(
                    [
                        _step("load", "load", step_type="raw_sql", target="select 1 as n"),
                        _step(
                            "use",
                            "use",
                            step_type="raw_sql",
                            target="select {{steps.load.n}} as n",
                        ),
                    ],
                    [{"from_key": "load", "to_key": "use"}],
                ),
            },
        )
        assert undeclared.status_code == 400
        detail = undeclared.json()["detail"]
        # The message names what the parent does publish, not just that it is wrong.
        assert "declares rows" in detail["message"]
        assert sorted(detail["steps"]) == ["load", "use"]

        # With no edge the referenced step is not a parent at all.
        disconnected = await client.post(
            "/api/pipelines",
            json={
                "name": f"refs-{uuid.uuid4().hex[:6]}",
                "graph": _graph(
                    [
                        _step("load", "load", step_type="raw_sql", target="select 1 as n"),
                        _step(
                            "use",
                            "use",
                            step_type="raw_sql",
                            target="select {{steps.load.n}} as n",
                        ),
                    ]
                ),
            },
        )
        assert disconnected.status_code == 400
        assert "not one of its direct parents" in disconnected.json()["detail"]["message"]

        # A declared scalar plus a table bound from materialize is accepted.
        accepted = await client.post(
            "/api/pipelines",
            json={
                "name": f"refs-{uuid.uuid4().hex[:6]}",
                "graph": _graph(
                    [
                        _step(
                            "load",
                            "load",
                            step_type="raw_sql",
                            target="select 1 as n",
                            args={
                                "materialize": {"folder": "bronze", "name": "orders_delta"},
                                "outputs": {"n": {"type": "scalar"}},
                            },
                        ),
                        _step(
                            "use",
                            "use",
                            step_type="raw_sql",
                            target=(
                                "select * from {{steps.load.table}} "
                                "where n > {{steps.load.n}}"
                            ),
                        ),
                    ],
                    [{"from_key": "load", "to_key": "use"}],
                ),
            },
        )
        assert accepted.status_code == 201, accepted.text
        await client.delete(f"/api/pipelines/{accepted.json()['id']}")


@pytest.mark.asyncio
async def test_materialize_writes_a_dataset_and_publishes_its_name():
    """End to end through the engine: real Parquet, real catalog row, real log line."""
    import asyncio
    import shutil
    import time

    from sqlalchemy import delete, select

    from app.config import settings
    from app.database import AsyncSessionLocal
    from app.engines.pipeline_engine import pipeline_engine
    from app.models.pipeline import PipelineRun, StepRun

    folder = f"zzpipe_{uuid.uuid4().hex[:6]}"
    pipeline_id = None
    pipeline_engine.start()
    try:
        async with _client() as client:
            created = await client.post(
                "/api/pipelines",
                json={
                    "name": f"materialize-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph(
                        [
                            _step(
                                "load",
                                "load",
                                step_type="raw_sql",
                                target="select 1 as n union all select 2 as n",
                                args={"materialize": {"folder": folder, "name": "numbers"}},
                            )
                        ]
                    ),
                },
            )
            assert created.status_code == 201, created.text
            pipeline_id = created.json()["id"]

        run_id = await pipeline_engine.run_now(pipeline_id)
        deadline = time.monotonic() + 90
        status = None
        while time.monotonic() < deadline:
            await pipeline_engine.wake()
            async with AsyncSessionLocal() as session:
                run = await session.get(PipelineRun, run_id)
                if run and run.status in ("success", "failed", "cancelled", "interrupted"):
                    status = run.status
                    break
            await asyncio.sleep(0.2)
        assert status == "success", status

        async with AsyncSessionLocal() as session:
            step_run = (
                await session.execute(select(StepRun).where(StepRun.run_id == run_id))
            ).scalars().one()
        outputs = json.loads(step_run.output_json or "{}")
        # The published value is the address a later step can query, whichever way the
        # materialiser spelled the target.
        assert outputs["table"]["value"] == f"lakehouse.{folder}.numbers"
        # Row count is published without being declared.
        assert outputs["rows"]["value"] == 2
        assert "Materialized 2 row(s)" in (step_run.logs or "")
        # The dataset is durable in the lakehouse, and the catalog is what says so.
        from app.core.catalog import scan

        written = next(
            (t for t in scan().tables if t.fqn == f"lakehouse.{folder}.numbers"), None
        )
        assert written is not None, "the materialised table must appear in the catalog"
        assert written.num_rows == 2
    finally:
        await pipeline_engine.stop()
        # Removing the files is enough: the projection prunes the catalog row itself.
        shutil.rmtree(settings.lakehouse_root_path / folder, ignore_errors=True)
        if pipeline_id:
            async with _client() as client:
                await client.delete(f"/api/pipelines/{pipeline_id}")


@pytest.mark.asyncio
async def test_script_outputs_reach_the_next_step_and_survive_history():
    """What a step declares is what a child can read — nothing else.

    The emitter writes ``{"rows": 7}`` to ``OBELISK_OUTPUT_FILE``; the consumer prints
    ``OBELISK_STEP_EMIT_ROWS``. The value is also copied into ``pipeline_state``, which
    is why pruning the run does not make a re-run of the consumer impossible.
    """
    import asyncio
    import time

    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.engines.pipeline_engine import pipeline_engine
    from app.models.pipeline import (
        PipelineRun,
        PipelineState,
        PipelineStep,
        StepRun,
    )

    root = Path(__file__).resolve().parent.parent.parent
    scripts = root / "git_repo" / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    emit = scripts / "zz_test_emit.py"
    consume = scripts / "zz_test_consume.py"
    emit.write_text(
        "import json, os\n"
        "path = os.environ.get('OBELISK_OUTPUT_FILE')\n"
        "if path:\n"
        "    with open(path, 'w', encoding='utf-8') as fh:\n"
        "        json.dump({'rows': 7, 'note': 'ignored'}, fh)\n"
        "print('emit:done')\n",
        encoding="utf-8",
    )
    consume.write_text(
        "import os\n"
        "print('seen=' + os.environ.get('OBELISK_STEP_EMIT_ROWS', 'missing'))\n",
        encoding="utf-8",
    )

    pipeline_id = None
    pipeline_engine.start()
    try:
        async with _client() as client:
            created = await client.post(
                "/api/pipelines",
                json={
                    "name": f"outputs-{uuid.uuid4().hex[:6]}",
                    "is_enabled": False,
                    "graph": _graph(
                        [
                            _step(
                                "emit",
                                "emit",
                                target="zz_test_emit.py",
                                args={"outputs": {"rows": {"type": "scalar"}}},
                            ),
                            _step("consume", "consume", target="zz_test_consume.py"),
                        ],
                        [{"from_key": "emit", "to_key": "consume"}],
                    ),
                },
            )
            assert created.status_code == 201, created.text
            pipeline_id = created.json()["id"]

        run_id = await pipeline_engine.run_now(pipeline_id)
        deadline = time.monotonic() + 90
        status = None
        while time.monotonic() < deadline:
            await pipeline_engine.wake()
            async with AsyncSessionLocal() as session:
                run = await session.get(PipelineRun, run_id)
                if run and run.status in ("success", "failed", "cancelled", "interrupted"):
                    status = run.status
                    break
            await asyncio.sleep(0.2)
        assert status == "success", status

        async with AsyncSessionLocal() as session:
            runs = (
                await session.execute(select(StepRun).where(StepRun.run_id == run_id))
            ).scalars().all()
            steps = (
                await session.execute(
                    select(PipelineStep).where(PipelineStep.pipeline_id == pipeline_id)
                )
            ).scalars().all()
            state = (
                await session.execute(
                    select(PipelineState).where(PipelineState.pipeline_id == pipeline_id)
                )
            ).scalars().all()

        by_step = {row.step_id: row for row in runs}
        ids = {step.name: step.id for step in steps}
        emit_run = by_step[ids["emit"]]
        consume_run = by_step[ids["consume"]]
        # Only the declared key is published; "note" is not in the declaration.
        assert json.loads(emit_run.output_json or "{}") == {
            "rows": {"type": "scalar", "value": 7}
        }
        assert "output rows = 7" in (emit_run.logs or "")
        assert "seen=7" in (consume_run.logs or "")
        assert {(row.key, json.loads(row.value_json)["value"]) for row in state} == {
            ("emit.rows", 7)
        }
    finally:
        await pipeline_engine.stop()
        emit.unlink(missing_ok=True)
        consume.unlink(missing_ok=True)
        if pipeline_id:
            async with _client() as client:
                await client.delete(f"/api/pipelines/{pipeline_id}")

@pytest.mark.asyncio
async def test_a_name_with_a_space_is_reachable_by_its_slug():
    """Refusing free-form names would break existing graphs; the slug bridges them."""
    async with _client() as client:
        created = await client.post(
            "/api/pipelines",
            json={
                "name": f"slug-{uuid.uuid4().hex[:6]}",
                "graph": _graph(
                    [
                        _step(
                            "load",
                            "Load orders",
                            step_type="raw_sql",
                            target="select 1 as n",
                            args={"outputs": {"n": {"type": "scalar"}}},
                        ),
                        _step(
                            "use",
                            "use",
                            step_type="raw_sql",
                            target="select {{steps.Load_orders.n}} as n",
                        ),
                    ],
                    [{"from_key": "load", "to_key": "use"}],
                ),
            },
        )
        assert created.status_code == 201, created.text
        await client.delete(f"/api/pipelines/{created.json()['id']}")



def test_structured_values_are_interpolated_as_json():
    """A dict must not reach a webhook body as Python repr (single quotes, None)."""
    parents = {
        "dbt": {
            "dbt_summary": {
                "type": "scalar",
                "value": {"total": 9, "failed": 0, "failures": []},
            }
        }
    }
    rendered = render_plain_step_refs('{"summary": "{steps.dbt.dbt_summary}"}', parents)
    assert rendered == '{"summary": "{\"total\":9,\"failed\":0,\"failures\":[]}"}'
    sql = render_step_refs("select {{steps.dbt.dbt_summary}} as s", parents)
    assert chr(34) + "total" + chr(34) + ":9" in sql
    assert "None" not in sql


@pytest.mark.asyncio
async def test_a_pipeline_without_steps_refuses_to_run():
    async with _client() as client:
        created = await client.post(
            "/api/pipelines", json={"name": f"empty-{uuid.uuid4().hex[:6]}"}
        )
        assert created.status_code == 201
        pipeline_id = created.json()["id"]
        refused = await client.post(f"/api/pipelines/{pipeline_id}/run", json={})
        assert refused.status_code == 400
        assert "no steps" in refused.json()["detail"]["message"]
        await client.delete(f"/api/pipelines/{pipeline_id}")
