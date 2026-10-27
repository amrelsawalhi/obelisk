"""dbt runs must not share one ``target/`` directory.

``compile_model`` used to read compiled SQL out of ``dbt_project_dir/target``, which
is the directory every other dbt caller writes to. Two Query Studio compiles — or a
compile racing a pipeline dbt run — could therefore return SQL compiled for someone
else's model. Each call now compiles into its own scratch directory
(``dbt_compile_<uuid>`` under the temp root) and reads the SQL from there only.

These tests drive real dbt against the shipped project. ``dbt parse`` needs no
warehouse, and the lakehouse target is a local DuckDB file, so both paths run
offline.
"""
from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from app.config import settings
from app.engines.dbt_runner import dbt_runner

SHARED_TARGET = Path(dbt_runner.dbt_project_dir) / "target"
SCRATCH_PREFIX = "dbt_compile_"


def _a_real_model() -> str | None:
    """A model that exists in the project on disk.

    The project's models are local demo content and are not in the repo, so the model
    is discovered rather than named — a hardcoded name goes stale the moment the demo
    workspace changes.
    """
    for folder in ("staging", "marts"):
        models = sorted((Path(dbt_runner.dbt_project_dir) / "models" / folder).glob("*.sql"))
        if models:
            return models[0].stem
    return None


def _scratch_dirs() -> set[Path]:
    """Scratch directories that exist right now; empty means every call cleaned up."""
    root = Path(settings.duckdb_temp_path)
    return {p for p in root.glob(f"{SCRATCH_PREFIX}*")} if root.exists() else set()


def _shared_manifest_stamp() -> int | None:
    manifest = SHARED_TARGET / "manifest.json"
    return manifest.stat().st_mtime_ns if manifest.exists() else None


def test_run_scoped_dbt_writes_artifacts_to_the_given_target_path(tmp_path):
    """``--target-path`` really redirects the run: nothing lands in the shared target."""
    target_path = tmp_path / "isolated-target"
    shared_before = _shared_manifest_stamp()

    result = dbt_runner.run_scoped_dbt(
        command="parse", target=dbt_runner.default_target(), target_path=target_path
    )

    assert result["success"], result["output"][-3000:]
    assert (target_path / "manifest.json").exists(), sorted(p.name for p in target_path.iterdir())
    assert _shared_manifest_stamp() == shared_before, (
        "a scoped dbt run must not rewrite the project's shared target/"
    )


def test_compile_model_ignores_artifacts_left_in_the_shared_target():
    """A stale artifact in the shared target must never become someone's compiled SQL.

    The decoy is exactly what the pre-isolation code would have read: the file the
    shared ``target/compiled`` tree holds for this model name. Because the model does
    not exist, dbt selects nothing, so no warehouse is contacted.
    """
    model = f"zz_isolation_{uuid.uuid4().hex[:8]}"
    decoy_dir = SHARED_TARGET / "compiled" / "zz_isolation"
    decoy = decoy_dir / f"{model}.sql"
    decoy.parent.mkdir(parents=True, exist_ok=True)
    decoy.write_text("select 'stale artifact from another caller';", encoding="utf-8")
    scratch_before = _scratch_dirs()
    try:
        result = dbt_runner.compile_model(model, target=dbt_runner.default_target())
    finally:
        shutil.rmtree(decoy_dir, ignore_errors=True)

    assert "stale artifact" not in result["compiled_sql"]
    assert result["success"] is False, "nothing was selected, so there is no compiled SQL"
    assert _scratch_dirs() == scratch_before, (
        "the scratch target directory must be removed when compilation fails too"
    )


def test_compile_model_compiles_a_real_model_and_cleans_its_scratch_dir():
    """The happy path: real compiled SQL, and no scratch directory left behind."""
    model = _a_real_model()
    if model is None:
        pytest.skip("the project on disk ships no models (the demo models are local content)")

    shared_before = _shared_manifest_stamp()
    scratch_before = _scratch_dirs()

    result = dbt_runner.compile_model(model, target=dbt_runner.default_target())

    assert result["success"], result["logs"][-3000:]
    compiled = result["compiled_sql"].strip().lower()
    # Any real model compiles to SQL that reads something; only a stub would not.
    assert "select" in compiled, compiled[:200]
    assert "from" in compiled, compiled[:200]
    assert _scratch_dirs() == scratch_before, "the scratch target directory must be removed"
    assert _shared_manifest_stamp() == shared_before, (
        "compiling one model must not rewrite the project's shared target/"
    )
