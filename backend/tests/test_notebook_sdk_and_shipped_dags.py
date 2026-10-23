"""Regression tests for the shipped notebook shim and sample DAG (audit B5, B6).

B5: notebooks run with cwd=git_repo/notebooks, so `import obelisk` resolves to the
    shim there. It omitted `var`, which made obelisk.var() raise AttributeError
    inside every Marimo notebook.
B6: obelisk.query() already returns a Polars DataFrame, so calling .to_polars()
    on it raised AttributeError. The shipped DAG and the pipeline template did
    exactly that, and the pipeline caught the error and silently reported 0 rows.
"""
import importlib.util
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NOTEBOOK_DIR = PROJECT_ROOT / "git_repo" / "notebooks"
DAG_DIR = PROJECT_ROOT / "git_repo" / "dags"
SCRIPT_DIR = PROJECT_ROOT / "git_repo" / "scripts"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader, f"cannot load {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_notebook_shim_exports_var():
    """B5: the notebook shim must expose obelisk.var for notebook kernels."""
    shim = _load_module(NOTEBOOK_DIR / "obelisk.py", "obelisk_notebook_shim")

    assert hasattr(shim, "var"), "git_repo/notebooks/obelisk.py is missing var()"
    assert "var" in shim.__all__


def test_query_returns_a_polars_frame():
    """B6: query() returns a DataFrame, which has no .to_polars()."""
    from app.sdk import obelisk as sdk

    frame = sdk.query("SELECT 1 AS one")

    assert isinstance(frame, pl.DataFrame)
    assert not hasattr(frame, "to_polars")


def test_shipped_samples_do_not_call_to_polars_on_query_results():
    """B6: no shipped DAG or script may re-introduce the broken chain."""
    offenders = []
    for directory in (DAG_DIR, SCRIPT_DIR):
        if not directory.is_dir():
            continue
        for path in directory.glob("*.py"):
            if ".to_polars()" in path.read_text(encoding="utf-8"):
                offenders.append(path.name)

    assert offenders == [], f".to_polars() on a query result in: {offenders}"


def test_pipeline_template_does_not_call_to_polars():
    """B6: the generated Dagster/KPI template had the same bug."""
    source = (
        PROJECT_ROOT / "backend" / "app" / "api" / "routes_pipeline.py"
    ).read_text(encoding="utf-8")

    assert "obelisk.query(" in source
    assert ".to_polars()" not in source
