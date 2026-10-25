"""dbt owns its schemas; Obelisk only reads the names.

There is no layer table and no layer→schema map any more. ``dbt_project.yml`` declares
``+schema`` per model folder, dbt writes there, and Obelisk stages from and into the
matching warehouse folder. These tests pin that wiring — including a guard that stops
anyone reintroducing a medallion schema or a layer variable.
"""
import re
import sqlite3
from pathlib import Path

from app.config import settings
from app.engines.dbt_runner import dbt_runner

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "git_repo" / "dbt_project"


def _project_sources() -> list[Path]:
    return [
        path
        for path in PROJECT_DIR.rglob("*")
        if path.suffix in {".sql", ".yml", ".yaml"}
        and "target" not in path.parts
        and "dbt_packages" not in path.parts
    ]


def test_the_project_declares_a_schema_per_model_folder() -> None:
    folders = dbt_runner._dbt_folders()
    assert set(folders) == {"staging", "marts"}
    for name in folders.values():
        assert name and "/" not in name, name


def test_no_layer_variables_survive_anywhere_in_the_project() -> None:
    """A `var('layer_...')` is the old mapping creeping back in."""
    forbidden = re.compile(r"layer_(staging|marts|schemas)", re.IGNORECASE)
    offenders = []
    for path in _project_sources():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if forbidden.search(line):
                offenders.append(
                    f"{path.relative_to(PROJECT_DIR).as_posix()}:{number}: {line.strip()}"
                )
    assert offenders == [], "layer variable in:\n" + "\n".join(offenders)


def test_no_model_hardcodes_a_medallion_schema() -> None:
    """One `schema='silver'` would put every model back in a fixed tier."""
    forbidden = re.compile(
        r"schema\s*=\s*['\"](bronze|silver|gold)['\"]"
        r"|\+schema:\s*(bronze|silver|gold)\b"
        r"|^\s*schema:\s*(bronze|silver|gold)\s*$"
        r"|source\(\s*['\"](bronze|silver|gold)['\"]",
        re.IGNORECASE | re.MULTILINE,
    )
    offenders = []
    for path in _project_sources():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if forbidden.search(line):
                offenders.append(
                    f"{path.relative_to(PROJECT_DIR).as_posix()}:{number}: {line.strip()}"
                )
    assert offenders == [], "medallion schema hardcoded in:\n" + "\n".join(offenders)


def test_obelisk_passes_no_vars_to_dbt() -> None:
    assert dbt_runner._dbt_vars_payload() == {}


def test_the_schema_macro_uses_the_custom_schema_verbatim() -> None:
    macro = (PROJECT_DIR / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8")
    assert "custom_schema_name" in macro
    assert "target.schema" in macro
    assert "var(" not in macro


def test_a_model_output_is_found_by_its_file_not_by_registration() -> None:
    """The catalog follows the warehouse: a model that wrote a file is a table."""
    from app.core.catalog import fqn_for, sql_name_for

    folders = dbt_runner._dbt_folders()
    for folder, schema in (("staging", folders["staging"]), ("marts", folders["marts"])):
        path = f"{schema}/{folder}.parquet"
        assert fqn_for(path) == f"lakehouse.{schema}.{folder}"
        assert sql_name_for(path) == f"lakehouse.{schema}.{folder}"

    # Nothing claims a model exists before its file does.
    with sqlite3.connect(settings.sqlite_db_path) as con:
        invented = con.execute(
            "SELECT COUNT(*) FROM catalog_objects WHERE name = 'fct_revenue'"
            " AND location LIKE 'postgresql://%'"
        ).fetchone()[0]
    assert invented == 0


def test_asset_graph_nodes_are_keyed_by_the_schema_each_folder_declares() -> None:
    """Graph nodes point at the schema the model lands in, not at a fixed tier."""
    graph = dbt_runner.get_asset_graph()
    folders = dbt_runner._dbt_folders()

    assert folders["staging"] in graph["schemas"]
    assert folders["marts"] in graph["schemas"]
    assert len(graph["schemas"]) == len(set(graph["schemas"]))
    assert {"bronze", "silver", "gold"}.isdisjoint(graph["schemas"])

    ids = {node["id"] for node in graph["nodes"]}
    assert f"{folders['staging']}.stg_orders" in ids
    assert f"{folders['marts']}.fct_revenue" in ids


