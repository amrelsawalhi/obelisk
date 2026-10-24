"""The bundled dbt project targets layers, not fixed tiers.

A workspace can call its layers anything, so the pipeline has to follow: the project
declares two configurable slots, Obelisk resolves each slot's layer path to a schema,
and the macro applies the mapping. These tests pin that wiring down — including a
guard that stops anyone reintroducing `schema='silver'` in a model.
"""
import re
import sqlite3
from pathlib import Path

from app.config import settings
from app.core.layers import resolve_layer
from app.engines.dbt_runner import dbt_runner
from app.services.layers import layer_schemas

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "git_repo" / "dbt_project"


def _project_sources() -> list[Path]:
    return [
        path
        for path in PROJECT_DIR.rglob("*")
        if path.suffix in {".sql", ".yml", ".yaml"}
        and "target" not in path.parts
        and "dbt_packages" not in path.parts
    ]


def test_shipped_project_declares_layer_slots() -> None:
    targets = dbt_runner._dbt_layer_vars()
    assert set(targets) == {"staging", "marts"}
    for path in targets.values():
        resolve_layer(path)  # every slot must be a valid layer path


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


def test_vars_payload_carries_the_layer_to_schema_map() -> None:
    payload = dbt_runner._dbt_vars_payload()
    targets = dbt_runner._dbt_layer_vars()

    assert payload["layer_staging"] == targets["staging"]
    assert payload["layer_marts"] == targets["marts"]

    mapping = payload["layer_schemas"]
    for path in targets.values():
        assert mapping[path] == resolve_layer(path).warehouse_schema
    # Every layer in the tree can be targeted, not just the two configured slots.
    for path, schema in layer_schemas().items():
        assert mapping.get(path, schema) == schema


def test_schema_macro_consults_the_mapping() -> None:
    macro = (PROJECT_DIR / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8")
    assert "var('layer_schemas'" in macro
    assert "mapping.get(custom_schema_name)" in macro


def test_folder_for_layer_follows_the_config() -> None:
    targets = dbt_runner._dbt_layer_vars()
    assert dbt_runner._folder_for_layer(targets["staging"]) == "staging"
    assert dbt_runner._folder_for_layer(targets["marts"]) == "marts"
    # A layer with no folder of its own falls back to the marts folder.
    assert dbt_runner._folder_for_layer("marts.sales") == "marts"


def test_catalog_records_the_layer_each_folder_targets() -> None:
    targets = dbt_runner._dbt_layer_vars()
    dbt_runner._catalog_dbt_models()

    with sqlite3.connect(settings.sqlite_db_path) as con:
        # Rows for older layer names may still exist from earlier runs; assert the pair
        # this run wrote rather than assuming it is the only one.
        pairs = set(
            con.execute(
                "SELECT name, layer_path, location FROM catalog_objects"
                " WHERE name IN ('stg_orders', 'fct_revenue')"
            )
        )

    staging_schema = resolve_layer(targets["staging"]).warehouse_schema
    marts_schema = resolve_layer(targets["marts"]).warehouse_schema

    assert (
        "stg_orders",
        targets["staging"],
        f"postgresql:///{staging_schema}.stg_orders",
    ) in pairs
    assert (
        "fct_revenue",
        targets["marts"],
        f"postgresql:///{marts_schema}.fct_revenue",
    ) in pairs


def test_dotted_layer_paths_resolve_to_a_flat_schema() -> None:
    """The reason the mapping exists at all: schemas cannot nest."""
    mapping = dbt_runner._dbt_vars_payload()["layer_schemas"]
    dotted = {path: schema for path, schema in mapping.items() if "." in path}
    for path, schema in dotted.items():
        assert "." not in schema, path
        assert schema == path.replace(".", "__")


def test_asset_graph_is_keyed_by_layer_path() -> None:
    """Graph nodes point at the schema the model landed in, not at a fixed tier."""
    graph = dbt_runner.get_asset_graph()
    targets = dbt_runner._dbt_layer_vars()

    assert targets["staging"] in graph["layers"]
    assert targets["marts"] in graph["layers"]
    assert len(graph["layers"]) == len(set(graph["layers"]))
    assert {"bronze", "silver", "gold"}.isdisjoint(graph["layers"])

    ids = {node["id"] for node in graph["nodes"]}
    assert f"{targets['staging']}.stg_orders" in ids
    assert f"{targets['marts']}.fct_revenue" in ids

