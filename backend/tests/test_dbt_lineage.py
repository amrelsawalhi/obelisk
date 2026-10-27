"""Lineage means the whole graph, not the neighbours of one model.

``get_model_lineage`` read ``parent_map[id]`` and ``child_map[id]`` once each, so a mart
built on an intermediate model that reads a source showed one hop and stopped. The panel
could not answer "where does this number come from", which is the only question a lineage
view exists to answer.

These tests drive the real method against a real ``manifest.json`` written to a temporary
project directory — the same file shape dbt produces, no mocks.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.engines.dbt_runner import DbtRunner

SOURCE = "source.obelisk_dbt.landing.orders"
STG = "model.obelisk_dbt.stg_orders"
DIM = "model.obelisk_dbt.dim_customers"
FCT = "model.obelisk_dbt.fct_revenue"
MART = "model.obelisk_dbt.mart_daily_revenue"
TEST = "test.obelisk_dbt.not_null_fct_revenue_id.abc123"


def _project_with(tmp_path: Path, parents: dict, children: dict) -> DbtRunner:
    """A real dbt project directory holding only the artifacts lineage reads."""
    project = tmp_path / "dbt_project"
    (project / "target").mkdir(parents=True, exist_ok=True)
    (project / "models" / "staging").mkdir(parents=True, exist_ok=True)
    (project / "models" / "marts").mkdir(parents=True, exist_ok=True)

    nodes = {
        SOURCE: {"original_file_path": "models/sources.yml", "resource_type": "source"},
        STG: {"original_file_path": "models/staging/stg_orders.sql", "config": {"schema": "raw"}},
        DIM: {"original_file_path": "models/marts/dim_customers.sql", "config": {"schema": "marts"}},
        FCT: {"original_file_path": "models/marts/fct_revenue.sql",
              "config": {"schema": "marts", "materialized": "table"}},
        MART: {"original_file_path": "models/marts/mart_daily_revenue.sql",
               "config": {"schema": "marts"}},
        TEST: {"original_file_path": "models/marts/schema.yml", "config": {"schema": "test"}},
    }
    manifest = {
        "nodes": nodes,
        "parent_map": {key: value for key, value in parents.items()},
        "child_map": {key: value for key, value in children.items()},
    }
    (project / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return DbtRunner(dbt_project_dir=project)


@pytest.fixture()
def lineage_graph(tmp_path):
    """A four-deep chain up and two deep down, exactly what one hop used to truncate."""
    return _project_with(
        tmp_path,
        parents={
            STG: [SOURCE],
            DIM: [STG],
            FCT: [DIM],
            TEST: [FCT],
        },
        children={
            SOURCE: [STG],
            STG: [DIM],
            DIM: [FCT],
            FCT: [MART, TEST],
        },
    )


def test_upstream_lineage_reaches_the_source_not_just_the_parent(lineage_graph):
    """fct_revenue ← dim_customers ← stg_orders ← source: all of it, in edges."""
    result = lineage_graph.get_model_lineage("fct_revenue")

    node_ids = {node["id"] for node in result["nodes"]}
    assert SOURCE in node_ids, "the source at the top of the chain is part of the lineage"
    assert STG in node_ids, "the grandparent must not be dropped either"

    edges = {(edge["source"], edge["target"]) for edge in result["edges"]}
    assert (SOURCE, STG) in edges
    assert (STG, DIM) in edges
    assert (DIM, FCT) in edges


def test_downstream_lineage_reaches_the_far_mart(lineage_graph):
    """A model two steps below the selected one is still downstream of it."""
    result = lineage_graph.get_model_lineage("stg_orders")

    node_ids = {node["id"] for node in result["nodes"]}
    assert DIM in node_ids
    assert FCT in node_ids
    assert MART in node_ids, "a mart built on a child's child is still downstream"

    edges = {(edge["source"], edge["target"]) for edge in result["edges"]}
    assert (FCT, MART) in edges


def test_a_test_node_is_a_leaf_and_does_not_expand(lineage_graph):
    """Tests hang off a model; they have no children of their own to walk."""
    result = lineage_graph.get_model_lineage("fct_revenue")

    assert TEST in {node["id"] for node in result["nodes"]}
    tests = [node for node in result["nodes"] if node["id"] == TEST]
    assert tests[0]["type"] == "test"
    assert all(edge["source"] != TEST for edge in result["edges"])


def test_each_node_appears_once_even_in_a_diamond(tmp_path):
    """Two paths to the same ancestor must not draw the node twice."""
    graph = _project_with(
        tmp_path,
        parents={
            STG: [SOURCE],
            DIM: [STG],
            FCT: [STG, DIM],  # both a short and a long path to the source
        },
        children={SOURCE: [STG], STG: [DIM, FCT], DIM: [FCT]},
    )

    result = graph.get_model_lineage("fct_revenue")

    ids = [node["id"] for node in result["nodes"]]
    assert len(ids) == len(set(ids)), f"duplicate nodes in the graph: {ids}"
    assert SOURCE in ids


def test_the_selected_model_is_marked_current(lineage_graph):
    """The panel highlights one node; exactly one node may claim to be it."""
    result = lineage_graph.get_model_lineage("fct_revenue")

    current = [node for node in result["nodes"] if node.get("is_current")]
    assert len(current) == 1
    assert current[0]["id"] == FCT
    assert current[0]["materialized"] == "table"
