"""Creates the demo pipeline set in the workspace, with real edges, and runs it.

The end-to-end verifier deletes what it creates, so a workspace it has run in looks
empty. This script is the opposite: it leaves a curated set behind, one pipeline per
idea, so the Pipelines tab has something to open and the run history has something in
it. Every step is a real asset that ships with the project.

    python backend/scripts/create_demo_pipelines.py
    python backend/scripts/create_demo_pipelines.py --run      # also run each once
    python backend/scripts/create_demo_pipelines.py --delete   # remove them again

It is idempotent: a pipeline with the same name is replaced, not duplicated.
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Dict, List, Optional

import httpx

TERMINAL = {"success", "failed", "cancelled", "interrupted", "skipped"}
WEBHOOK_URL = "http://127.0.0.1:9099/pipeline-event"

FEDERATED_SQL = (
    "SELECT c.region, count(*) AS events, count(DISTINCT e.customer_id) AS customers, "
    "round(sum(e.revenue), 2) AS revenue "
    "FROM pg_enterprise_warehouse.public.e2e_customers c "
    "JOIN bronze.e2e_clickstream e ON e.customer_id = c.customer_id "
    "GROUP BY 1 ORDER BY 1"
)


def step(key: str, name: str, step_type: str, target: str, *,
         args: Optional[dict] = None, x: float = 0, y: float = 0,
         retries: Optional[int] = None) -> Dict[str, Any]:
    return {
        "key": key,
        "name": name,
        "step_type": step_type,
        "target": target,
        "args": args or {},
        "max_retries": retries,
        "retry_delay_seconds": 0 if retries is not None else None,
        "position_x": x,
        "position_y": y,
    }


def edge(source: str, target: str, condition: str = "success") -> Dict[str, Any]:
    return {"from_key": source, "to_key": target, "condition": condition}


def param(name: str, kind: str, default: str) -> Dict[str, Any]:
    return {"name": name, "type": kind, "default_value": default, "required": False}


def pipeline(name: str, description: str, cron: str, steps: List[dict],
             edges: List[dict], params: Optional[List[dict]] = None) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "cron_expression": cron,
        "timezone": "UTC",
        "is_enabled": False,
        "graph": {"steps": steps, "edges": edges, "params": params or []},
    }
CHILD_NAME = "Demo - child rollup"
ALL_NAMES = [
    "Demo - lakehouse ETL",
    "Demo - dbt build and test",
    "Demo - federated analytics",
    "Demo - edge conditions",
    CHILD_NAME,
    "Demo - parent pipeline",
]


def upsert(client: httpx.Client, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for existing in client.get("/api/pipelines").json():
        if existing["name"] == payload["name"]:
            client.delete(f"/api/pipelines/{existing['id']}")
    response = client.post("/api/pipelines", json=payload)
    if response.status_code != 201:
        print(f"  [FAIL] {payload['name']}: {response.text[:300]}")
        return None
    created = response.json()
    count = len(payload["graph"]["steps"])
    edges = len(payload["graph"]["edges"])
    print(f"  [created] {created['name']:<30} {count} step(s), {edges} edge(s)")
    return created


def run_once(client: httpx.Client, pipeline_id: str, timeout: float = 900) -> tuple[str, str]:
    queued = client.post(f"/api/pipelines/{pipeline_id}/run", json={})
    if queued.status_code != 202:
        return "queue-failed", queued.text[:160]
    run_id = queued.json()["run_id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        detail = client.get(f"/api/pipeline-runs/{run_id}").json()
        if detail["status"] in TERMINAL:
            summary = ", ".join(f"{s['step_name']}={s['status']}" for s in detail["step_runs"])
            return detail["status"], summary
        time.sleep(1.5)
    return "timeout", "did not finish in time"


def build(child_id: str) -> List[Dict[str, Any]]:
    return [
        pipeline(
            "Demo - lakehouse ETL",
            "Synthetic orders, a quality gate, a SQL check and a notebook - the "
            "success path plus a completion edge that runs either way.",
            "0 3 * * *",
            [
                step("gen", "generate orders", "script", "sample_generate_orders", x=40, y=160),
                step("gate", "quality gate", "script", "sample_quality_gate", x=330, y=160),
                step("count", "count rows", "raw_sql",
                     "SELECT count(*) AS rows, count(DISTINCT region) AS regions "
                     "FROM bronze.orders WHERE amount >= {{min_amount}}", x=620, y=120),
                step("nb", "healthcheck", "notebook", "Sample_Pipeline_Healthcheck",
                     x=910, y=200),
            ],
            [
                edge("gen", "gate"),
                edge("gate", "count"),
                edge("count", "nb", "completion"),
            ],
            [param("rows", "int", "500"), param("min_amount", "float", "0")],
        ),
        pipeline(
            "Demo - dbt build and test",
            "Stages the raw folder into the warehouse, builds the e2e models, then "
            "tests them. dbt is asked to stage explicitly.",
            "0 5 * * *",
            [
                step("run", "dbt run", "dbt_run", "stg_e2e_orders+", x=40, y=160,
                     args={"stage": {"folder": "raw"}}),
                step("test", "dbt test", "dbt_test", "stg_e2e_orders", x=330, y=160),
            ],
            [edge("run", "test")],
        ),
        pipeline(
            "Demo - federated analytics",
            "Every step type in one chain: dbt, a live federated SQL join across "
            "PostgreSQL and the lakehouse, a materialized copy, a notebook, a retried "
            "gate and a webhook.",
            "0 2 * * *",
            [
                step("dbt", "dbt build e2e", "dbt_run", "stg_e2e_orders+", x=40, y=40,
                     args={"stage": {"folder": "raw"}}),
                step("fed", "federated join", "raw_sql", FEDERATED_SQL, x=340, y=40),
                step("mat", "materialize federated", "raw_sql", FEDERATED_SQL, x=640, y=40,
                     args={"materialize": {"folder": "silver", "name": "e2e_federated"}}),
                step("score", "scoring", "notebook", "E2E_Scoring", x=940, y=40),
                step("gate", "flaky gate", "script", "e2e_flaky_gate", x=940, y=200,
                     retries=1),
                step("hook", "notify", "webhook", WEBHOOK_URL, x=940, y=360,
                     args={"body": {"pipeline": "e2e", "status": "ok"}}),
            ],
            [
                edge("dbt", "fed"), edge("fed", "mat"), edge("mat", "score"),
                edge("score", "gate"), edge("gate", "hook"),
            ],
        ),
        pipeline(
            "Demo - edge conditions",
            "A gate that fails on purpose: the success child is skipped, the failure "
            "child runs, and the completion child runs regardless.",
            "0 4 * * *",
            [
                step("gate", "quality gate", "script", "sample_quality_gate", x=40, y=200),
                step("publish", "publish", "raw_sql",
                     "select count(*) as n from bronze.e2e_products", x=340, y=40),
                step("alert", "alert", "webhook", WEBHOOK_URL, x=340, y=200,
                     args={"body": {"pipeline": "e2e", "reason": "quality gate failed"}}),
                step("always", "always runs", "raw_sql",
                     "select count(*) as n from bronze.e2e_products", x=340, y=360),
            ],
            [
                edge("gate", "publish", "success"),
                edge("gate", "alert", "failure"),
                edge("gate", "always", "completion"),
            ],
            [param("force_fail", "bool", "true"),
             param("table", "string", "bronze.e2e_products")],
        ),
        pipeline(
            CHILD_NAME,
            "Rolls up the scores the parent's notebook wrote.",
            "0 8 * * *",
            [step("q", "rollup", "raw_sql",
                  "select count(*) as n from gold.e2e_scores", x=40, y=160)],
            [],
        ),
        pipeline(
            "Demo - parent pipeline",
            "Runs the child pipeline as a step and waits for it.",
            "0 9 * * *",
            [step("run", "run child", "pipeline", child_id, x=40, y=160)],
            [],
        ),
    ]
def main() -> int:
    parser = argparse.ArgumentParser(description="Create the demo pipeline set.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--run", action="store_true", help="Run every pipeline once.")
    parser.add_argument("--delete", action="store_true", help="Delete the set instead.")
    args = parser.parse_args()

    client = httpx.Client(base_url=args.base_url, timeout=900)
    try:
        health = client.get("/health")
    except Exception as exc:
        print(f"Backend unreachable at {args.base_url}: {exc}")
        return 1
    if health.status_code != 200:
        print(f"Backend unhealthy: {health.text[:160]}")
        return 1

    if args.delete:
        for existing in client.get("/api/pipelines").json():
            if existing["name"] in ALL_NAMES:
                client.delete(f"/api/pipelines/{existing['id']}")
                print(f"  [deleted] {existing['name']}")
        return 0

    seed = build("")
    child_payload = next(item for item in seed if item["name"] == CHILD_NAME)
    child = upsert(client, child_payload)
    if not child:
        return 1

    created = [child]
    for payload in build(child["id"]):
        if payload["name"] == CHILD_NAME:
            continue
        made = upsert(client, payload)
        if made:
            created.append(made)

    if args.run:
        print("\nrunning each pipeline once:")
        for made in created:
            started = time.monotonic()
            status, summary = run_once(client, made["id"])
            elapsed = time.monotonic() - started
            print(f"  [{status:<8}] {made['name']:<30} {elapsed:5.1f}s  {summary[:130]}")

    print(f"\n{len(created)} pipeline(s) in the workspace.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())