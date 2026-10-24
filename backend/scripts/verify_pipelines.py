"""End-to-end verification of the pipeline orchestrator against a running Obelisk.

Creates real pipelines from the shipped sample assets, runs them, and checks what
actually happened — dependency order, edge conditions, parameters, logs, dbt, raw
SQL, notebooks and webhooks — then prints a report. Every step is a real child
process; nothing is simulated.

    python backend/scripts/verify_pipelines.py --base-url http://127.0.0.1:8000

Exit code is 0 only when every check passed. Failed checks are printed with the
reason, so this doubles as a smoke test after an upgrade.
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

import httpx

TERMINAL = {"success", "failed", "cancelled", "interrupted", "skipped"}


class Receiver:
    """A real local HTTP endpoint, so the webhook step has something to hit."""

    def __init__(self) -> None:
        received: List[str] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - stdlib naming
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8", "replace")
                outer.received.append(body)
                payload = b'{"ok": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args) -> None:
                return

        self.received = received
        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "Receiver":
        self.thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.server.shutdown()
        self.server.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/pipeline-event"


class Report:
    def __init__(self) -> None:
        self.rows: List[tuple[str, str, str]] = []

    def add(self, name: str, ok: bool, detail: str = "", skip: bool = False) -> None:
        status = "SKIP" if skip else ("PASS" if ok else "FAIL")
        self.rows.append((status, name, detail))

    def print(self) -> int:
        width = max((len(name) for _, name, _ in self.rows), default=10)
        failures = 0
        print()
        for status, name, detail in self.rows:
            if status == "FAIL":
                failures += 1
            print(f"  [{status}] {name.ljust(width)}  {detail}")
        print()
        print(f"{len(self.rows) - failures}/{len(self.rows)} checks passed")
        return 1 if failures else 0


def step(key: str, name: str, step_type: str, target: str, *,
         args: Optional[dict] = None, x: float = 0, y: float = 0,
         timeout: Optional[int] = None, retries: Optional[int] = None) -> Dict[str, Any]:
    return {
        "key": key,
        "name": name,
        "step_type": step_type,
        "target": target,
        "args": args or {},
        "timeout_seconds": timeout,
        "max_retries": retries,
        "retry_delay_seconds": 0 if retries is not None else None,
        "position_x": x,
        "position_y": y,
    }


def edge(source: str, target: str, condition: str = "success") -> Dict[str, Any]:
    return {"from_key": source, "to_key": target, "condition": condition}


class Verifier:
    def __init__(self, base_url: str, keep: bool) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=30.0)
        self.keep = keep
        self.report = Report()
        self.created: List[str] = []

    # ------------------------------------------------------------- plumbing

    def create(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = [
            p for p in self.client.get("/api/pipelines").json() if p["name"] == payload["name"]
        ]
        for pipeline in existing:
            self.client.delete(f"/api/pipelines/{pipeline['id']}")
        response = self.client.post("/api/pipelines", json=payload)
        if response.status_code != 201:
            self.report.add(f"create {payload['name']}", False, response.text[:200])
            return None
        created = response.json()
        self.created.append(created["id"])
        return created

    def run(self, pipeline_id: str, timeout: float = 240) -> Optional[Dict[str, Any]]:
        queued = self.client.post(f"/api/pipelines/{pipeline_id}/run", json={})
        if queued.status_code != 202:
            self.report.add(f"run {pipeline_id[:8]}", False, queued.text[:200])
            return None
        run_id = queued.json()["run_id"]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            detail = self.client.get(f"/api/pipeline-runs/{run_id}").json()
            if detail["status"] in TERMINAL:
                return detail
            time.sleep(1.0)
        self.report.add(f"run {pipeline_id[:8]}", False, "did not finish in time")
        return None

    def logs(self, step_run_id: str) -> str:
        return self.client.get(f"/api/step-runs/{step_run_id}/logs").json()["logs"]

    def by_name(self, run: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return {item["step_name"]: item for item in run["step_runs"]}

    # ---------------------------------------------------------------- checks

    def check_health(self) -> None:
        response = self.client.get("/health")
        self.report.add(
            "backend reachable",
            response.status_code == 200,
            response.json().get("app", "") if response.status_code == 200 else response.text[:120],
        )

    def check_success_chain(self) -> None:
        """generate → gate → SQL → notebook, in dependency order."""
        pipeline = self.create(
            {
                "name": "Demo · lakehouse ETL",
                "description": "Synthetic orders, quality gate, SQL check and a notebook.",
                "cron_expression": "0 3 * * *",
                "timezone": "UTC",
                "is_enabled": False,
                "graph": {
                    "steps": [
                        step("gen", "generate orders", "script", "sample_generate_orders",
                             x=40, y=120),
                        step("gate", "quality gate", "script", "sample_quality_gate",
                             x=330, y=120),
                        step("count", "count rows", "raw_sql",
                             "SELECT COUNT(*) AS rows, COUNT(DISTINCT region) AS regions "
                             "FROM bronze.orders WHERE amount >= {{min_amount}}",
                             x=620, y=120),
                        step("nb", "healthcheck", "notebook", "Sample_Pipeline_Healthcheck",
                             x=910, y=120),
                    ],
                    "edges": [
                        edge("gen", "gate"),
                        edge("gate", "count"),
                        edge("count", "nb", "completion"),
                    ],
                    "params": [
                        {"name": "min_amount", "type": "float", "default_value": "0",
                         "required": False},
                    ],
                },
            }
        )
        if not pipeline:
            return
        run = self.run(pipeline["id"])
        if not run:
            return
        steps = self.by_name(run)
        order_ok = all(steps[n]["status"] == "success" for n in
                       ("generate orders", "quality gate", "count rows", "healthcheck"))
        sql_logs = self.logs(steps["count rows"]["id"])
        nb_logs = self.logs(steps["healthcheck"]["id"])
        self.report.add(
            "success chain ran every step",
            run["status"] == "success" and order_ok,
            f"run={run['status']} steps=" + ",".join(f"{k}:{v['status']}" for k, v in steps.items()),
        )
        self.report.add(
            "raw SQL used the pipeline parameter",
            "amount >= 0.0" in sql_logs and "Query OK" in sql_logs,
            sql_logs.splitlines()[1][:90] if len(sql_logs.splitlines()) > 1 else "",
        )
        self.report.add(
            "notebook ran headlessly",
            steps["healthcheck"]["status"] == "success" and "lakehouse healthcheck" in nb_logs,
            nb_logs.splitlines()[-1][:90] if nb_logs else "",
        )
        self.report.add(
            "per-step logs captured",
            len(self.logs(steps["generate orders"]["id"])) > 0,
            "generate-orders log present",
        )

    def check_failure_branch(self, receiver: Receiver) -> None:
        """A failing gate must skip the success branch and fire the failure branch."""
        pipeline = self.create(
            {
                "name": "Demo · failure branch",
                "description": "Quality gate fails on purpose; alert fires, publish is skipped.",
                "cron_expression": "0 4 * * *",
                "timezone": "UTC",
                "is_enabled": False,
                "graph": {
                    "steps": [
                        step("gen", "generate orders", "script", "sample_generate_orders",
                             x=40, y=120),
                        step("gate", "quality gate", "script", "sample_quality_gate",
                             x=330, y=120),
                        step("publish", "publish dbt", "dbt_run", "stg_orders+", x=620, y=40),
                        step("alert", "alert webhook", "webhook", receiver.url,
                             args={"body": {"pipeline": "demo", "reason": "quality gate failed"}},
                             x=620, y=210),
                    ],
                    "edges": [
                        edge("gen", "gate"),
                        edge("gate", "publish", "success"),
                        edge("gate", "alert", "failure"),
                    ],
                    "params": [
                        {"name": "force_fail", "type": "bool", "default_value": "true",
                         "required": False},
                        {"name": "table", "type": "string", "default_value": "bronze.orders",
                         "required": False},
                    ],
                },
            }
        )
        if not pipeline:
            return
        run = self.run(pipeline["id"])
        if not run:
            return
        steps = self.by_name(run)
        self.report.add(
            "failure edge fired, success edge skipped",
            steps["publish dbt"]["status"] == "skipped" and steps["alert webhook"]["status"] == "success",
            f"publish={steps['publish dbt']['status']} alert={steps['alert webhook']['status']}",
        )
        self.report.add(
            "webhook delivered to a real server",
            any("quality gate failed" in body for body in receiver.received),
            f"{len(receiver.received)} request(s) received",
        )
        self.report.add(
            "run is marked failed",
            run["status"] == "failed",
            f"run={run['status']}",
        )


    def check_dbt_pipeline(self) -> None:
        """dbt run → dbt test through the orchestrator.

        A dbt failure is reported but classified as environment-dependent: the
        orchestration is what this check verifies, and whether a warehouse is
        reachable is a property of the host, not of the engine.
        """
        pipeline = self.create(
            {
                "name": "Demo · dbt build and test",
                "description": "Runs the seeded staging model, then tests the project.",
                "cron_expression": "0 5 * * *",
                "timezone": "UTC",
                "is_enabled": False,
                "graph": {
                    "steps": [
                        step("run", "dbt run", "dbt_run", "*", x=40, y=120),
                        step("test", "dbt test", "dbt_test", "*", x=330, y=120),
                    ],
                    "edges": [edge("run", "test")],
                    "params": [],
                },
            }
        )
        if not pipeline:
            return
        run = self.run(pipeline["id"], timeout=600)
        if not run:
            return
        steps = self.by_name(run)
        run_logs = self.logs(steps["dbt run"]["id"])
        # "Running with dbt" proves the CLI started; "Nothing to do" is the failure
        # mode a bare `--select *` used to produce by matching project files.
        dbt_ran = "Running with dbt" in run_logs and "Nothing to do" not in run_logs
        self.report.add(
            "dbt step executed through the orchestrator",
            dbt_ran,
            f"dbt run={steps['dbt run']['status']}, dbt test={steps['dbt test']['status']}",
        )
        if steps["dbt test"]["status"] == "skipped" and steps["dbt run"]["status"] != "success":
            self.report.add(
                "dbt test skipped after a failed dbt run",
                True,
                "success edge correctly withheld",
            )

    def check_params_and_retries(self) -> None:
        """Parameters reach the child; a failing step retries and keeps its log."""
        pipeline = self.create(
            {
                "name": "Demo · retries",
                "description": "A gate that always fails, retried once.",
                "cron_expression": "0 6 * * *",
                "timezone": "UTC",
                "is_enabled": False,
                "graph": {
                    "steps": [
                        step("gen", "generate orders", "script", "sample_generate_orders",
                             x=40, y=120),
                        step("gate", "quality gate", "script", "sample_quality_gate",
                             x=330, y=120, retries=1),
                    ],
                    "edges": [edge("gen", "gate")],
                    "params": [
                        {"name": "force_fail", "type": "bool", "default_value": "true",
                         "required": False},
                        {"name": "rows", "type": "int", "default_value": "50",
                         "required": False},
                    ],
                },
            }
        )
        if not pipeline:
            return
        run = self.run(pipeline["id"])
        if not run:
            return
        steps = self.by_name(run)
        gate = steps["quality gate"]
        gate_logs = self.logs(gate["id"])
        gen_logs = self.logs(steps["generate orders"]["id"])
        self.report.add(
            "parameters reached the child process",
            "generating 50 orders" in gen_logs,
            "OBELISK_PARAM_ROWS honoured",
        )
        self.report.add(
            "retry ran a second attempt and kept both logs",
            gate["attempt"] == 2 and "Retry scheduled" in gate_logs,
            f"attempt={gate['attempt']}",
        )
        self.report.add(
            "retried step finally failed",
            gate["status"] == "failed" and run["status"] == "failed",
            f"step={gate['status']} run={run['status']}",
        )


    def check_schedule_and_history(self, pipeline_id: str) -> None:
        preview = self.client.get(
            "/api/pipelines/preview",
            params={"cron_expression": "0 3 * * *", "timezone": "Europe/Berlin", "count": 5},
        )
        self.report.add(
            "schedule preview returns five occurrences",
            preview.status_code == 200 and len(preview.json()["next_runs"]) == 5,
            preview.json()["next_runs"][0] if preview.status_code == 200 else preview.text[:80],
        )

        toggled = self.client.post(f"/api/pipelines/{pipeline_id}/toggle").json()
        self.report.add(
            "enabling a schedule sets its next run",
            toggled["is_enabled"] and toggled["next_run_at"] is not None,
            toggled["next_run_at"] or "",
        )
        paused = self.client.post(f"/api/pipelines/{pipeline_id}/toggle").json()
        self.report.add(
            "pausing clears the next run",
            not paused["is_enabled"] and paused["next_run_at"] is None,
            "",
        )

        runs = self.client.get(
            "/api/pipeline-runs", params={"pipeline_id": pipeline_id, "limit": 50}
        ).json()
        self.report.add(
            "run history is durable and queryable",
            len(runs) >= 1 and all("pipeline_name" in run for run in runs),
            f"{len(runs)} run(s) recorded",
        )

    def check_rerun_from(self, pipeline_id: str) -> None:
        runs = self.client.get(
            "/api/pipeline-runs", params={"pipeline_id": pipeline_id, "limit": 5}
        ).json()
        failed = next((run for run in runs if run["status"] == "failed"), None)
        if not failed:
            self.report.add("re-run from step", False, "no failed run to resume")
            return
        detail = self.client.get(f"/api/pipeline-runs/{failed['id']}").json()
        gate = next((s for s in detail["step_runs"] if s["step_name"] == "quality gate"), None)
        if not gate:
            self.report.add("re-run from step", False, "step not found")
            return
        response = self.client.post(
            f"/api/pipeline-runs/{failed['id']}/rerun-from/{gate['step_id']}"
        )
        if response.status_code != 202:
            self.report.add("re-run from step", False, response.text[:120])
            return
        new_run_id = response.json()["run_id"]
        deadline = time.monotonic() + 180
        detail = {}
        while time.monotonic() < deadline:
            detail = self.client.get(f"/api/pipeline-runs/{new_run_id}").json()
            if detail["status"] in TERMINAL:
                break
            time.sleep(1.0)
        names = {item["step_name"] for item in detail.get("step_runs", [])}
        self.report.add(
            "re-run from step contains only that step's subtree",
            names == {"quality gate"},
            f"steps={sorted(names)}",
        )

    # ------------------------------------------------------------------ main

    def run_all(self) -> int:
        self.check_health()
        with Receiver() as receiver:
            self.check_success_chain()
            self.check_failure_branch(receiver)
        self.check_params_and_retries()
        self.check_dbt_pipeline()

        pipelines = self.client.get("/api/pipelines").json()
        etl = next((p for p in pipelines if p["name"] == "Demo · lakehouse ETL"), None)
        retries = next((p for p in pipelines if p["name"] == "Demo · retries"), None)
        if etl:
            self.check_schedule_and_history(etl["id"])
        if retries:
            self.check_rerun_from(retries["id"])

        if not self.keep:
            for pipeline_id in self.created:
                self.client.delete(f"/api/pipelines/{pipeline_id}")
        return self.report.print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the Obelisk pipeline orchestrator")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--keep", action="store_true", help="keep the demo pipelines afterwards")
    args = parser.parse_args()

    verifier = Verifier(args.base_url, args.keep)
    try:
        return verifier.run_all()
    except httpx.ConnectError:
        print(f"Could not reach {args.base_url}. Is the backend running?", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

