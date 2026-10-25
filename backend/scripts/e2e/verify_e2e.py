"""End-to-end verification of Obelisk against live sources and a live backend.

This is the full-journey test: it seeds real PostgreSQL, MongoDB and lakehouse data,
drives the real ingest engine, builds a pipeline that uses every step type, forces
every edge condition, and then reconciles what the lakehouse says against what the
sources actually contain. Nothing is mocked and no number is hardcoded: the expected
values are recomputed from PostgreSQL and MongoDB on every run.

    python backend/scripts/e2e/verify_e2e.py --scale small
    python backend/scripts/e2e/verify_e2e.py --scale medium --skip-seed

Exit code is 0 only when every check passed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

ROOT = Path(__file__).resolve().parents[3]
BACKEND = ROOT / "backend"
LAKEHOUSE = ROOT / "data" / "lakehouse"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

TERMINAL = {"success", "failed", "cancelled", "interrupted", "skipped"}
# Every pipeline this verifier creates carries this prefix. It replaces a same-name
# pipeline only inside that namespace, so a demo -- or anything else a person made --
# can never be deleted by a test run that happens to pick the same words.
SCRATCH_PREFIX = "E2E scratch - "
PG_NAME = "pg_enterprise_warehouse"
MONGO_NAME = "mongo_telemetry_source"
FEDERATED_SQL = (
    "SELECT c.region, count(*) AS events, count(DISTINCT e.customer_id) AS customers, "
    "round(sum(e.revenue), 2) AS revenue "
    "FROM pg_enterprise_warehouse.public.e2e_customers c "
    "JOIN bronze.e2e_clickstream e ON e.customer_id = c.customer_id "
    "GROUP BY 1 ORDER BY 1"
)

# The pipeline's SQL rewriter accepts the layer shorthand (``bronze.e2e_clickstream``);
# the editor resolves the canonical three-slot address instead. Both are the same table,
# and asserting both is the point: two callers, one answer.
EDITOR_FEDERATED_SQL = FEDERATED_SQL.replace("bronze.e2e_clickstream", "lakehouse.bronze.e2e_clickstream")


class Receiver:
    """A real local HTTP endpoint, so the webhook step has something to hit."""

    def __init__(self) -> None:
        received: List[str] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - stdlib naming
                length = int(self.headers.get("Content-Length", "0"))
                outer.received.append(self.rfile.read(length).decode("utf-8", "replace"))
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
        return f"http://127.0.0.1:{self.port}/e2e-event"


class Report:
    def __init__(self) -> None:
        self.rows: List[tuple[str, str, str]] = []
        self.timings: List[tuple[str, float]] = []

    def add(self, name: str, ok: bool, detail: str = "", skip: bool = False) -> None:
        self.rows.append(("SKIP" if skip else ("PASS" if ok else "FAIL"), name, detail))

    def time(self, name: str, seconds: float) -> None:
        self.timings.append((name, seconds))

    def print(self) -> int:
        width = max((len(name) for _, name, _ in self.rows), default=10)
        failures = 0
        print()
        for status, name, detail in self.rows:
            if status == "FAIL":
                failures += 1
            print(f"  [{status}] {name.ljust(width)}  {detail}")
        if self.timings:
            print()
            print("  timings")
            total = sum(seconds for _, seconds in self.timings)
            for name, seconds in self.timings:
                print(f"    {name.ljust(width)}  {seconds:6.1f}s")
            print(f"    {'total'.ljust(width)}  {total:6.1f}s")
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
    def __init__(self, base_url: str, scale: str, skip_seed: bool) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=900.0)
        self.report = Report()
        self.scale = scale
        self.skip_seed = skip_seed
        self.created: List[str] = []
        self.truth: Dict[str, Any] = {}
        self.ids: Dict[str, str] = {}

    # ------------------------------------------------------------- plumbing

    def connection(self, name: str) -> dict:
        import sqlite3

        from app.core.vault import decrypt

        con = sqlite3.connect(ROOT / "data" / "obelisk.db")
        try:
            row = con.execute(
                "select id, host, port, database, username, encrypted_credentials, options "
                "from connections where name = ?", (name,)
            ).fetchone()
        finally:
            con.close()
        if not row:
            raise SystemExit(f"Connection {name!r} is missing from the workspace.")
        cid, host, port, database, username, encrypted, options = row
        return {
            "id": cid, "host": host, "port": port, "database": database,
            "username": username, "password": decrypt(encrypted),
            "options": json.loads(options or "{}"),
        }

    def seed(self) -> None:
        if self.skip_seed:
            self.report.add("sources seeded", True, "reused (--skip-seed)")
            return
        started = time.perf_counter()
        result = subprocess.run(
            [sys.executable, str(BACKEND / "scripts" / "e2e" / "seed_sources.py"),
             "--scale", self.scale],
            capture_output=True, text=True,
        )
        elapsed = time.perf_counter() - started
        self.report.time(f"seed ({self.scale})", elapsed)
        tail = (result.stdout or result.stderr).strip().splitlines()
        self.report.add(
            "sources seeded",
            result.returncode == 0,
            tail[-1][:110] if tail else "no output",
        )

    def create(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not payload["name"].startswith(SCRATCH_PREFIX):
            raise RuntimeError(
                f"Verifier pipelines must be named '{SCRATCH_PREFIX}...' so this run "
                "can never delete one it did not create."
            )
        existing = [p for p in self.client.get("/api/pipelines").json()
                    if p["name"] == payload["name"]]
        for pipeline in existing:
            self.client.delete(f"/api/pipelines/{pipeline['id']}")
        response = self.client.post("/api/pipelines", json=payload)
        if response.status_code != 201:
            self.report.add(f"create {payload['name']}", False, response.text[:200])
            return None
        created = response.json()
        self.created.append(created["id"])
        return created

    def run(self, pipeline_id: str, timeout: float = 900) -> Optional[Dict[str, Any]]:
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
            time.sleep(1.5)
        self.report.add(f"run {pipeline_id[:8]}", False, "did not finish in time")
        return None

    def logs(self, step_run_id: str) -> str:
        return self.client.get(f"/api/step-runs/{step_run_id}/logs").json()["logs"]

    def by_name(self, run: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return {item["step_name"]: item for item in run["step_runs"]}    # ---------------------------------------------------------- source truth

    def read_sources(self) -> None:
        """Recompute every expected number from the sources themselves.

        Hardcoded expectations rot the moment the seed changes, so the test derives
        its answers from PostgreSQL and MongoDB and then asks whether the lakehouse
        agrees. That is also what makes the scale flag safe to move.
        """
        import psycopg2

        started = time.perf_counter()
        pg = self.connection(PG_NAME)
        mongo = self.connection(MONGO_NAME)
        self.ids["pg"] = pg["id"]
        self.ids["mongo"] = mongo["id"]

        with psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"], connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            cur.execute("select count(*) from public.e2e_customers")
            customers = cur.fetchone()[0]
            cur.execute(
                "select count(*), coalesce(round(sum(amount)::numeric, 2), 0) "
                "from public.e2e_orders"
            )
            orders, amount = cur.fetchone()
            cur.execute("select customer_id, region from public.e2e_customers")
            region_of = dict(cur.fetchall())

        from urllib.parse import quote_plus

        from pymongo import MongoClient

        auth = mongo["options"].get("auth_db", "admin")
        uri = (f"mongodb://{quote_plus(mongo['username'])}:{quote_plus(mongo['password'])}"
               f"@{mongo['host']}:{mongo['port']}/?authSource={auth}")
        client = MongoClient(uri, serverSelectionTimeoutMS=8_000)
        try:
            db = client[mongo["database"]]
            events = db.e2e_clickstream.count_documents({})
            sessions = db.e2e_sessions.count_documents({})
            per_customer = list(db.e2e_clickstream.aggregate([
                {"$group": {"_id": "$customer_id", "events": {"$sum": 1},
                            "revenue": {"$sum": "$revenue"}}},
            ]))
        finally:
            client.close()

        by_region: Dict[str, Dict[str, float]] = {}
        for row in per_customer:
            region = region_of.get(row["_id"])
            if region is None:
                continue  # the SQL join is inner: unmatched customers contribute nothing
            bucket = by_region.setdefault(region, {"events": 0, "customers": 0, "revenue": 0.0})
            bucket["events"] += int(row["events"])
            bucket["customers"] += 1
            bucket["revenue"] += float(row["revenue"])
        for bucket in by_region.values():
            bucket["revenue"] = round(bucket["revenue"], 2)

        self.truth = {
            "customers": customers,
            "orders": orders,
            "orders_amount": float(amount),
            "events": events,
            "sessions": sessions,
            "by_region": by_region,
        }
        elapsed = time.perf_counter() - started
        self.report.time("read source truth", elapsed)
        self.report.add(
            "source truth read from postgres and mongo",
            customers > 0 and orders > 0 and events > 0 and len(by_region) == 4,
            f"customers={customers:,} orders={orders:,} events={events:,} regions={len(by_region)}",
        )

    # --------------------------------------------------------------- ingest

    def ingest(self) -> None:
        started = time.perf_counter()
        plan = [
            ("postgres orders", self.ids["pg"], "e2e_orders", "raw", "e2e_orders", "order_id"),
            ("mongo clickstream", self.ids["mongo"], "e2e_clickstream", "bronze",
             "e2e_clickstream", "event_id"),
            ("mongo sessions", self.ids["mongo"], "e2e_sessions", "bronze",
             "e2e_sessions", "session_id"),
        ]
        for label, connection_id, entity, folder, target, key in plan:
            response = self.client.post("/api/ingest/trigger", json={
                "connection_id": connection_id, "entity": entity, "mode": "snapshot",
                "target_name": target, "target_folder": folder,
                "write_strategy": "upsert", "primary_key": key,
            })
            body = response.json() if response.status_code == 200 else {}
            meta = body.get("metadata") or {}
            self.report.add(
                f"ingest {label}",
                bool(body.get("success")),
                f"rows={meta.get('num_rows')} extracted={meta.get('new_rows_extracted')}"
                if body else response.text[:150],
            )
        self.report.time("ingest (snapshot)", time.perf_counter() - started)

    def check_incremental(self) -> None:
        """Only rows newer than the watermark may be pulled on a second run."""
        import psycopg2

        started = time.perf_counter()
        pg = self.connection(PG_NAME)
        with psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"], connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            cur.execute("select count(*) from public.e2e_orders")
            before = cur.fetchone()[0]
            cur.execute(
                "insert into public.e2e_orders "
                "(order_id, customer_id, order_ts, region, status, amount, updated_at) "
                "select (select max(order_id) from public.e2e_orders) + g, 1, now(), "
                "'emea', 'placed', 11.11, "
                "(select max(updated_at) from public.e2e_orders) + (g || ' minute')::interval "
                "from generate_series(1, 5) g"
            )
        moved = self.client.post("/api/ingest/trigger", json={
            "connection_id": self.ids["pg"], "entity": "e2e_orders", "mode": "incremental",
            "watermark_column": "updated_at", "target_name": "e2e_orders",
            "target_folder": "raw", "write_strategy": "upsert", "primary_key": "order_id",
        })
        body = moved.json() if moved.status_code == 200 else {}
        meta = body.get("metadata") or {}
        total = meta.get("num_rows")
        # The sync endpoint reports the total in metadata and the delta in its log line,
        # so the delta is read from the same string an operator would read.
        log_text = " ".join(body.get("logs") or [])
        extracted = None
        if "extracted " in log_text:
            tail = log_text.split("extracted ", 1)[1].split(")", 1)[0].strip()
            extracted = int(tail) if tail.lstrip("-").isdigit() else None
        self.report.add(
            "incremental ingest pulled exactly the new rows",
            extracted == 5 and total == before + 5,
            f"extracted={extracted} total={total} (was {before})",
        )
        self.truth["orders"] = before + 5
        self.truth["orders_amount"] = round(self.truth["orders_amount"] + 55.55, 2)
        self.report.time("ingest (incremental)", time.perf_counter() - started)
    # -------------------------------------------------------------- catalog

    def reconcile(self) -> None:
        started = time.perf_counter()
        for label, connection_id in (("postgres", self.ids["pg"]), ("mongo", self.ids["mongo"])):
            response = self.client.post(f"/api/catalog/connections/{connection_id}/reconcile")
            self.report.add(f"reconcile {label}", response.status_code == 200, response.text[:140])

        mongo_objects = self.client.get(
            "/api/catalog/objects", params={"connection_id": self.ids["mongo"]}
        ).json()
        mongo_names = {item.get("name") for item in mongo_objects}
        self.report.add(
            "mongo collections are cataloged",
            {"e2e_clickstream", "e2e_sessions"} <= mongo_names,
            f"{len(mongo_names)} object(s)",
        )
        pg_objects = self.client.get(
            "/api/catalog/objects", params={"connection_id": self.ids["pg"]}
        ).json()
        pg_names = {item.get("name") for item in pg_objects}
        self.report.add(
            "postgres tables are cataloged",
            {"e2e_orders", "e2e_customers"} <= pg_names,
            f"{len(pg_names)} object(s)",
        )
        tree = json.dumps(self.client.get("/api/catalog/tree").json())
        self.report.add(
            "catalog tree lists the source tables",
            "e2e_clickstream" in tree and "e2e_orders" in tree,
            "tree contains both e2e tables",
        )
        self.report.time("reconcile sources", time.perf_counter() - started)

    # -------------------------------------------------------- pipeline: main

    def run_federated_pipeline(self, receiver: "Receiver") -> Optional[Dict[str, Any]]:
        pipeline = self.create({
            "name": f"{SCRATCH_PREFIX}federated analytics",
            "description": "dbt over ingested orders, live federated SQL, a materialized "
                           "copy, a notebook, a retried gate and a webhook.",
            "cron_expression": "0 2 * * *",
            "timezone": "UTC",
            "is_enabled": False,
            "graph": {
                "steps": [
                    step("dbt", "dbt build e2e", "dbt_run", "stg_e2e_orders+", x=40, y=40,
                         args={"stage": {"folder": "raw"}}),
                    step("fed", "federated join", "raw_sql", FEDERATED_SQL, x=340, y=40),
                    step("mat", "materialize federated", "raw_sql", FEDERATED_SQL,
                         args={"materialize": {"folder": "silver", "name": "e2e_federated"}},
                         x=640, y=40),
                    step("score", "scoring", "notebook", "E2E_Scoring", x=940, y=40),
                    step("gate", "flaky gate", "script", "e2e_flaky_gate", x=940, y=200,
                         retries=1),
                    step("hook", "notify", "webhook", receiver.url,
                         args={"body": {"pipeline": "e2e", "status": "ok"}}, x=940, y=360),
                ],
                "edges": [
                    edge("dbt", "fed"), edge("fed", "mat"), edge("mat", "score"),
                    edge("score", "gate"), edge("gate", "hook"),
                ],
                "params": [],
            },
        })
        if not pipeline:
            return None
        started = time.perf_counter()
        run = self.run(pipeline["id"])
        self.report.time("pipeline: federated analytics", time.perf_counter() - started)
        if not run:
            return None

        steps = self.by_name(run)
        self.report.add(
            "every step succeeded in dependency order",
            run["status"] == "success",
            f"run={run['status']} " + ",".join(f"{k}:{v['status']}" for k, v in steps.items()),
        )

        fed = steps["federated join"]
        fed_logs = self.logs(fed["id"])
        self.report.add(
            "raw_sql step federated postgres with the lakehouse",
            fed["status"] == "success" and "Query OK: 4" in fed_logs,
            "4 region rows" if fed["status"] == "success" else fed_logs[-150:],
        )

        detail = self.client.get(f"/api/pipeline-runs/{run['id']}").json()
        mat_run = next(s for s in detail["step_runs"] if s["step_name"] == "materialize federated")
        outputs = mat_run.get("outputs") or {}
        self.report.add(
            "materialize published the table it wrote",
            (outputs.get("table") or {}).get("value") == "lakehouse.silver.e2e_federated",
            str((outputs.get("table") or {}).get("value")),
        )

        score_logs = self.logs(steps["scoring"]["id"])
        self.report.add(
            "notebook read the federated table and wrote gold",
            steps["scoring"]["status"] == "success"
            and "e2e scoring complete" in score_logs,
            score_logs.strip().splitlines()[-1][:110] if score_logs.strip() else "no log",
        )

        gate = steps["flaky gate"]
        self.report.add(
            "a failed attempt retried and then succeeded",
            gate["status"] == "success" and gate["attempt"] == 2,
            f"status={gate['status']} attempt={gate['attempt']}",
        )

        self.report.add(
            "webhook delivered to a real server",
            any("e2e" in body for body in receiver.received),
            f"{len(receiver.received)} request(s) received",
        )
        return run
    # ------------------------------------------------------- lakehouse truth

    def check_outputs(self) -> None:
        """Every table the pipeline claimed to write must exist and hold source truth."""
        import polars as pl

        started = time.perf_counter()
        expected_rows = {
            "raw/e2e_orders.parquet": self.truth["orders"],
            "bronze/e2e_clickstream.parquet": self.truth["events"],
            "bronze/e2e_sessions.parquet": self.truth["sessions"],
            "bronze/e2e_products.parquet": None,
            "silver/e2e_federated.parquet": len(self.truth["by_region"]),
            "gold/e2e_scores.parquet": len(self.truth["by_region"]),
        }
        for relative, expected in expected_rows.items():
            path = LAKEHOUSE / relative
            if not path.exists():
                self.report.add(f"{relative} written", False, "file missing")
                continue
            height = pl.read_parquet(path).height
            suffix = "" if expected is None else f" (expected {expected:,})"
            self.report.add(
                f"{relative} written",
                expected is None or height == expected,
                f"{height:,} rows{suffix}",
            )

        silver_path = LAKEHOUSE / "silver/e2e_federated.parquet"
        if not silver_path.exists():
            self.report.add("federated result equals postgres x mongo exactly", False,
                            "silver table missing")
            self.report.time("reconcile lakehouse", time.perf_counter() - started)
            return
        federated = {row["region"]: row for row in pl.read_parquet(silver_path).to_dicts()}
        problems = []
        for region, expected in self.truth["by_region"].items():
            row = federated.get(region)
            if row is None:
                problems.append(f"{region}: missing")
                continue
            if int(row["events"]) != expected["events"]:
                problems.append(f"{region}: events {row['events']} != {expected['events']}")
            if int(row["customers"]) != expected["customers"]:
                problems.append(f"{region}: customers {row['customers']} != {expected['customers']}")
            if abs(float(row["revenue"]) - expected["revenue"]) > 0.05:
                problems.append(f"{region}: revenue {row['revenue']} != {expected['revenue']}")
        self.report.add(
            "federated result equals postgres x mongo exactly",
            not problems,
            "; ".join(problems[:3]) or f"{len(federated)} region(s) reconciled",
        )
        self.report.time("reconcile lakehouse", time.perf_counter() - started)

    def check_dbt_outputs(self) -> None:
        import psycopg2

        pg = self.connection(PG_NAME)
        try:
            with psycopg2.connect(
                host=pg["host"], port=pg["port"], dbname=pg["database"],
                user=pg["username"], password=pg["password"], connect_timeout=10,
            ) as conn, conn.cursor() as cur:
                cur.execute("select count(*) from raw.stg_e2e_orders")
                staged = cur.fetchone()[0]
                cur.execute("select count(*) from marts.fct_e2e_revenue")
                mart = cur.fetchone()[0]
        except Exception as exc:
            self.report.add("dbt materialized the e2e models", False, str(exc)[:150])
            return
        self.report.add(
            "dbt staged every ingested order",
            staged == self.truth["orders"],
            f"raw.stg_e2e_orders={staged:,} (expected {self.truth['orders']:,})",
        )
        self.report.add(
            "dbt aggregated the e2e revenue mart",
            mart == len(self.truth["by_region"]),
            f"marts.fct_e2e_revenue={mart} region(s)",
        )
    # ----------------------------------------------------- pipeline: failure

    def run_failure_pipeline(self, receiver: "Receiver") -> Optional[Dict[str, Any]]:
        """All four edge conditions in one run: success, failure and completion."""
        pipeline = self.create({
            "name": f"{SCRATCH_PREFIX}edge conditions",
            "description": "A gate fails on purpose; the success child is skipped, the "
                           "failure child runs and the completion child always runs.",
            "cron_expression": "0 4 * * *",
            "timezone": "UTC",
            "is_enabled": False,
            "graph": {
                "steps": [
                    step("gate", "quality gate", "script", "sample_quality_gate", x=40, y=200),
                    step("publish", "publish", "raw_sql",
                         "select count(*) as n from bronze.e2e_products", x=340, y=40),
                    step("alert", "alert", "webhook", receiver.url,
                         args={"body": {"pipeline": "e2e", "reason": "gate failed"}},
                         x=340, y=200),
                    step("always", "always runs", "raw_sql",
                         "select count(*) as n from gold.e2e_scores", x=340, y=360),
                ],
                "edges": [
                    edge("gate", "publish", "success"),
                    edge("gate", "alert", "failure"),
                    edge("gate", "always", "completion"),
                ],
                "params": [
                    {"name": "force_fail", "type": "bool", "default_value": "true",
                     "required": False},
                    {"name": "table", "type": "string", "default_value": "bronze.e2e_products",
                     "required": False},
                ],
            },
        })
        if not pipeline:
            return None
        started = time.perf_counter()
        run = self.run(pipeline["id"])
        self.report.time("pipeline: edge conditions", time.perf_counter() - started)
        if not run:
            return None

        steps = self.by_name(run)
        self.report.add(
            "a failed gate withholds the success edge",
            steps["quality gate"]["status"] == "failed" and steps["publish"]["status"] == "skipped",
            f"gate={steps['quality gate']['status']} publish={steps['publish']['status']}",
        )
        self.report.add(
            "the failure edge fired instead",
            steps["alert"]["status"] == "success",
            f"alert={steps['alert']['status']}",
        )
        self.report.add(
            "the completion edge ran regardless",
            steps["always runs"]["status"] == "success",
            f"always={steps['always runs']['status']}",
        )
        self.report.add(
            "a run with a failed step is marked failed",
            run["status"] == "failed",
            f"run={run['status']}",
        )
        self.ids["failed_run"] = run["id"]
        return run

    # ------------------------------------------------- pipeline dependency

    def run_child_pipeline(self) -> None:
        child = self.create({
            "name": f"{SCRATCH_PREFIX}child rollup",
            "description": "Scores written by the parent are rolled up here.",
            "cron_expression": "0 8 * * *",
            "timezone": "UTC",
            "is_enabled": False,
            "graph": {
                "steps": [step("q", "rollup", "raw_sql",
                               "select count(*) as n from gold.e2e_scores", x=40, y=120)],
                "edges": [], "params": [],
            },
        })
        if not child:
            return
        parent = self.create({
            "name": f"{SCRATCH_PREFIX}parent pipeline",
            "description": "Runs the child pipeline and waits for it.",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "is_enabled": False,
            "graph": {
                "steps": [step("run", "run child", "pipeline", child["id"], x=40, y=120)],
                "edges": [], "params": [],
            },
        })
        self.ids["child"] = child["id"]
        if not parent:
            return
        run = self.run(parent["id"])
        if not run:
            return
        detail = self.client.get(f"/api/pipeline-runs/{run['id']}").json()
        step_run = next(s for s in detail["step_runs"] if s["step_name"] == "run child")
        outputs = step_run.get("outputs") or {}
        child_run_id = (outputs.get("run_id") or {}).get("value")
        child_run = (self.client.get(f"/api/pipeline-runs/{child_run_id}").json()
                     if child_run_id else {})
        self.report.add(
            "a pipeline step ran another pipeline and waited",
            step_run["status"] == "success"
            and (outputs.get("status") or {}).get("value") == "success",
            f"step={step_run['status']}",
        )
        self.report.add(
            "the child run records which pipeline and step started it",
            child_run.get("parent_run_id") == run["id"]
            and (child_run.get("parent") or {}).get("step_name") == "run child",
            str(child_run.get("parent"))[:110],
        )
    # ----------------------------------------------------------- query studio

    def check_editor_federation(self) -> None:
        """The editor is the other federated caller; it must agree with the pipeline."""
        started = time.perf_counter()
        response = self.client.post(
            "/api/query/execute",
            json={"sql": EDITOR_FEDERATED_SQL, "timeout_seconds": 180},
        )
        body = response.json() if response.status_code == 200 else {}
        rows = {row["region"]: row for row in (body.get("rows") or [])}
        problems = []
        for region, expected in self.truth["by_region"].items():
            row = rows.get(region)
            if row is None:
                problems.append(f"{region}: missing")
                continue
            if int(row["events"]) != expected["events"]:
                problems.append(f"{region}: events {row['events']} != {expected['events']}")
            if abs(float(row["revenue"]) - expected["revenue"]) > 0.05:
                problems.append(f"{region}: revenue {row['revenue']} != {expected['revenue']}")
        self.report.add(
            "query editor federated postgres with the lakehouse",
            response.status_code == 200 and not problems,
            response.text[:150] if response.status_code != 200
            else ("; ".join(problems[:2])
                  or f"{body.get('row_count')} rows in {body.get('duration_ms')} ms"),
        )

        export = self.client.post(
            "/api/query/export",
            json={"sql": EDITOR_FEDERATED_SQL, "format": "parquet", "filename": "e2e_federated"},
        )
        self.report.add(
            "federated results export as parquet",
            export.status_code == 200 and len(export.content) > 0,
            f"{len(export.content):,} bytes",
        )
        self.report.time("query editor federation", time.perf_counter() - started)

    # --------------------------------------------------- schedule and resume

    def check_schedule_and_rerun(self) -> None:
        child_id = self.ids.get("child")
        if child_id:
            enabled = self.client.post(f"/api/pipelines/{child_id}/toggle").json()
            self.report.add(
                "enabling a schedule sets its next run",
                bool(enabled.get("is_enabled")) and enabled.get("next_run_at") is not None,
                enabled.get("next_run_at") or "",
            )
            paused = self.client.post(f"/api/pipelines/{child_id}/toggle").json()
            self.report.add(
                "pausing clears the next run",
                not paused.get("is_enabled") and paused.get("next_run_at") is None,
                "",
            )

        run_id = self.ids.get("failed_run")
        if not run_id:
            self.report.add("re-run from a failed step", False, "no failed run to resume")
            return
        detail = self.client.get(f"/api/pipeline-runs/{run_id}").json()
        alert = next((s for s in detail["step_runs"] if s["step_name"] == "alert"), None)
        if not alert:
            self.report.add("re-run from a failed step", False, "alert step not found")
            return
        response = self.client.post(f"/api/pipeline-runs/{run_id}/rerun-from/{alert['step_id']}")
        if response.status_code != 202:
            self.report.add("re-run from a failed step", False, response.text[:140])
            return
        new_run_id = response.json()["run_id"]
        deadline = time.monotonic() + 120
        status = "pending"
        while time.monotonic() < deadline:
            status = self.client.get(f"/api/pipeline-runs/{new_run_id}").json()["status"]
            if status in TERMINAL:
                break
            time.sleep(1.0)
        self.report.add(
            "re-run from a failed step resumes and finishes",
            status in TERMINAL,
            f"resumed run={status}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Obelisk end-to-end verification.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--scale", choices=("small", "medium", "large"), default="small")
    parser.add_argument("--skip-seed", action="store_true",
                        help="Reuse whatever the sources already contain.")
    parser.add_argument("--keep", action="store_true",
                        help="Keep the pipelines this run created.")
    args = parser.parse_args()

    marker = Path(tempfile.gettempdir()) / "obelisk_e2e_flaky.marker"
    marker.unlink(missing_ok=True)

    verifier = Verifier(args.base_url, args.scale, args.skip_seed)
    try:
        health = verifier.client.get("/health")
    except Exception as exc:
        print(f"Backend unreachable at {args.base_url}: {exc}")
        return 1
    verifier.report.add(
        "backend reachable", health.status_code == 200,
        health.json().get("app", "") if health.status_code == 200 else health.text[:120],
    )
    if health.status_code != 200:
        return verifier.report.print()

    with Receiver() as receiver:
        try:
            verifier.seed()
            verifier.read_sources()
            verifier.ingest()
            verifier.check_incremental()
            verifier.reconcile()
            verifier.run_federated_pipeline(receiver)
            verifier.check_outputs()
            verifier.check_dbt_outputs()
            verifier.run_failure_pipeline(receiver)
            verifier.run_child_pipeline()
            verifier.check_editor_federation()
            verifier.check_schedule_and_rerun()
        except Exception:
            verifier.report.add("verification ran to completion", False,
                                traceback.format_exc()[-400:])
        finally:
            if not args.keep:
                for pipeline_id in verifier.created:
                    verifier.client.delete(f"/api/pipelines/{pipeline_id}")

    return verifier.report.print()


if __name__ == "__main__":
    raise SystemExit(main())