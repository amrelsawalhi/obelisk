"""
dbt Runner & Pipeline Engine:
- Stages a warehouse folder's Parquet files into the matching PostgreSQL schema
- Executes dbt run and dbt test via python -m dbt.cli.main
- Exposes the model graph with node ids keyed by the folder each model lives in
- Tracks pipeline run history in SQLite

Where a model lands is dbt's own business: ``dbt_project.yml`` sets ``+schema`` per
model folder and dbt uses it as the schema. Obelisk reads those names to know which
warehouse folder to copy into PostgreSQL, and nothing else. There is no layer table,
no layer→schema mapping and no schema macro of ours.

Nothing in this module writes data on its own initiative. A build that finds no source
tables returns :class:`NoSourceData` — a failure that names the empty folder and the
one call that fills it — instead of seeding sample files behind the operator's back.
Sample data exists, but only when `POST /api/dbt/sample-data` asks for it.
"""

import os
import re
import sys
import time
import json
import uuid
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import duckdb
from app.config import settings
from app.core.catalog import path_for
from app.core.host_resolver import resolve_connection_host
from app.core.dsn import build_pg_dsn, sql_ident, sql_string, validate_alias
from app.core.storage import warehouse_storage
from app.core.warehouse_resolver import resolve_write_warehouse_postgres_params
from app.services.state import task_history


class NoSourceData(RuntimeError):
    """A build was asked to run while the folder it reads holds no table at all.

    Distinct from a generic failure so callers can report "there is nothing to read
    yet" as a fact, not as a broken warehouse connection.
    """


class DbtRunner:
    def __init__(self, dbt_project_dir: Optional[Path] = None):
        if dbt_project_dir is None:
            root = Path(__file__).resolve().parent.parent.parent.parent
            self.dbt_project_dir = (root / "git_repo" / "dbt_project").resolve()
        else:
            self.dbt_project_dir = dbt_project_dir.resolve()

        self._runs_history: List[Dict[str, Any]] = []

    def _record_run(self, run: Dict[str, Any]) -> None:
        """Appends a run to the in-memory list *and* to durable task history (M3).

        The list stays because the UI polls it at will; the table is what survives a
        restart, which previously wiped every run.
        """
        self._runs_history.append(run)
        run_id = str(run.get("run_id") or uuid.uuid4())
        task_history.record(
            "dbt",
            run_id,
            str(run.get("status") or "unknown"),
            run.get("started_at"),
            {**run, "run_id": run_id},
        )

    def _dbt_folders(self) -> Dict[str, str]:
        """The warehouse folder each model folder writes to, read from the project.

        ``dbt_project.yml`` sets ``+schema`` per model folder, and dbt uses that same
        name as the warehouse schema. Obelisk reads it rather than imposing a mapping:
        the project decides where its output lands, and no layer table is involved.
        """
        project_file = self.dbt_project_dir / "dbt_project.yml"
        try:
            import yaml

            document = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}
        except Exception:
            document = {}

        models_config = ((document.get("models") or {}).get("obelisk_dbt") or {})

        def schema_for(folder: str, fallback: str) -> str:
            block = models_config.get(folder) or {}
            value = block.get("+schema") if isinstance(block, dict) else None
            return str(value or fallback).strip().strip("\"'")

        return {
            "staging": schema_for("staging", "raw"),
            "marts": schema_for("marts", "marts"),
        }

    def _dbt_vars_payload(self) -> Dict[str, Any]:
        """What Obelisk tells dbt about itself: nothing.

        Kept as a seam because pipeline params are merged into it, but the project no
        longer needs a layer→schema map — it declares its own schemas.
        """
        return {}

    def _empty_folder_message(self, folder: str, directory: str) -> str:
        """What a build says when it has nothing to read.

        A fresh install holds no tables, and quietly inventing them turned that fact
        into a green run. Naming the empty folder and the one call that fills it turns
        an obscure dbt relation error into an instruction the operator can act on.
        """
        return (
            f"Nothing to build: '{folder}' holds no Parquet files ({directory}), so the "
            "project has no source tables to read. Ingest a source, or call "
            "POST /api/dbt/sample-data to load the shipped sample tables, then run again."
        )

    def _no_source_data_result(
        self,
        command: str,
        selector: Optional[str],
        target: str,
        folder: str,
        directory: str,
    ) -> Dict[str, Any]:
        """The shaped-like-a-run failure a build with no source tables returns."""
        message = self._empty_folder_message(folder, directory)
        return {
            "command": f"dbt {command}",
            "selector": selector or "*",
            "target": target,
            "success": False,
            "exit_code": 1,
            "duration_seconds": 0.0,
            "duration_ms": 0,
            "reason": "no_source_data",
            "error": message,
            "output": message,
            "logs": message,
        }

    def seed_sample_data(self, folder: Optional[str] = None) -> Dict[str, Any]:
        """Writes the shipped sample tables into one folder, on request only.

        The explicit replacement for the seeding the runner used to do behind a
        build's back: the caller decides that an empty workspace needs something to
        look at, and gets back exactly which tables that created. Idempotent — files
        that already exist are left untouched.
        """
        from app.core.catalog import scan as scan_lakehouse

        target = (folder or self._dbt_folders()["staging"]).strip().strip("/")
        store = warehouse_storage()
        schema = target.replace("/", ".")
        before = {table.name for table in scan_lakehouse().tables if table.folder == schema}
        self._write_sample_tables(target)
        after = {table.name for table in scan_lakehouse().tables if table.folder == schema}
        return {
            "status": "success",
            "folder": target,
            "directory": store.display_root(),
            "tables": sorted(after),
            "created": sorted(after - before),
        }

    @staticmethod
    def _write_table(rel_path: str, table: Any) -> None:
        """Creates one sample table in the lakehouse, if it is not there already.

        ``IF NOT EXISTS`` keeps the seeding idempotent: a workspace that already has the
        sample set is left exactly as it is, which is what the caller promised.
        """
        from app.core.catalog import SOURCE, quote_ident
        from app.engines.duckdb_engine import duckdb_engine

        folder, name = rel_path.rsplit("/", 1) if "/" in rel_path else ("", rel_path)
        if name.endswith(".parquet"):
            name = name[: -len(".parquet")]
        schema = folder.replace("/", ".") if folder else "main"

        conn = duckdb_engine.get_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema)};")
            conn.register("_obelisk_sample", table)
            try:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {SOURCE}.{quote_ident(schema)}."
                    f"{quote_ident(name)} AS SELECT * FROM _obelisk_sample"
                )
            finally:
                conn.unregister("_obelisk_sample")
        finally:
            conn.close()

    def _write_sample_tables(self, folder: str) -> None:
        """Writes the demo tables the shipped models read.

        The shipped models reference ``orders`` and ``customers``. Reached only
        through :meth:`seed_sample_data` — a build never calls this, because a
        pipeline that dies on a missing relation is honest and one that silently
        invents six tables is not.
        """
        import pyarrow as pa

        store = warehouse_storage()

        orders_file = path_for(folder, "orders")
        if not store.exists(orders_file):
            orders_data = {
                "order_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "customer_id": pa.array([101, 102, 101, 103, 102], type=pa.int64()),
                "amount": pa.array([150.50, 89.99, 450.00, 25.00, 199.95], type=pa.float64()),
                "status": pa.array(["completed", "completed", "completed", "pending", "completed"], type=pa.string()),
                "created_at": pa.array([
                    datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 1, 16, 11, 30, 0, tzinfo=timezone.utc),
                    datetime(2026, 1, 17, 14, 15, 0, tzinfo=timezone.utc),
                    datetime(2026, 1, 18, 9, 45, 0, tzinfo=timezone.utc),
                    datetime(2026, 1, 19, 16, 20, 0, tzinfo=timezone.utc),
                ]),
            }
            self._write_table(orders_file, pa.Table.from_pydict(orders_data))

        customers_file = path_for(folder, "customers")
        if not store.exists(customers_file):
            customers_data = {
                "customer_id": pa.array([101, 102, 103], type=pa.int64()),
                "name": pa.array(["Acme Corp", "Globex", "Initech"], type=pa.string()),
                "tier": pa.array(["Enterprise", "Pro", "Standard"], type=pa.string()),
                "country": pa.array(["USA", "UK", "Germany"], type=pa.string()),
            }
            self._write_table(customers_file, pa.Table.from_pydict(customers_data))

        # The project's two "enterprise" models read these, so the sample set covers
        # both model families — a run on sample data exercises every shipped model.
        ecommerce_orders_file = path_for(folder, "ecommerce_orders")
        if not store.exists(ecommerce_orders_file):
            orders_data = {
                "order_id": pa.array([9001, 9002], type=pa.int64()),
                "order_uuid": pa.array(
                    [
                        "11111111-1111-1111-1111-111111111111",
                        "22222222-2222-2222-2222-222222222222",
                    ],
                    type=pa.string(),
                ),
                "customer_id": pa.array(
                    [
                        "33333333-3333-3333-3333-333333333333",
                        "44444444-4444-4444-4444-444444444444",
                    ],
                    type=pa.string(),
                ),
                "order_status": pa.array(["paid", "pending"], type=pa.string()),
                "currency": pa.array(["EUR", "USD"], type=pa.string()),
                "subtotal_amount": pa.array([100.0, 40.0], type=pa.float64()),
                "tax_amount": pa.array([19.0, 4.0], type=pa.float64()),
                "discount_amount": pa.array([0.0, 5.0], type=pa.float64()),
                "total_amount": pa.array([119.0, 39.0], type=pa.float64()),
                "order_placed_at": pa.array(
                    ["2026-01-15 10:00:00+00", "2026-01-16 11:30:00+00"], type=pa.string()
                ),
                "shipping_address": pa.array(
                    [
                        '{"city": "Berlin", "street": "Main 1", "is_commercial": false}',
                        '{"city": "Austin", "street": "Oak 7", "is_commercial": true}',
                    ],
                    type=pa.string(),
                ),
            }
            self._write_table(ecommerce_orders_file, pa.Table.from_pydict(orders_data)
            )

        ecommerce_customers_file = path_for(folder, "ecommerce_customers")
        if not store.exists(ecommerce_customers_file):
            ecommerce_customers_data = {
                "customer_id": pa.array(
                    [
                        "33333333-3333-3333-3333-333333333333",
                        "44444444-4444-4444-4444-444444444444",
                    ],
                    type=pa.string(),
                ),
                "first_name": pa.array(["Ada", "Grace"], type=pa.string()),
                "last_name": pa.array(["Lovelace", "Hopper"], type=pa.string()),
                "email": pa.array(["ada@example.com", "grace@example.com"], type=pa.string()),
                "tier": pa.array(["Platinum", "Standard"], type=pa.string()),
                "country_code": pa.array(["DEU", "USA"], type=pa.string()),
                "signup_timestamp": pa.array(
                    ["2026-01-01 09:00:00+00", "2026-01-02 09:00:00+00"], type=pa.string()
                ),
                "metadata_json": pa.array(
                    [
                        '{"referral_source": "search", "preferred_channel": "email", "vip_flag": true}',
                        '{"referral_source": "direct", "preferred_channel": "phone", "vip_flag": false}',
                    ],
                    type=pa.string(),
                ),
            }
            self._write_table(ecommerce_customers_file, pa.Table.from_pydict(ecommerce_customers_data)
            )

    def sync_folder_to_postgres(
        self,
        postgres_params: Dict[str, Any],
        table_names: Optional[List[str]] = None,
        folder: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Copies one folder's Parquet files into the matching warehouse schema.

        The folder is the schema — ``marts/sales`` → ``marts__sales``, flattened
        because Postgres schemas cannot nest — and the files come from that same
        folder, so the path is the only thing that decides where data lives.
        """
        target_folder = (folder or self._dbt_folders()["staging"]).strip().strip("/")
        store = warehouse_storage()
        schema_name = target_folder.replace("/", "__") or "main"
        port = int(postgres_params["port"])
        host = resolve_connection_host(postgres_params["host"], port)
        database = postgres_params["database"]
        user = postgres_params.get("username") or postgres_params.get("user")
        password = postgres_params["password"]
        sslmode = postgres_params.get("sslmode", "disable")

        # The source is the lakehouse catalog, not the folder: a table is a catalog entry
        # now, and its files are whatever the catalog says belong to it.
        from app.core.catalog import scan as scan_lakehouse

        schema_key = target_folder.replace("/", ".")
        source_tables = [
            table for table in scan_lakehouse().tables if (table.folder or "") == schema_key
        ]
        if not source_tables:
            # Nothing to copy: the project has no source tables to read. Say that
            # plainly instead of writing sample tables the operator never asked for.
            raise NoSourceData(
                self._empty_folder_message(target_folder or "/", store.location(target_folder or ""))
            )
        if table_names:
            target_tables = [table for table in source_tables if table.name in table_names]
        else:
            target_tables = source_tables

        if not target_tables:
            return {
                "status": "success",
                "folder": target_folder,
                "schema": schema_name,
                "synced_tables": 0,
                "tables": [],
            }

        schema = sql_ident(schema_name)

        conn = duckdb.connect(":memory:")
        synced_info = []

        try:
            from app.core.catalog import attach_lakehouse

            attach_lakehouse(conn)
            conn.execute("LOAD postgres;")
            conn_str = build_pg_dsn(
                dbname=database,
                host=host,
                port=port,
                user=user,
                password=password,
                sslmode=sslmode,
                connect_timeout=10,
            )
            alias = validate_alias(f"pg_sync_{uuid.uuid4().hex[:6]}")
            conn.execute(f"ATTACH {sql_string(conn_str)} AS {alias} (TYPE POSTGRES);")

            # The folder's own schema, created on first use. `schema` is already quoted.
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{schema};")

            for table in target_tables:
                table_name = table.name
                query = f"""
                    CREATE OR REPLACE TABLE {alias}.{schema}.{sql_ident(table_name)} AS
                    SELECT * FROM {table.sql_name};
                """
                conn.execute(query)

                # Get row count
                cnt_row = conn.execute(
                    f"SELECT COUNT(*) FROM {alias}.{schema}.{sql_ident(table_name)};"
                ).fetchone()
                cnt = cnt_row[0] if cnt_row else 0
                synced_info.append({"table": table_name, "rows": cnt})

            conn.execute(f"DETACH {alias};")
        finally:
            conn.close()

        return {
            "status": "success",
            "folder": target_folder,
            "schema": schema_name,
            "synced_tables": len(synced_info),
            "tables": synced_info,
        }

    def run_dbt(
        self,
        command: str = "run",
        env_vars: Optional[Dict[str, str]] = None,
        select: Optional[str] = None,
        dbt_vars: Optional[Dict[str, Any]] = None,
        on_line: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Executes a dbt command (run, test, seed, compile) inside the dbt_project directory.
        Captures full terminal output and execution metrics.

        ``dbt_vars`` carries the layer → schema mapping, so the project never has to
        know which schema a layer resolves to.
        """
        cmd = [
            sys.executable,
            "-m",
            "dbt.cli.main",
            command,
            "--project-dir",
            str(self.dbt_project_dir),
            "--profiles-dir",
            str(self.dbt_project_dir),
        ]
        if select:
            cmd.extend(["--select", select])
        if dbt_vars:
            cmd.extend(["--vars", json.dumps(dbt_vars)])

        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        start_time = time.time()
        if on_line is None:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                cwd=str(self.dbt_project_dir),
            )
            output = proc.stdout + ("\n" + proc.stderr if proc.stderr else "")
            # Filter out noisy runpy import warnings
            clean_lines = [l for l in output.splitlines() if "RuntimeWarning: 'dbt.cli.main'" not in l]
            output = "\n".join(clean_lines).strip()
            exit_code = proc.returncode
        else:
            # Streaming mode: hand each line to the caller as dbt writes it, so a step's
            # log is readable while the step runs instead of appearing all at once at the
            # end. ``output`` is rebuilt from the same kept lines, so callers that only
            # want the text see no difference.
            kept: List[str] = []
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                cwd=str(self.dbt_project_dir),
            )
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\n")
                if "RuntimeWarning: 'dbt.cli.main'" in line:
                    continue
                kept.append(line)
                on_line(line)
            proc.wait()
            output = "\n".join(kept).strip()
            exit_code = proc.returncode

        duration = round(time.time() - start_time, 2)
        success = exit_code == 0

        return {
            "command": f"dbt {command}",
            "success": success,
            "exit_code": exit_code,
            "duration_seconds": duration,
            "output": output,
        }

    def run_pipeline(
        self,
        postgres_params: Dict[str, Any],
        table_names: Optional[List[str]] = None,
        selector: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Runs the pipeline end to end:
        1. Copies the source folder's Parquet files into the matching warehouse schema
        2. Runs the selected dbt models, each into the schema its folder declares
        3. Runs the dbt tests
        """
        run_id = str(uuid.uuid4())
        now_dt = datetime.now(timezone.utc)
        started_at = now_dt.isoformat()
        time_str = now_dt.strftime("%H:%M:%S UTC")
        logs: List[str] = []

        # Prepare dbt environment variables
        port = int(postgres_params["port"])
        host = resolve_connection_host(postgres_params["host"], port)
        user = postgres_params.get("username") or postgres_params.get("user")
        env_vars = {
            # This pipeline copies the staging folder into PostgreSQL and then builds, so
            # it must build *there*: pin the target instead of inheriting the profile
            # default, which is the lakehouse.
            "DBT_TARGET": self.postgres_target(),
            "DBT_PG_HOST": str(host),
            "DBT_PG_PORT": str(port),
            "DBT_PG_USER": str(user),
            "DBT_PG_PASSWORD": str(postgres_params["password"]),
            "DBT_PG_DBNAME": str(postgres_params["database"]),
            "DBT_PG_SSLMODE": str(postgres_params.get("sslmode", "disable")),
        }

        dbt_vars = self._dbt_vars_payload()
        folders = self._dbt_folders()
        staging_folder = folders["staging"]

        # Step 1: copy the source folder into its schema
        logs.append(
            f"[{time_str}] Step 1/3: Copying folder '{staging_folder}' into warehouse "
            f"schema '{staging_folder}'..."
        )
        try:
            sync_res = self.sync_folder_to_postgres(
                postgres_params=postgres_params,
                table_names=table_names,
                folder=staging_folder,
            )
            tbl_summary = ", ".join([f"{t['table']} ({t['rows']} rows)" for t in sync_res.get("tables", [])])
            logs.append(f"[{time_str}] Synced {sync_res['synced_tables']} tables to warehouse: {tbl_summary}")
        except Exception as e:
            err_msg = (
                f"Failed to copy folder '{staging_folder}' into the warehouse at "
                f"{host}:{port}: {str(e)}"
            )
            logs.append(f"[{time_str}] ERROR: {err_msg}")
            run_record = {
                "run_id": run_id,
                "pipeline_name": pipeline_name or "Full pipeline",
                "selector": selector or "*",
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "duration_seconds": round(time.time() - now_dt.timestamp(), 2),
                "error": err_msg,
                "logs": "\n".join(logs),
            }
            self._record_run(run_record)
            return run_record

        # Step 2: dbt run with selector
        dbt_select = selector if selector and selector != "*" else None
        select_display = f" (selector: '{dbt_select}')" if dbt_select else " (all models)"
        target_folders = ", ".join(
            f"'{folders[slot]}'" for slot in ("staging", "marts")
        )
        logs.append(
            f"\nStep 2/3: Executing dbt run{select_display} "
            f"(writing to schemas {target_folders})..."
        )
        run_res = self.run_dbt(
            command="run", env_vars=env_vars, select=dbt_select, dbt_vars=dbt_vars
        )
        logs.append(run_res["output"])

        if not run_res["success"]:
            run_data = {
                "run_id": run_id,
                "pipeline_name": pipeline_name or "Full pipeline",
                "selector": selector or "*",
                "status": "failed",
                "failed_step": "dbt_run",
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "logs": "\n".join(logs),
            }
            self._record_run(run_data)
            return run_data

        # Step 3: dbt test with selector
        logs.append(f"\nStep 3/3: Executing dbt test{select_display} (enforcing constraints & tests)...")
        test_res = self.run_dbt(
            command="test", env_vars=env_vars, select=dbt_select, dbt_vars=dbt_vars
        )
        logs.append(test_res["output"])

        status = "success" if test_res["success"] else "test_failure"

        # Nothing is registered here: a model's output is a Parquet file, and the
        # catalog finds it the next time anyone reads the catalog.

        run_data = {
            "run_id": run_id,
            "pipeline_name": pipeline_name or "Full pipeline",
            "selector": selector or "*",
            "status": status,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "sync_details": sync_res,
            "dbt_run_duration": run_res["duration_seconds"],
            "dbt_test_duration": test_res["duration_seconds"],
            "logs": "\n".join(logs),
        }
        self._record_run(run_data)
        return run_data

    def folder_tables(self, folder: str) -> List[str]:
        """The table names this warehouse folder holds, from the catalog."""
        from app.core.catalog import scan as scan_lakehouse

        target = (folder or "").strip().strip("/").replace("/", ".")
        if target in ("", "main"):
            target = ""
        return [
            table.name
            for table in scan_lakehouse().tables
            if (table.folder or "") == target
        ]

    def get_model_code(self, folder: str, name: str) -> Optional[str]:
        """Reads SQL source code of a dbt model from git_repo/dbt_project/models/."""
        target_dir = (folder or "marts").strip().strip("/")
        file_path = self.dbt_project_dir / "models" / target_dir / f"{name}.sql"
        if file_path.exists():
            return file_path.read_text(encoding="utf-8")
        return None

    def save_model_code(self, folder: str, name: str, sql: str) -> Dict[str, Any]:
        """Saves or creates a dbt SQL transformation model on disk."""
        clean_name = name.lower().strip().replace(".sql", "")
        target_dir = (folder or "marts").strip().strip("/")
        dir_path = self.dbt_project_dir / "models" / target_dir
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{clean_name}.sql"
        file_path.write_text(sql, encoding="utf-8")
        return {
            "status": "success",
            "name": clean_name,
            "folder": target_dir,
            "path": file_path.as_posix(),
        }

    def get_runs_history(self) -> List[Dict[str, Any]]:
        # After a restart the in-memory list is empty; the table is the record.
        if not self._runs_history:
            self._runs_history.extend(reversed(task_history.recent("dbt", limit=100)))
        return list(reversed(self._runs_history))

    def get_asset_graph(self, selector: Optional[str] = None) -> Dict[str, Any]:
        """Builds the lineage graph from what is on disk.

        Node ids are ``<schema>.<name>``, where the schema is the folder the model is
        configured to write to (from ``+schema`` in the project). A node therefore points
        at the schema the model actually lands in.
        """
        import re

        folders = self._dbt_folders()
        staging_folder = folders["staging"]
        marts_folder = folders["marts"]

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        node_map: Dict[str, str] = {}
        store = warehouse_storage()

        for table_name in self.folder_tables(staging_folder):
            node_id = f"{staging_folder}.{table_name}"
            node_map.setdefault(table_name, node_id)
            nodes.append({
                "id": node_id,
                "name": table_name,
                "schema": staging_folder,
                "type": "parquet",
                "path": store.location(path_for(staging_folder, table_name)),
            })

        # 1 & 2. Scan the project's model folders, in dependency order
        for folder, schema_name, node_type in (
            ("staging", staging_folder, "dbt_model"),
            ("marts", marts_folder, "dbt_mart"),
        ):
            models_dir = self.dbt_project_dir / "models" / folder
            if not models_dir.exists():
                continue
            for sql_file in sorted(models_dir.glob("*.sql")):
                model_name = sql_file.stem
                node_id = f"{schema_name}.{model_name}"
                node_map[model_name] = node_id
                code = sql_file.read_text(encoding="utf-8")
                nodes.append({
                    "id": node_id,
                    "name": model_name,
                    "schema": schema_name,
                    "folder": folder,
                    "type": node_type,
                    "path": sql_file.as_posix(),
                    "sql_code": code,
                })
                # source('<name>', '<table>') reads the staging schema.
                for src_match in re.finditer(
                    r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", code
                ):
                    table = src_match.group(1)
                    edges.append({
                        "source": node_map.get(table, f"{staging_folder}.{table}"),
                        "target": node_id,
                    })
                for ref_match in re.finditer(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", code):
                    ref_name = ref_match.group(1)
                    edges.append({
                        "source": node_map.get(ref_name, f"{staging_folder}.{ref_name}"),
                        "target": node_id,
                    })

        # 3. Dynamically scan custom DAG assets from git_repo/dags/
        custom_dag_nodes = []
        try:
            from app.engines.dag_loader import dag_loader
            custom_dag_nodes = dag_loader.introspect_dag_nodes()
            for cnode in custom_dag_nodes:
                c_name = cnode["name"]
                c_id = cnode["id"]
                node_map[c_name] = c_id
                for dep in cnode.get("deps", []):
                    upstream_id = node_map.get(dep)
                    if not upstream_id:
                        # A dependency may name a model without its layer prefix.
                        matches = [n["id"] for n in nodes if n["name"] == dep]
                        upstream_id = matches[0] if matches else None
                    if upstream_id:
                        edges.append({"source": upstream_id, "target": c_id})
        except Exception as e:
            print(f"[DbtRunner] Warning: Custom DAG introspection failed: {e}")

        # Deduplicate edges
        unique_edges = []
        seen = set()
        for e in edges:
            key = (e["source"], e["target"])
            if key not in seen:
                seen.add(key)
                unique_edges.append(e)

        all_nodes = nodes + custom_dag_nodes

        # Selector active node calculation
        active_ids = set()
        if not selector or selector == "*":
            active_ids = {n["id"] for n in all_nodes}
        elif selector == "models/staging" or selector == "staging":
            for n in all_nodes:
                if n.get("folder") == "staging":
                    active_ids.add(n["id"])
                    for e in unique_edges:
                        if e["target"] == n["id"]:
                            active_ids.add(e["source"])
        elif selector == "models/marts" or selector == "marts":
            for n in all_nodes:
                if n.get("folder") == "marts":
                    active_ids.add(n["id"])
                    for e in unique_edges:
                        if e["target"] == n["id"]:
                            active_ids.add(e["source"])
        else:
            # Model-based selector e.g. "stg_orders+" or "+fct_orders_enriched" or "stg_orders"
            clean_sel = selector.strip()
            include_downstream = clean_sel.endswith("+")
            include_upstream = clean_sel.startswith("+")
            target_name = clean_sel.strip("+")

            target_nodes = [n for n in all_nodes if n["name"] == target_name]
            for tn in target_nodes:
                active_ids.add(tn["id"])

            if include_downstream:
                frontier = list(active_ids)
                while frontier:
                    curr = frontier.pop(0)
                    for e in unique_edges:
                        if e["source"] == curr and e["target"] not in active_ids:
                            active_ids.add(e["target"])
                            frontier.append(e["target"])

            if include_upstream:
                frontier = list(active_ids)
                while frontier:
                    curr = frontier.pop(0)
                    for e in unique_edges:
                        if e["target"] == curr and e["source"] not in active_ids:
                            active_ids.add(e["source"])
                            frontier.append(e["source"])

            # Also always include direct inputs of active models so DAG is coherent
            for e in unique_edges:
                if e["target"] in active_ids:
                    active_ids.add(e["source"])

        for n in all_nodes:
            n["is_in_pipeline"] = n["id"] in active_ids

        schemas = {n["schema"] for n in all_nodes if n.get("schema")}
        if custom_dag_nodes:
            schemas.add("custom")
        schemas = sorted(schemas)

        return {
            "nodes": all_nodes,
            "edges": unique_edges,
            "schemas": schemas,
            "active_pipeline_selector": selector or "*",
            "active_node_count": len([n for n in all_nodes if n.get("is_in_pipeline", False)]),
        }
    def _resolve_postgres_params(
        self,
        custom_params: Optional[Dict[str, Any]] = None,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Credentials for the PostgreSQL warehouse a build writes into.

        ``connection_id`` is the binding the caller chose; without it the operator's saved
        selection is used. There is no fallback to "some Postgres connection" — a build
        either targets a chosen warehouse or refuses.
        """
        return resolve_write_warehouse_postgres_params(custom_params, connection_id)

    # ---------------------------------------------------------- dbt targets

    def profiles_path(self) -> Path:
        return self.dbt_project_dir / "profiles.yml"

    def _read_profiles(self) -> Dict[str, Any]:
        """Parses ``profiles.yml``.

        Jinja only ever appears inside quoted strings (``"{{ env_var(...) }}"``), so the
        file is valid YAML and safe to read. A broken profile returns ``{}`` rather than
        taking down the Studio's target list.
        """
        import yaml

        path = self.profiles_path()
        if not path.is_file():
            return {}
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, ValueError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _profile_default(value: Any, fallback: str) -> str:
        """``"{{ env_var('DBT_TARGET', 'duckdb') }}"`` -> ``"duckdb"``."""
        if not isinstance(value, str):
            return fallback
        match = re.search(
            r"env_var\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]", value
        )
        return match.group(1) if match else (value.strip() or fallback)

    def _profile_block(self) -> tuple:
        """Returns ``(project_name, outputs)`` from the first profile with outputs."""
        for name, block in self._read_profiles().items():
            if isinstance(block, dict) and isinstance(block.get("outputs"), dict):
                return name, block["outputs"]
        return "obelisk", {}

    def default_target(self) -> str:
        """The profile's own default target, not a guess about adapters."""
        for block in self._read_profiles().values():
            if isinstance(block, dict) and isinstance(block.get("outputs"), dict):
                return self._profile_default(block.get("target"), "dev")
        return "dev"

    def postgres_target(self) -> str:
        """The output that publishes into PostgreSQL (``dev`` in the shipped profile).

        Derived from ``profiles.yml`` rather than hardcoded: a build that syncs the
        staging folder into PostgreSQL must also *build* there, and the name of that
        output is the operator's to choose.
        """
        _, outputs = self._profile_block()
        for name, config in outputs.items():
            if isinstance(config, dict) and str(config.get("type", "")).lower() == "postgres":
                return name
        return "dev"

    def _target_adapter(self, name: str) -> str:
        """The adapter type declared for one output; ``""`` when the name is unknown."""
        _, outputs = self._profile_block()
        config = outputs.get(name)
        return str(config.get("type") or "").lower() if isinstance(config, dict) else ""

    def list_targets(self) -> Dict[str, Any]:
        """Every target the project defines, as *bindings* the operator can pick.

        ``profiles.yml`` names the adapters; a Postgres output still needs to know which
        database it publishes into. So a Postgres output appears once per registered
        Postgres connection — picking a row *is* choosing the warehouse — while a duckdb
        output appears once. Nothing is resolved until a row is picked.
        """
        from importlib.util import find_spec

        from app.core.warehouse_resolver import describe_warehouse_postgres, warehouse_candidates

        project, outputs = self._profile_block()
        candidates = warehouse_candidates()
        targets: List[Dict[str, Any]] = []
        for name, config in outputs.items():
            config = config if isinstance(config, dict) else {}
            adapter = str(config.get("type", "unknown")).lower()
            available = find_spec(f"dbt.adapters.{adapter}") is not None

            if adapter == "postgres":
                if not candidates:
                    targets.append(
                        {
                            "name": name,
                            "adapter": adapter,
                            "available": available,
                            "connection_id": None,
                            "connection": None,
                            "read_only": True,
                            "detail": {
                                "source": "unbound",
                                "message": "No Postgres connection registered. Add one in "
                                "Settings → Connection Vault.",
                            },
                        }
                    )
                    continue
                for candidate in candidates:
                    targets.append(
                        {
                            "name": name,
                            "adapter": adapter,
                            "available": available,
                            "connection_id": candidate["id"],
                            "connection": candidate["name"],
                            "read_only": candidate["read_only"],
                            "detail": {
                                "host": candidate["host"],
                                "port": candidate["port"],
                                "database": candidate["database"],
                                "source": "connection",
                                "connection": candidate["name"],
                                "read_only": candidate["read_only"],
                            },
                        }
                    )
                continue

            detail: Dict[str, Any] = {}
            if adapter == "duckdb":
                detail = {
                    "path": (settings.lakehouse_root_path / "lakehouse.duckdb")
                    .resolve()
                    .as_posix(),
                    "source": "local file",
                }
            elif config.get("OBELISK_CONNECTION"):
                detail = {"source": f"vault:{config['OBELISK_CONNECTION']}"}
            targets.append(
                {
                    "name": name,
                    "adapter": adapter,
                    "available": available,
                    "connection_id": None,
                    "connection": None,
                    "read_only": False,
                    "detail": detail,
                }
            )
        return {
            "project": project,
            "default": self.default_target(),
            "targets": targets,
            "warehouse": describe_warehouse_postgres(),
        }

    def _get_target_env(
        self, target: str = "", connection_id: Optional[str] = None
    ) -> Dict[str, str]:
        """Environment for one dbt target.

        The target name is passed through untouched: if the profile calls its default
        ``dev``, dbt must see ``dev``. Credentials come from an ``OBELISK_CONNECTION``
        reference in the profile, or — for a Postgres output — from the connection the
        caller chose (``connection_id``) or the operator selected in Settings. Nothing is
        guessed: an unbound Postgres target raises instead of picking a database.
        """
        clean_target = (target or self.default_target()).strip() or self.default_target()
        _, outputs = self._profile_block()
        if clean_target not in outputs and clean_target.lower() == "postgres":
            # ``postgres`` used to be a duplicate output; it now means "the PostgreSQL
            # target", so an env var left over from that era keeps working.
            clean_target = self.postgres_target()
        config = outputs.get(clean_target, {})
        config = config if isinstance(config, dict) else {}
        adapter = str(config.get("type") or "").lower()

        env: Dict[str, str] = {"DBT_TARGET": clean_target}

        connection_ref = config.get("OBELISK_CONNECTION")
        if isinstance(connection_ref, str) and connection_ref.strip():
            env.update(self._connection_env(connection_ref.strip()))

        if adapter == "postgres":
            pg = self._resolve_postgres_params(connection_id=connection_id)
            resolved_host = resolve_connection_host(pg["host"], int(pg["port"]))
            env.update(
                {
                    "DBT_PG_HOST": str(resolved_host),
                    "DBT_PG_PORT": str(pg["port"]),
                    "DBT_PG_USER": str(pg.get("username") or pg.get("user") or "testuser"),
                    "DBT_PG_PASSWORD": str(pg.get("password") or "testpass"),
                    "DBT_PG_DBNAME": str(pg.get("database") or "sourcedb"),
                    "DBT_PG_SSLMODE": str(pg.get("sslmode", "disable")),
                }
            )
        elif adapter == "duckdb":
            env["DBT_DUCKDB_PATH"] = (
                settings.lakehouse_root_path / "lakehouse.duckdb"
            ).resolve().as_posix()
            # The lakehouse output attaches the DuckLake catalog; give dbt its path so
            # `dbt run` writes tables into the lake instead of a file.
            env["DBT_LAKEHOUSE_CATALOG"] = settings.lakehouse_catalog_path.as_posix()

        return env

    @staticmethod
    def _connection_env(reference: str) -> Dict[str, str]:
        """Credentials for ``OBELISK_CONNECTION`` references, from the vault.

        The profile declares a connection by name or id and reads the exported
        ``DBT_*`` variables, so credentials never sit in a file that gets committed.
        """
        import sqlite3

        try:
            with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
                row = con.execute(
                    "SELECT host, port, database, username, encrypted_credentials "
                    "FROM connections WHERE id = ? OR name = ? LIMIT 1",
                    (reference, reference),
                ).fetchone()
        except Exception:
            return {}
        if not row:
            return {}
        host, port, database, username, encrypted = row
        try:
            from app.core.vault import decrypt

            password = decrypt(encrypted) if encrypted else ""
        except Exception:
            password = ""
        env: Dict[str, str] = {}
        if host:
            env["DBT_HOST"] = str(resolve_connection_host(host, int(port or 0) or 5432))
        if port:
            env["DBT_PORT"] = str(port)
        if username:
            env["DBT_USER"] = str(username)
        if password:
            env["DBT_PASSWORD"] = password
        if database:
            env["DBT_DATABASE"] = str(database)
        return env

    def run_scoped_dbt(
        self,
        command: str = "run",
        selector: Optional[str] = None,
        target: str = "",
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes dbt <command> --select <selector> --target <target>.
        Captures exit codes, durations, and clean terminal logs.

        An empty ``target`` means the profile's default. ``postgres`` is accepted as a
        legacy alias for whichever output uses the postgres adapter. ``connection_id``
        names the warehouse a Postgres target publishes into; without it the saved
        selection is used.
        """
        clean_target = (target or self.default_target()).strip().lower()
        if clean_target == "postgres" and self._target_adapter(clean_target) == "":
            clean_target = self.postgres_target().lower()
        is_postgres_target = self._target_adapter(clean_target) == "postgres"

        dbt_vars = self._dbt_vars_payload()
        staging_folder = self._dbt_folders()["staging"]
        store = warehouse_storage()

        # A build against an empty source folder has nothing to read: report that,
        # with the fix, rather than seeding sample tables and calling it a success.
        if is_postgres_target and command in ("run", "test"):
            if not self.folder_tables(staging_folder):
                return self._no_source_data_result(
                    command,
                    selector,
                    clean_target,
                    staging_folder,
                    store.location(staging_folder),
                )

        if is_postgres_target and command == "run":
            pg_params = self._resolve_postgres_params(connection_id=connection_id)
            try:
                self.sync_folder_to_postgres(pg_params, folder=staging_folder)
            except NoSourceData as exc:
                # Scoped syncs pass their own table filter; an empty folder is still
                # nothing to build, so keep the explicit answer.
                return {
                    **self._no_source_data_result(
                        command,
                        selector,
                        clean_target,
                        staging_folder,
                        store.location(staging_folder),
                    ),
                    "error": str(exc),
                    "output": str(exc),
                    "logs": str(exc),
                }
            except Exception:
                pass

        env_vars = self._get_target_env(clean_target, connection_id=connection_id)
        cmd = [
            sys.executable,
            "-m",
            "dbt.cli.main",
            command,
            "--project-dir",
            str(self.dbt_project_dir),
            "--profiles-dir",
            str(self.dbt_project_dir),
            "--target",
            clean_target,
        ]
        if selector and selector != "*":
            clean_sel = selector.strip()
            cmd.extend(["--select", clean_sel])
        cmd.extend(["--vars", json.dumps(dbt_vars)])

        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        start_time = time.time()
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            cwd=str(self.dbt_project_dir),
        )
        duration_sec = round(time.time() - start_time, 2)
        duration_ms = int(duration_sec * 1000)

        output = proc.stdout + ("\n" + proc.stderr if proc.stderr else "")
        clean_lines = [l for l in output.splitlines() if "RuntimeWarning: 'dbt.cli.main'" not in l]
        output = "\n".join(clean_lines).strip()
        success = proc.returncode == 0

        # No catalog write on success: the models' Parquet files are the record, and
        # the catalog projection reads them when someone asks what exists.

        return {
            "command": f"dbt {command}",
            "selector": selector or "*",
            "target": clean_target,
            "success": success,
            "exit_code": proc.returncode,
            "duration_seconds": duration_sec,
            "duration_ms": duration_ms,
            "output": output,
            "logs": output,
        }

    def compile_model(
        self, model_name: str, target: str = "postgres", connection_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Runs dbt compile --select <model_name> --target <target> via a sys.executable -m dbt.cli.main.
        Reads the generated compiled SQL from target/compiled/obelisk_dbt/models/.../<model_name>.sql and returns it.
        """
        clean_name = model_name.strip().replace(".sql", "")
        # Run dbt compile
        res = self.run_scoped_dbt(
            command="compile", selector=clean_name, target=target, connection_id=connection_id
        )

        # Read compiled SQL from target/compiled/
        compiled_sql = ""
        compiled_dir = self.dbt_project_dir / "target" / "compiled" / "obelisk_dbt" / "models"
        if compiled_dir.exists():
            matches = list(compiled_dir.rglob(f"{clean_name}.sql"))
            if matches:
                latest_match = max(matches, key=lambda f: f.stat().st_mtime)
                compiled_sql = latest_match.read_text(encoding="utf-8")

        if not compiled_sql:
            target_compiled = self.dbt_project_dir / "target" / "compiled"
            if target_compiled.exists():
                all_matches = list(target_compiled.rglob(f"{clean_name}.sql"))
                if all_matches:
                    latest_match = max(all_matches, key=lambda f: f.stat().st_mtime)
                    compiled_sql = latest_match.read_text(encoding="utf-8")

        return {
            "model_name": clean_name,
            "target": target,
            "compiled_sql": compiled_sql or "",
            "duration_ms": res.get("duration_ms", int(res.get("duration_seconds", 0) * 1000)),
            "logs": res.get("output", ""),
            "success": res.get("success", False) and bool(compiled_sql),
        }

    def preview_model_data(
        self,
        model_name: str,
        target: str = "postgres",
        limit: int = 50,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compiles the model, wraps the compiled SQL with SELECT * FROM (...) AS _subq LIMIT 50,
        and executes it via NativeQueryEngine or DuckDBEngine to return column headers and data rows as JSON.
        """
        clean_name = model_name.strip().replace(".sql", "")
        comp_res = self.compile_model(
            model_name=clean_name, target=target, connection_id=connection_id
        )
        compiled_sql = comp_res.get("compiled_sql", "").strip()

        if not compiled_sql:
            return {
                "columns": [],
                "rows": [],
                "row_count": 0,
                "duration_ms": comp_res.get("duration_ms", 0),
                "status": "error",
                "error": f"Failed to compile model '{clean_name}'. Logs:\n{comp_res.get('logs', '')}",
            }

        clean_target = (target or self.default_target()).strip().lower()
        if clean_target == "postgres" and self._target_adapter(clean_target) == "":
            clean_target = self.postgres_target().lower()
        if self._target_adapter(clean_target) == "postgres":
            pg_params = self._resolve_postgres_params(connection_id=connection_id)
            db_name = pg_params.get("database", "sourcedb")

            import re
            # Clean cross-database references e.g. "sourcedb"."bronze" -> "bronze" for local PG execution
            sql_for_preview = re.sub(rf'"{re.escape(db_name)}"\.', '', compiled_sql, flags=re.IGNORECASE)
            sql_for_preview = re.sub(rf'{re.escape(db_name)}\.', '', sql_for_preview, flags=re.IGNORECASE)
            wrapped_sql = f"SELECT * FROM (\n{sql_for_preview}\n) AS _subq LIMIT {limit};"

            from app.engines.native_query_engine import NativeQueryEngine
            res = NativeQueryEngine.execute_postgres(
                host=pg_params["host"],
                port=int(pg_params["port"]),
                database=pg_params["database"],
                username=pg_params["username"],
                password=pg_params["password"],
                sql=wrapped_sql,
                sslmode=pg_params.get("sslmode", "disable"),
            )
            res["model_name"] = clean_name
            res["target"] = target
            return res
        else:
            from app.engines.duckdb_engine import DuckDBEngine
            duck_engine = DuckDBEngine()
            wrapped_sql = f"SELECT * FROM (\n{compiled_sql}\n) AS _subq LIMIT {limit};"
            res = duck_engine.execute_query(wrapped_sql)
            res["model_name"] = clean_name
            res["target"] = target
            return res

    def get_dbt_project_tree(self) -> Dict[str, Any]:
        """
        Recursively inspects git_repo/dbt_project/ and returns a structured JSON tree
        of directories and files (models/staging, models/marts, macros, seeds, schema.yml, dbt_project.yml).
        """
        ignore_names = {".git", ".user.yml", "target", "logs", "dbt_packages", "__pycache__", ".venv"}

        def _build_node(path: Path) -> Dict[str, Any]:
            rel_path = path.relative_to(self.dbt_project_dir).as_posix()
            if path.is_dir():
                children = []
                for child in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
                    if child.name in ignore_names:
                        continue
                    children.append(_build_node(child))
                return {
                    "name": path.name,
                    "path": rel_path,
                    "type": "directory",
                    "children": children,
                }
            else:
                stat = path.stat()
                ext = path.suffix.lower()
                folder = None
                if "models/staging" in rel_path or "models\\staging" in rel_path:
                    folder = "staging"
                elif "models/marts" in rel_path or "models\\marts" in rel_path:
                    folder = "marts"

                return {
                    "name": path.name,
                    "path": rel_path,
                    "type": "file",
                    "extension": ext,
                    "folder": folder,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }

        root_children = []
        for child in sorted(self.dbt_project_dir.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            if child.name in ignore_names:
                continue
            root_children.append(_build_node(child))

        return {
            "name": "dbt_project",
            "path": "",
            "type": "directory",
            "children": root_children,
        }

    def _resolve_safe_path(self, rel_path: str) -> Path:
        clean = Path(rel_path.strip().replace("\\", "/"))
        if clean.is_absolute() or ".." in clean.parts:
            raise ValueError(f"Path traversal detected or invalid path: {rel_path}")
        target = (self.dbt_project_dir / clean).resolve()
        try:
            target.relative_to(self.dbt_project_dir)
        except ValueError:
            raise ValueError(f"Access denied: path outside dbt_project: {rel_path}")
        return target

    def read_project_file(self, rel_path: str) -> Dict[str, Any]:
        """Reads file content from git_repo/dbt_project/."""
        target = self._resolve_safe_path(rel_path)
        if not target.exists():
            raise FileNotFoundError(f"File not found: {rel_path}")
        if not target.is_file():
            raise IsADirectoryError(f"Path is a directory, not a file: {rel_path}")

        content = target.read_text(encoding="utf-8")
        stat = target.stat()
        return {
            "path": rel_path,
            "name": target.name,
            "extension": target.suffix.lower(),
            "content": content,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        }

    def write_project_file(self, rel_path: str, content: str) -> Dict[str, Any]:
        """Saves file changes safely to disk."""
        target = self._resolve_safe_path(rel_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        stat = target.stat()
        return {
            "status": "success",
            "path": rel_path,
            "name": target.name,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        }

    def create_project_file(self, rel_path: str, template: Optional[str] = None) -> Dict[str, Any]:
        """Creates a .sql or .yml file with default boilerplate template."""
        target = self._resolve_safe_path(rel_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        if template is not None and template.strip():
            content = template
        else:
            model_stem = target.stem
            posix_rel = rel_path.replace("\\", "/").lower()
            if target.suffix.lower() == ".sql":
                if "staging" in posix_rel:
                    content = (
                        "{{ config(materialized='table') }}\n\n"
                        "with source as (\n"
                        f"    select * from {{{{ source('landing', '{model_stem.replace('stg_', '')}') }}}}\n"
                        "),\n\n"
                        "cleaned as (\n"
                        "    select\n"
                        "        cast(id as integer) as id,\n"
                        "        cast(created_at as timestamp) as created_at\n"
                        "    from source\n"
                        ")\n\n"
                        "select * from cleaned\n"
                    )
                else:
                    content = (
                        "{{ config(materialized='table') }}\n\n"
                        "with staging as (\n"
                        "    select * from {{ ref('stg_orders') }}\n"
                        ")\n\n"
                        "select\n"
                        "    date_trunc('day', order_timestamp) as metric_date,\n"
                        "    count(*) as total_records,\n"
                        "    sum(amount) as total_volume\n"
                        "from staging\n"
                        "group by 1\n"
                    )
            elif target.suffix.lower() in (".yml", ".yaml"):
                content = (
                    "version: 2\n\n"
                    "models:\n"
                    f"  - name: {model_stem}\n"
                    f"    description: \"Model {model_stem}\"\n"
                    "    columns:\n"
                    "      - name: id\n"
                    "        tests:\n"
                    "          - unique\n"
                    "          - not_null\n"
                )
            else:
                content = ""

        target.write_text(content, encoding="utf-8")
        return {
            "status": "success",
            "path": rel_path,
            "name": target.name,
            "content": content,
            "size_bytes": len(content.encode("utf-8")),
        }

    def delete_project_file(self, rel_path: str) -> Dict[str, Any]:
        """Deletes a file safely from git_repo/dbt_project/."""
        target = self._resolve_safe_path(rel_path)
        if not target.exists():
            raise FileNotFoundError(f"File not found: {rel_path}")
        if not target.is_file():
            raise IsADirectoryError(f"Cannot delete directory directly: {rel_path}")

        target.unlink()
        return {
            "status": "success",
            "path": rel_path,
            "name": target.name,
        }

    def get_model_lineage(self, model_name: str) -> Dict[str, Any]:
        """
        Extracts and returns the dependency sub-graph for the selected model from target/manifest.json
        (with dynamic AST/regex fallback).
        """
        clean_name = model_name.strip().replace(".sql", "")
        manifest_path = self.dbt_project_dir / "target" / "manifest.json"

        folders = self._dbt_folders()
        staging_folder = folders["staging"]
        marts_folder = folders["marts"]

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        target_node_id = f"model.obelisk_dbt.{clean_name}"
        found_in_manifest = False

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)

                manifest_nodes = manifest.get("nodes", {})
                parent_map = manifest.get("parent_map", {})
                child_map = manifest.get("child_map", {})

                matching_id = None
                for nid in manifest_nodes.keys():
                    if nid == target_node_id or nid.endswith(f".{clean_name}"):
                        matching_id = nid
                        break

                if matching_id:
                    found_in_manifest = True
                    target_node = manifest_nodes[matching_id]
                    target_path = target_node.get("original_file_path", "")
                    target_folder = "staging" if "staging" in target_path else "marts"
                    schema = target_node.get("config", {}).get("schema") or (
                        staging_folder if target_folder == "staging" else marts_folder
                    )

                    # Current Node
                    nodes.append({
                        "id": matching_id,
                        "name": clean_name,
                        "type": "model",
                        "schema": schema,
                        "folder": target_folder,
                        "is_current": True,
                        "materialized": target_node.get("config", {}).get("materialized", "table"),
                        "path": target_path,
                    })

                    # Upstream Parents
                    upstreams = parent_map.get(matching_id, [])
                    for up_id in upstreams:
                        if up_id.startswith("macro."):
                            continue
                        up_name = up_id.split(".")[-1]
                        is_source = up_id.startswith("source.")
                        up_path = manifest_nodes.get(up_id, {}).get("original_file_path", "")
                        up_folder = "staging" if "staging" in up_path else "marts"
                        up_schema = staging_folder if is_source or up_folder == "staging" else marts_folder
                        nodes.append({
                            "id": up_id,
                            "name": up_name,
                            "type": "source" if is_source else "model",
                            "schema": up_schema,
                            "folder": "landing" if is_source else up_folder,
                            "is_current": False,
                        })
                        edges.append({"source": up_id, "target": matching_id})

                    # Downstream Children
                    downstreams = child_map.get(matching_id, [])
                    for down_id in downstreams:
                        down_name = down_id.split(".")[-1]
                        is_test = down_id.startswith("test.")
                        down_path = manifest_nodes.get(down_id, {}).get("original_file_path", "")
                        down_folder = "staging" if "staging" in down_path else "marts"
                        down_schema = staging_folder if down_folder == "staging" else marts_folder
                        nodes.append({
                            "id": down_id,
                            "name": down_name if not is_test else down_id.split(".")[2],
                            "type": "test" if is_test else "model",
                            "schema": down_schema if not is_test else "test",
                            "folder": down_folder,
                            "is_current": False,
                        })
                        edges.append({"source": matching_id, "target": down_id})
            except Exception:
                pass

        if not found_in_manifest:
            code = self.get_model_code(staging_folder, clean_name) or self.get_model_code(marts_folder, clean_name)
            current_folder = staging_folder if self.get_model_code(staging_folder, clean_name) else marts_folder
            nodes.append({
                "id": target_node_id,
                "name": clean_name,
                "type": "model",
                "schema": current_folder,
                "is_current": True,
            })

            if code:
                import re
                for src_match in re.finditer(r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", code):
                    s_name = src_match.group(1)
                    s_id = f"source.obelisk_dbt.landing.{s_name}"
                    nodes.append({
                        "id": s_id,
                        "name": s_name,
                        "type": "source",
                        "schema": staging_folder,
                        "is_current": False,
                    })
                    edges.append({"source": s_id, "target": target_node_id})

                for ref_match in re.finditer(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", code):
                    r_name = ref_match.group(1)
                    r_id = f"model.obelisk_dbt.{r_name}"
                    r_folder = staging_folder if "stg_" in r_name else marts_folder
                    nodes.append({
                        "id": r_id,
                        "name": r_name,
                        "type": "model",
                        "schema": r_folder,
                        "is_current": False,
                    })
                    edges.append({"source": r_id, "target": target_node_id})

            all_models_dir = self.dbt_project_dir / "models"
            if all_models_dir.exists():
                import re
                for other_file in all_models_dir.rglob("*.sql"):
                    if other_file.stem == clean_name:
                        continue
                    other_code = other_file.read_text(encoding="utf-8")
                    if f"ref('{clean_name}')" in other_code or f'ref("{clean_name}")' in other_code:
                        other_id = f"model.obelisk_dbt.{other_file.stem}"
                        other_folder = staging_folder if "staging" in str(other_file) else marts_folder
                        nodes.append({
                            "id": other_id,
                            "name": other_file.stem,
                            "type": "model",
                            "schema": other_folder,
                            "is_current": False,
                        })
                        edges.append({"source": target_node_id, "target": other_id})

        unique_nodes = []
        seen_nodes = set()
        for n in nodes:
            if n["id"] not in seen_nodes:
                seen_nodes.add(n["id"])
                unique_nodes.append(n)

        unique_edges = []
        seen_edges = set()
        for e in edges:
            pair = (e["source"], e["target"])
            if pair not in seen_edges:
                seen_edges.add(pair)
                unique_edges.append(e)

        return {
            "model_name": clean_name,
            "nodes": unique_nodes,
            "edges": unique_edges,
            "total_nodes": len(unique_nodes),
            "total_edges": len(unique_edges),
        }

dbt_runner = DbtRunner()
