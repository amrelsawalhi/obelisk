"""
dbt Runner & Pipeline Engine:
- Stages landing Parquet files into the PostgreSQL warehouse, one schema per layer
- Executes dbt run and dbt test via python -m dbt.cli.main
- Exposes the model graph with node ids keyed by the layer each model targets
- Tracks pipeline run history and registers the transformed models in SQLite

Which warehouse schema a model lands in comes from the layer it targets, never from
the folder it sits in: the project declares two configurable slots (staging, marts),
Obelisk resolves each slot's layer path to a schema and passes the mapping to dbt, and
a `generate_schema_name` macro applies it. Rename a layer and the pipeline follows.
"""

import os
import sys
import time
import json
import uuid
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import duckdb
from app.config import settings
from app.core.host_resolver import resolve_connection_host
from app.core.dsn import build_pg_dsn, sql_ident, sql_string, validate_alias
from app.core.layers import InvalidLayerPath, LayerLocation, resolve_layer
from app.core.warehouse_resolver import resolve_warehouse_postgres_params
from app.services.layers import default_layer_path, layer_paths, layer_schemas
from app.services.state import task_history

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

    def _dbt_layer_vars(self) -> Dict[str, str]:
        """Layer paths the bundled project writes to, read from its own ``vars`` block.

        ``staging`` and ``marts`` are folder names in the project; the layer each one
        targets is configuration, so repointing or renaming a layer never needs a code
        change.
        """
        project_file = self.dbt_project_dir / "dbt_project.yml"
        try:
            import yaml

            document = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}
        except Exception:
            document = {}

        variables = document.get("vars") or {}
        if isinstance(variables, dict) and "obelisk_dbt" in variables:
            variables = variables["obelisk_dbt"] or {}

        return {
            "staging": str(variables.get("layer_staging") or "raw"),
            "marts": str(variables.get("layer_marts") or "marts"),
        }

    def _dbt_vars_payload(self) -> Dict[str, Any]:
        """The ``--vars`` payload: which layer each folder targets, and how paths flatten.

        Every known layer is included so a model can target any of them, and the paths
        the project itself declares are included even when no layer row exists yet —
        that way a dotted path still resolves to its flattened schema instead of
        silently becoming a schema with a dot in its name.
        """
        targets = self._dbt_layer_vars()
        mapping = layer_schemas()
        for path in targets.values():
            try:
                mapping.setdefault(path, resolve_layer(path).warehouse_schema)
            except InvalidLayerPath:
                continue
        return {
            "layer_staging": targets["staging"],
            "layer_marts": targets["marts"],
            "layer_schemas": mapping,
        }

    def _ensure_core_layer_tables(self, location: LayerLocation) -> None:
        """Seeds two demo tables into a layer that holds no data yet.

        The shipped models reference ``orders`` and ``customers``. On a fresh install
        there is nothing to read, and a pipeline that dies on a missing relation is a
        worse first run than one that works on sample data. Only called for an empty
        layer, so it can never shadow real tables.
        """
        # lakehouse_root_path creates the directory and rejects remote schemes;
        # no local mkdir logic belongs here (audit B1).
        directory = location.filesystem_dir
        directory.mkdir(parents=True, exist_ok=True)

        orders_file = directory / "orders.parquet"
        if not orders_file.exists():
            import pyarrow as pa
            import pyarrow.parquet as pq

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
            pq.write_table(pa.Table.from_pydict(orders_data), orders_file, compression="SNAPPY")

        customers_file = directory / "customers.parquet"
        if not customers_file.exists():
            import pyarrow as pa
            import pyarrow.parquet as pq

            customers_data = {
                "customer_id": pa.array([101, 102, 103], type=pa.int64()),
                "name": pa.array(["Acme Corp", "Globex", "Initech"], type=pa.string()),
                "tier": pa.array(["Enterprise", "Pro", "Standard"], type=pa.string()),
                "country": pa.array(["USA", "UK", "Germany"], type=pa.string()),
            }
            pq.write_table(pa.Table.from_pydict(customers_data), customers_file, compression="SNAPPY")

        # The project's two "enterprise" models read these, so a fresh install needs
        # them too — otherwise a default run fails on a missing relation.
        ecommerce_orders_file = directory / "ecommerce_orders.parquet"
        if not ecommerce_orders_file.exists():
            import pyarrow as pa
            import pyarrow.parquet as pq

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
            pq.write_table(
                pa.Table.from_pydict(orders_data), ecommerce_orders_file, compression="SNAPPY"
            )

        ecommerce_customers_file = directory / "ecommerce_customers.parquet"
        if not ecommerce_customers_file.exists():
            import pyarrow as pa
            import pyarrow.parquet as pq

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
            pq.write_table(
                pa.Table.from_pydict(ecommerce_customers_data),
                ecommerce_customers_file,
                compression="SNAPPY",
            )

    def sync_layer_to_postgres(
        self,
        postgres_params: Dict[str, Any],
        table_names: Optional[List[str]] = None,
        layer_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Copies one layer's Parquet files into that layer's warehouse schema.

        The schema comes from the layer path (``marts.sales`` -> ``marts__sales``) and
        the files come from that same layer's directory, so a layer is the single thing
        that decides where its data lives.
        """
        location = resolve_layer(layer_path or default_layer_path())
        port = int(postgres_params["port"])
        host = resolve_connection_host(postgres_params["host"], port)
        database = postgres_params["database"]
        user = postgres_params.get("username") or postgres_params.get("user")
        password = postgres_params["password"]
        sslmode = postgres_params.get("sslmode", "disable")

        all_parquet = self._discover_layer_parquet(location)
        if not all_parquet:
            # Nothing to copy and nothing to overwrite: seed sample tables so the
            # shipped models have something to read on a fresh install.
            self._ensure_core_layer_tables(location)
            all_parquet = self._discover_layer_parquet(location)
        if table_names:
            target_files = [f for f in all_parquet if f.stem in table_names]
        else:
            target_files = all_parquet

        if not target_files:
            return {
                "status": "success",
                "layer": location.path,
                "schema": location.warehouse_schema,
                "synced_tables": 0,
                "tables": [],
            }

        schema = sql_ident(location.warehouse_schema)

        conn = duckdb.connect(":memory:")
        synced_info = []

        try:
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

            # The layer's own schema, created on first use.
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{schema};")

            for pf in target_files:
                table_name = pf.stem
                posix_src = pf.resolve().as_posix()
                query = f"""
                    CREATE OR REPLACE TABLE {alias}.{schema}."{table_name}" AS
                    SELECT * FROM read_parquet('{posix_src}');
                """
                conn.execute(query)

                # Get row count
                cnt_row = conn.execute(f'SELECT COUNT(*) FROM {alias}.{schema}."{table_name}";').fetchone()
                cnt = cnt_row[0] if cnt_row else 0
                synced_info.append({"table": table_name, "rows": cnt})

            conn.execute(f"DETACH {alias};")
        finally:
            conn.close()

        return {
            "status": "success",
            "layer": location.path,
            "schema": location.warehouse_schema,
            "synced_tables": len(synced_info),
            "tables": synced_info,
        }

    def run_dbt(
        self,
        command: str = "run",
        env_vars: Optional[Dict[str, str]] = None,
        select: Optional[str] = None,
        dbt_vars: Optional[Dict[str, Any]] = None,
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
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            cwd=str(self.dbt_project_dir),
        )
        duration = round(time.time() - start_time, 2)

        output = proc.stdout + ("\n" + proc.stderr if proc.stderr else "")
        # Filter out noisy runpy import warnings
        clean_lines = [l for l in output.splitlines() if "RuntimeWarning: 'dbt.cli.main'" not in l]
        output = "\n".join(clean_lines).strip()
        success = proc.returncode == 0

        return {
            "command": f"dbt {command}",
            "success": success,
            "exit_code": proc.returncode,
            "duration_seconds": duration,
            "output": output,
        }

    def run_medallion_pipeline(
        self,
        postgres_params: Dict[str, Any],
        table_names: Optional[List[str]] = None,
        selector: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Runs the pipeline end to end:
        1. Copies the landing layer's Parquet files into that layer's warehouse schema
        2. Runs the selected dbt models, each into the schema its layer resolves to
        3. Runs the dbt tests
        4. Registers the materialised models in the catalog
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
            "DBT_PG_HOST": str(host),
            "DBT_PG_PORT": str(port),
            "DBT_PG_USER": str(user),
            "DBT_PG_PASSWORD": str(postgres_params["password"]),
            "DBT_PG_DBNAME": str(postgres_params["database"]),
            "DBT_PG_SSLMODE": str(postgres_params.get("sslmode", "disable")),
        }

        dbt_vars = self._dbt_vars_payload()
        staging_layer = dbt_vars["layer_staging"]
        staging_schema = dbt_vars["layer_schemas"].get(staging_layer, staging_layer)

        # Step 1: copy the landing layer into its schema
        logs.append(
            f"[{time_str}] Step 1/3: Copying layer '{staging_layer}' into warehouse "
            f"schema '{staging_schema}'..."
        )
        try:
            sync_res = self.sync_layer_to_postgres(
                postgres_params=postgres_params,
                table_names=table_names,
                layer_path=staging_layer,
            )
            tbl_summary = ", ".join([f"{t['table']} ({t['rows']} rows)" for t in sync_res.get("tables", [])])
            logs.append(f"[{time_str}] Synced {sync_res['synced_tables']} tables to warehouse: {tbl_summary}")
        except Exception as e:
            err_msg = (
                f"Failed to copy layer '{staging_layer}' into the warehouse at "
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
        target_layers = ", ".join(
            f"'{self._dbt_layer_vars()[slot]}'" for slot in ("staging", "marts")
        )
        logs.append(
            f"\nStep 2/3: Executing dbt run{select_display} "
            f"(writing to layers {target_layers})..."
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

        # Step 4: register the materialised models in the catalog
        self._catalog_dbt_models()

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

    def _discover_layer_parquet(self, location: Optional[LayerLocation] = None) -> List[Path]:
        """Finds the Parquet files that belong to one layer.

        The layer's own directory is authoritative, but the lakehouse root is scanned
        too: installs that predate layers wrote their tables straight there. On a stem
        collision the layer directory wins.
        """
        location = location or resolve_layer(default_layer_path())
        found: Dict[str, Path] = {}
        for directory in (location.filesystem_dir, settings.lakehouse_root_path):
            if not directory.is_dir():
                continue
            for parquet in sorted(directory.glob("*.parquet")):
                found.setdefault(parquet.stem, parquet)
        return list(found.values())

    def _dbt_model_descriptions(self) -> Dict[str, str]:
        """Reads model descriptions from models/schema.yml (best effort)."""
        schema_file = self.dbt_project_dir / "models" / "schema.yml"
        if not schema_file.exists():
            return {}
        try:
            import yaml
            document = yaml.safe_load(schema_file.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}
        return {
            entry["name"]: entry.get("description", "")
            for entry in (document.get("models") or [])
            if isinstance(entry, dict) and entry.get("name")
        }

    def _catalog_dbt_models(self) -> None:
        """Registers every dbt model found on disk in the SQLite catalog.

        A model's layer is whatever its folder is configured to write to, resolved the
        same way dbt resolves it, so the catalog and the warehouse cannot disagree
        about where a model landed.
        """
        db_path = settings.sqlite_db_path
        now_iso = datetime.now(timezone.utc).isoformat()
        descriptions = self._dbt_model_descriptions()

        models: List[Dict[str, str]] = []
        for folder, layer_path in self._dbt_layer_vars().items():
            models_dir = self.dbt_project_dir / "models" / folder
            if not models_dir.exists():
                continue
            try:
                schema = resolve_layer(layer_path).warehouse_schema
            except InvalidLayerPath:
                schema = layer_path
            for sql_file in sorted(models_dir.glob("*.sql")):
                models.append(
                    {"name": sql_file.stem, "layer": layer_path, "schema": schema}
                )

        try:
            with sqlite3.connect(db_path, timeout=10.0) as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL;")
                for m in models:
                    asset_id = str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO assets (id, name, layer, storage_uri, format, record_count, schema_metadata, created_at, last_materialized_at)
                        VALUES (?, ?, ?, ?, 'postgres_table', 0, ?, ?, ?)
                        ON CONFLICT(layer, name) DO UPDATE SET
                            storage_uri=excluded.storage_uri,
                            format=excluded.format,
                            schema_metadata=excluded.schema_metadata,
                            last_materialized_at=excluded.last_materialized_at;
                        """,
                        (
                            asset_id,
                            m["name"],
                            m["layer"],
                            f"postgresql:///{m['schema']}.{m['name']}",
                            json.dumps({"description": descriptions.get(m["name"], "")}),
                            now_iso,
                            now_iso,
                        ),
                    )
                conn.commit()
        except Exception:
            pass

    def _folder_for_layer(self, layer: str) -> str:
        """Maps a layer path onto the project folder configured to write to it."""
        wanted = (layer or "").strip()
        targets = self._dbt_layer_vars()
        for folder in ("marts", "staging"):
            if wanted == targets[folder]:
                return folder
        return "marts"

    def get_model_code(self, layer: str, name: str) -> Optional[str]:
        """Reads SQL source code of a dbt model from git_repo/dbt_project/models/."""
        target_dir = self._folder_for_layer(layer)
        file_path = self.dbt_project_dir / "models" / target_dir / f"{name}.sql"
        if file_path.exists():
            return file_path.read_text(encoding="utf-8")
        return None

    def save_model_code(self, layer: str, name: str, sql: str) -> Dict[str, Any]:
        """Saves or creates a dbt SQL transformation model on disk."""
        clean_name = name.lower().strip().replace(".sql", "")
        target_dir = self._folder_for_layer(layer)
        dir_path = self.dbt_project_dir / "models" / target_dir
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{clean_name}.sql"
        file_path.write_text(sql, encoding="utf-8")
        return {
            "status": "success",
            "name": clean_name,
            "layer": layer.lower(),
            "path": file_path.as_posix(),
        }

    def get_runs_history(self) -> List[Dict[str, Any]]:
        # After a restart the in-memory list is empty; the table is the record.
        if not self._runs_history:
            self._runs_history.extend(reversed(task_history.recent("dbt", limit=100)))
        return list(reversed(self._runs_history))

    def get_asset_graph(self, selector: Optional[str] = None) -> Dict[str, Any]:
        """Builds the lineage graph from what is on disk, keyed by layer path.

        Node ids are ``<layer>.<model>`` using the layer each folder is configured to
        target, so a graph node always points at the schema the model actually landed
        in — whatever the workspace decided to call its layers.
        """
        import re

        targets = self._dbt_layer_vars()
        staging_layer = targets["staging"]
        marts_layer = targets["marts"]

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        node_map: Dict[str, str] = {}

        try:
            landing = resolve_layer(staging_layer)
        except InvalidLayerPath:
            landing = None

        for f in self._discover_layer_parquet(landing):
            node_id = f"{staging_layer}.{f.stem}"
            node_map.setdefault(f.stem, node_id)
            nodes.append({
                "id": node_id,
                "name": f.stem,
                "layer": staging_layer,
                "type": "parquet",
                "path": f.as_posix(),
            })

        # 1 & 2. Scan the project's model folders, in dependency order
        for folder, layer_path, node_type in (
            ("staging", staging_layer, "dbt_model"),
            ("marts", marts_layer, "dbt_mart"),
        ):
            models_dir = self.dbt_project_dir / "models" / folder
            if not models_dir.exists():
                continue
            for sql_file in sorted(models_dir.glob("*.sql")):
                model_name = sql_file.stem
                node_id = f"{layer_path}.{model_name}"
                node_map[model_name] = node_id
                code = sql_file.read_text(encoding="utf-8")
                nodes.append({
                    "id": node_id,
                    "name": model_name,
                    "layer": layer_path,
                    "folder": folder,
                    "type": node_type,
                    "path": sql_file.as_posix(),
                    "sql_code": code,
                })
                # source('<name>', '<table>') always reads the landing layer.
                for src_match in re.finditer(
                    r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", code
                ):
                    table = src_match.group(1)
                    edges.append({
                        "source": node_map.get(table, f"{staging_layer}.{table}"),
                        "target": node_id,
                    })
                for ref_match in re.finditer(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", code):
                    ref_name = ref_match.group(1)
                    edges.append({
                        "source": node_map.get(ref_name, f"{staging_layer}.{ref_name}"),
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

        layers = {n["layer"] for n in all_nodes if n.get("layer")}
        if custom_dag_nodes:
            layers.add("custom")
        layers = sorted(layers)

        return {
            "nodes": all_nodes,
            "edges": unique_edges,
            "layers": layers,
            "active_pipeline_selector": selector or "*",
            "active_node_count": len([n for n in all_nodes if n.get("is_in_pipeline", False)]),
        }
    def _resolve_postgres_params(self, custom_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Resolves target PostgreSQL warehouse connection parameters."""
        return resolve_warehouse_postgres_params(custom_params)

    def _get_target_env(self, target: str = "postgres") -> Dict[str, str]:
        """Prepares environment variables for dbt execution matching the active target adapter."""
        clean_target = (target or "postgres").strip().lower()
        if clean_target == "dev":
            clean_target = "postgres"

        env: Dict[str, str] = {
            "DBT_TARGET": clean_target,
        }

        if clean_target == "postgres":
            pg = self._resolve_postgres_params()
            resolved_host = resolve_connection_host(pg["host"], int(pg["port"]))
            env.update({
                "DBT_PG_HOST": str(resolved_host),
                "DBT_PG_PORT": str(pg["port"]),
                "DBT_PG_USER": str(pg.get("username") or pg.get("user") or "testuser"),
                "DBT_PG_PASSWORD": str(pg.get("password") or "testpass"),
                "DBT_PG_DBNAME": str(pg.get("database") or "sourcedb"),
                "DBT_PG_SSLMODE": str(pg.get("sslmode", "disable")),
            })
        elif clean_target == "duckdb":
            duck_path = (settings.lakehouse_root_path / "lakehouse.duckdb").resolve().as_posix()
            env["DBT_DUCKDB_PATH"] = duck_path

        return env

    def run_scoped_dbt(
        self,
        command: str = "run",
        selector: Optional[str] = None,
        target: str = "postgres",
    ) -> Dict[str, Any]:
        """
        Executes dbt <command> --select <selector> --target <target>.
        Captures exit codes, durations, and clean terminal logs.
        """
        clean_target = (target or "postgres").strip().lower()
        if clean_target == "dev":
            clean_target = "postgres"

        dbt_vars = self._dbt_vars_payload()
        staging_layer = dbt_vars["layer_staging"]

        # A layer with no data yet gets sample tables, so the shipped models have
        # something to read. Never when the operator already has data.
        if not self._discover_layer_parquet(resolve_layer(staging_layer)):
            self._ensure_core_layer_tables(resolve_layer(staging_layer))

        if clean_target == "postgres" and command == "run":
            pg_params = self._resolve_postgres_params()
            try:
                self.sync_layer_to_postgres(pg_params, layer_path=staging_layer)
            except Exception:
                pass

        env_vars = self._get_target_env(clean_target)
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

        # If run was successful on postgres, update SQLite catalog assets
        if command == "run" and success and clean_target == "postgres":
            try:
                self._catalog_dbt_models()
            except Exception:
                pass

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

    def compile_model(self, model_name: str, target: str = "postgres") -> Dict[str, Any]:
        """
        Runs dbt compile --select <model_name> --target <target> via sys.executable -m dbt.cli.main.
        Reads the generated compiled SQL from target/compiled/obelisk_dbt/models/.../<model_name>.sql and returns it.
        """
        clean_name = model_name.strip().replace(".sql", "")
        # Run dbt compile
        res = self.run_scoped_dbt(command="compile", selector=clean_name, target=target)

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
    ) -> Dict[str, Any]:
        """
        Compiles the model, wraps the compiled SQL with SELECT * FROM (...) AS _subq LIMIT 50,
        and executes it via NativeQueryEngine or DuckDBEngine to return column headers and data rows as JSON.
        """
        clean_name = model_name.strip().replace(".sql", "")
        comp_res = self.compile_model(model_name=clean_name, target=target)
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

        clean_target = (target or "postgres").strip().lower()
        if clean_target in ("postgres", "dev"):
            pg_params = self._resolve_postgres_params()
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
                layer = None
                if "models/staging" in rel_path or "models\\staging" in rel_path:
                    layer = "staging"
                elif "models/marts" in rel_path or "models\\marts" in rel_path:
                    layer = "marts"

                return {
                    "name": path.name,
                    "path": rel_path,
                    "type": "file",
                    "extension": ext,
                    "layer": layer,
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
                        "{{ config(materialized='table', schema=var('layer_staging')) }}\n\n"
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
                        "{{ config(materialized='table', schema=var('layer_marts')) }}\n\n"
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

        targets = self._dbt_layer_vars()
        staging_layer = targets["staging"]
        marts_layer = targets["marts"]

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
                    schema = target_node.get("config", {}).get("schema") or (
                        staging_layer if "staging" in target_node.get("original_file_path", "") else marts_layer
                    )

                    # Current Node
                    nodes.append({
                        "id": matching_id,
                        "name": clean_name,
                        "type": "model",
                        "layer": schema,
                        "is_current": True,
                        "materialized": target_node.get("config", {}).get("materialized", "table"),
                        "path": target_node.get("original_file_path", ""),
                    })

                    # Upstream Parents
                    upstreams = parent_map.get(matching_id, [])
                    for up_id in upstreams:
                        if up_id.startswith("macro."):
                            continue
                        up_name = up_id.split(".")[-1]
                        is_source = up_id.startswith("source.")
                        up_layer = staging_layer if is_source or "stg_" in up_name else marts_layer
                        nodes.append({
                            "id": up_id,
                            "name": up_name,
                            "type": "source" if is_source else "model",
                            "layer": up_layer,
                            "is_current": False,
                        })
                        edges.append({"source": up_id, "target": matching_id})

                    # Downstream Children
                    downstreams = child_map.get(matching_id, [])
                    for down_id in downstreams:
                        down_name = down_id.split(".")[-1]
                        is_test = down_id.startswith("test.")
                        down_layer = marts_layer if ("fct_" in down_name or "dim_" in down_name) else staging_layer
                        nodes.append({
                            "id": down_id,
                            "name": down_name if not is_test else down_id.split(".")[2],
                            "type": "test" if is_test else "model",
                            "layer": down_layer if not is_test else "test",
                            "is_current": False,
                        })
                        edges.append({"source": matching_id, "target": down_id})
            except Exception:
                pass

        if not found_in_manifest:
            code = self.get_model_code(staging_layer, clean_name) or self.get_model_code(marts_layer, clean_name)
            current_layer = staging_layer if self.get_model_code(staging_layer, clean_name) else marts_layer
            nodes.append({
                "id": target_node_id,
                "name": clean_name,
                "type": "model",
                "layer": current_layer,
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
                        "layer": staging_layer,
                        "is_current": False,
                    })
                    edges.append({"source": s_id, "target": target_node_id})

                for ref_match in re.finditer(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", code):
                    r_name = ref_match.group(1)
                    r_id = f"model.obelisk_dbt.{r_name}"
                    r_layer = staging_layer if "stg_" in r_name else marts_layer
                    nodes.append({
                        "id": r_id,
                        "name": r_name,
                        "type": "model",
                        "layer": r_layer,
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
                        other_layer = staging_layer if "staging" in str(other_file) else marts_layer
                        nodes.append({
                            "id": other_id,
                            "name": other_file.stem,
                            "type": "model",
                            "layer": other_layer,
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
