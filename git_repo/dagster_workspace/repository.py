import os
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional
import dagster as dg

import sys

# Bootstrap sys.path: ensure `git_repo/` is on sys.path so generated DAG
# files (which do `from dagster_workspace.repository import create_ingest_asset`)
# can resolve this package, regardless of how Dagster`s `python_file:` loader
# sets up sys.path.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_GIT_REPO_DIR = os.path.dirname(_THIS_DIR)
if _GIT_REPO_DIR and _GIT_REPO_DIR not in sys.path:
    sys.path.insert(0, _GIT_REPO_DIR)

from app.config import settings


def resolve_connection_params_by_id(connection_id: str) -> Dict[str, Any]:
    """
    Fetches a registered connection from the Obelisk SQLite brain and decrypts its
    credentials at runtime via the Fernet vault. This lets auto-generated Dagster
    DAG files persist only the connection ID — secrets never touch disk.
    """
    db_path = settings.DATABASE_URL.replace("sqlite+aiosqlite:///", "")
    con = sqlite3.connect(db_path, timeout=10.0)
    try:
        row = con.execute(
            """
            SELECT source_type, host, port, database, username, encrypted_credentials, options
            FROM connections WHERE id = ?
            """,
            (connection_id,),
        ).fetchone()
    finally:
        con.close()

    if not row:
        raise RuntimeError(f"Connection '{connection_id}' not found in Obelisk registry")

    source_type, host, port, database, username, encrypted_credentials, options = row
    from app.core.vault import decrypt
    password = decrypt(encrypted_credentials) or ""

    params: Dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
    }
    if options:
        try:
            opts = json.loads(options) if isinstance(options, str) else options
            if isinstance(opts, dict):
                params.update(opts)
        except (json.JSONDecodeError, TypeError):
            pass
    return params


def create_ingest_asset(
    asset_name: str,
    source_type: str,
    entity: str,
    mode: str = "snapshot",
    watermark_column: Optional[str] = None,
    target_name: Optional[str] = None,
    target_folder: Optional[str] = None,
    limit: Optional[int] = None,
    write_strategy: str = "append",
    primary_key: Optional[str] = None,
    connection_params: Optional[Dict[str, Any]] = None,
    connection_id: Optional[str] = None,
):
    """
    Asset factory for DuckDB and dlt ingestion assets streaming raw Parquet to STORAGE_URI.

    Credentials are never embedded in generated DAG files: pass `connection_id` and
    the plaintext password is decrypted at runtime from the Obelisk Vault.
    `connection_params` (containing a plaintext password) is still accepted for
    direct API-triggered materializations that are never persisted to disk.
    """
    @dg.asset(name=asset_name, group_name="ingestion")
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        from app.engines.ingest_engine import ingest_engine

        params = connection_params
        if params is None and connection_id:
            params = resolve_connection_params_by_id(connection_id)
        if not params:
            raise RuntimeError(
                f"Ingestion asset '{asset_name}' has no connection parameters "
                f"(connection_id={connection_id!r})."
            )

        context.log.info(f"Starting Dagster ingestion for entity '{entity}' from '{source_type}' (mode={mode})")

        res = ingest_engine.ingest_entity(
            source_type=source_type,
            connection_params=params,
            entity=entity,
            mode=mode,
            watermark_column=watermark_column,
            target_name=target_name,
            target_folder=target_folder,
            limit=limit,
            write_strategy=write_strategy,
            primary_key=primary_key,
        )

        context.log.info(f"Ingestion completed. Wrote {res['num_rows']} rows to {res['file_path']}")
        return dg.MaterializeResult(
            metadata={
                "num_rows": dg.MetadataValue.int(res["num_rows"]),
                "file_path": dg.MetadataValue.path(res["file_path"]),
                "mode": dg.MetadataValue.text(mode),
                "source_type": dg.MetadataValue.text(source_type),
            }
        )

    return _asset

from app.engines.dag_loader import dag_loader

# Auto-discover all custom Dagster assets, jobs, and schedules in git_repo/dags/*.py
_custom_dags = dag_loader.load_dagster_definitions()
custom_dag_assets = _custom_dags["assets"]

# Repository definitions containing auto-discovered DAGs
defs = dg.Definitions(
    assets=[*custom_dag_assets],
    jobs=[*_custom_dags["jobs"]],
    schedules=[*_custom_dags["schedules"]],
)
