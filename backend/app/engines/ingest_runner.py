"""In-process ingestion runner.

Ingestion used to be dispatched through Dagster: a generated asset file, a GraphQL
`launchRun`, and a materialize fallback when the service was down. With Dagster
removed, the fallback *is* the implementation — `ingest_engine.ingest_entity`,
which was always the code that actually moved rows.

The return shape is kept identical to the old client's so the ingest routes and the
SDK need no rewriting: ``success``, ``run_id``, ``asset_name``, ``metadata``,
``logs``.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.engines.ingest_engine import ingest_engine


def _clean_stem(target_name: Optional[str], entity: str) -> str:
    stem = target_name or (entity.split(".", 1)[-1] if "." in entity else entity)
    return stem.replace(".", "_").replace("/", "_").replace("\\", "_")


def trigger_ingest(
    source_type: str,
    connection_params: Dict[str, Any],
    entity: str,
    mode: str = "snapshot",
    watermark_column: Optional[str] = None,
    target_name: Optional[str] = None,
    target_folder: Optional[str] = None,
    limit: Optional[int] = None,
    write_strategy: str = "append",
    primary_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Extracts one entity into the lakehouse and reports what happened."""
    asset_name = _clean_stem(target_name, entity)
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logs = [
        f"[{started}] Ingest '{entity}' from {source_type} (mode={mode})",
        f"Target asset: {asset_name}",
    ]
    try:
        result = ingest_engine.ingest_entity(
            source_type=source_type,
            connection_params=connection_params,
            entity=entity,
            mode=mode,
            watermark_column=watermark_column,
            target_name=target_name,
            target_folder=target_folder,
            limit=limit,
            write_strategy=write_strategy,
            primary_key=primary_key,
        )
    except Exception as exc:
        logs.append(f"[error] {exc.__class__.__name__}: {exc}")
        return {
            "success": False,
            "run_id": run_id,
            "asset_name": asset_name,
            "metadata": {},
            "logs": logs,
        }

    logs.append(
        f"Ingested {result.get('num_rows', 0)} row(s) to {result.get('file_path', '(unknown)')} "
        f"(extracted {result.get('new_rows_extracted', result.get('num_rows', 0))})"
    )
    return {
        "success": True,
        "run_id": run_id,
        "asset_name": asset_name,
        "metadata": {
            "num_rows": result.get("num_rows"),
            "file_path": result.get("file_path"),
            "table_name": result.get("table_name"),
        },
        "logs": logs,
    }
