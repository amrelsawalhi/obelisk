import os
from pathlib import Path
from typing import Dict, Any, Optional
import dagster as dg
from app.config import settings

def create_ingest_asset(
    asset_name: str,
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
):
    """
    Asset factory for DuckDB and dlt ingestion assets streaming raw Parquet to STORAGE_URI.
    """
    @dg.asset(name=asset_name, group_name="bronze_ingestion")
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        from app.engines.ingest_engine import ingest_engine
        context.log.info(f"Starting Dagster ingestion for entity '{entity}' from '{source_type}' (mode={mode})")

        res = ingest_engine.ingest_entity(
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

# Auto-discover all custom Dagster assets defined in git_repo/dags/*.py
custom_dag_assets = dag_loader.load_dagster_assets()

# Repository definitions containing auto-discovered DAGs
defs = dg.Definitions(
    assets=[*custom_dag_assets],
)
