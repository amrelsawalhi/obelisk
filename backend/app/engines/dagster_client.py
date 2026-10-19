import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

# Ensure project root is in sys.path
_project_root = str(Path(__file__).resolve().parents[3])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import dagster as dg
from dagster_workspace.repository import create_ingest_asset

class DagsterClient:
    """
    Bridge to Dagster Engine:
    Executes asset materialization pipelines and captures execution event logs.
    """

    def trigger_ingest(
        self,
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
        """
        Triggers a Dagster asset materialization run for the given source and entity.
        Materializes asset into Snappy Parquet in Bronze layer.
        """
        clean_stem = (target_name or (entity.split(".", 1)[-1] if "." in entity else entity))
        clean_stem = clean_stem.replace(".", "_").replace("/", "_").replace("\\", "_")
        asset_name = f"bronze_{clean_stem}"

        ingest_asset = create_ingest_asset(
            asset_name=asset_name,
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

        # Run materialize in-process
        result: dg.ExecuteInProcessResult = dg.materialize([ingest_asset])

        logs: List[str] = []
        for event in result.all_events:
            if event.message:
                logs.append(f"[{event.event_type_value}] {event.message}")

        mat_events = [e for e in result.all_events if e.is_step_materialization]
        metadata_dict = {}
        if mat_events:
            mat = mat_events[0].event_specific_data.materialization
            for k, val in mat.metadata.items():
                metadata_dict[k] = val.value

        return {
            "success": result.success,
            "run_id": result.run_id,
            "asset_name": asset_name,
            "metadata": metadata_dict,
            "logs": logs,
        }

dagster_client = DagsterClient()
