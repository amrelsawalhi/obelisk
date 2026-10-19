"""
Obelisk Custom DAG Asset: test_dag (Notebook Runner)
"""

import sys
import subprocess
from pathlib import Path
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue


@asset(
    name="test_dag",
    group_name="notebooks",
    description="Custom pipeline asset 'test_dag'",
)
def test_dag(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Triggering Marimo notebook Quickstart_Analytics.py...")
    root_dir = Path(__file__).resolve().parent.parent.parent
    nb_path = root_dir / "git_repo" / "notebooks" / "Quickstart_Analytics.py"

    if not nb_path.exists():
        context.log.warning(f"Notebook {nb_path} not found.")
        return MaterializeResult(metadata={"status": MetadataValue.text("NOT_FOUND")})

    result = subprocess.run([sys.executable, str(nb_path)], cwd=str(root_dir), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Notebook execution failed: {result.stderr}")

    context.log.info("Notebook executed successfully.")
    return MaterializeResult(metadata={"notebook": MetadataValue.text("Quickstart_Analytics.py"), "status": MetadataValue.text("SUCCESS")})
