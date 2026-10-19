"""
Obelisk Custom DAG Asset: Customer Churn Notebook Runner
Demonstrates triggering a reactive Marimo notebook headlessly as a scheduled pipeline step ().
"""

import sys
import subprocess
from pathlib import Path
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue


@asset(
    name="customer_churn_notebook",
    group_name="data_science",
    description="Triggers the Marimo Quickstart Analytics notebook headlessly to generate exploratory charts.",
)
def customer_churn_notebook(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Triggering headless Marimo notebook execution...")

    # Locate notebook relative to workspace root
    root_dir = Path(__file__).resolve().parent.parent.parent
    nb_path = root_dir / "git_repo" / "notebooks" / "Quickstart_Analytics.py"

    if not nb_path.exists():
        context.log.warning(f"Notebook {nb_path} not found. Running synthetic verification.")
        return MaterializeResult(
            metadata={
                "notebook_path": MetadataValue.path(str(nb_path)),
                "status": MetadataValue.text("SKIPPED_NOT_FOUND"),
            }
        )

    context.log.info(f"Executing notebook: {nb_path}")
    result = subprocess.run(
        [sys.executable, str(nb_path)],
        cwd=str(root_dir),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        context.log.error(f"Notebook execution failed: {result.stderr}")
        raise RuntimeError(f"Notebook exited with code {result.returncode}: {result.stderr}")

    context.log.info("Notebook executed successfully.")
    return MaterializeResult(
        metadata={
            "notebook_name": MetadataValue.text("Quickstart_Analytics.py"),
            "status": MetadataValue.text("SUCCESS"),
            "stdout_summary": MetadataValue.text(result.stdout[:200] if result.stdout else "OK"),
        }
    )
