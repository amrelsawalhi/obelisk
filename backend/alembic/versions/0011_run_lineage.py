"""A run can be started by another pipeline's step.

Revision ID: 0011_run_lineage
Revises: 0010_step_outputs
Create Date: 2026-10-24

The `pipeline` step type runs another pipeline. Without lineage the child's run looks
like a manual trigger, which makes "why did this run?" unanswerable and cancel
propagation impossible to follow. Two nullable columns are enough: the run that
started this one, and the step run inside it.
"""
import sqlalchemy as sa
from alembic import op

revision = "0011_run_lineage"
down_revision = "0010_step_outputs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pipeline_runs", sa.Column("parent_run_id", sa.String(36), nullable=True))
    op.add_column(
        "pipeline_runs", sa.Column("parent_step_run_id", sa.String(36), nullable=True)
    )
    op.create_index(
        "ix_pipeline_runs_parent_run_id", "pipeline_runs", ["parent_run_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_pipeline_runs_parent_run_id", table_name="pipeline_runs")
    with op.batch_alter_table("pipeline_runs") as batch:
        batch.drop_column("parent_step_run_id")
        batch.drop_column("parent_run_id")
