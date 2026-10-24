"""Step outputs and the pipeline's durable output state.

Revision ID: 0010_step_outputs
Revises: 0009_pipelines
Create Date: 2026-10-24

A step can publish a small, declared result — a scalar (a watermark, a row count),
the name of a table it materialised, or a dbt summary. Children read those through
edge-scoped templates, never by inspecting another process.

Two places are needed:

* ``step_runs.output_json`` — what this attempt produced, so ``rerun-from-step``
  replays the parents of the same run.
* ``pipeline_state`` — the last value per ``(pipeline, step.output)``. Run history is
  pruned (``keep_runs``), and a value that disappears with the run it came from
  cannot be replayed; this table is never pruned and is the fallback when a run no
  longer contains the parent.

Both are display-scale data: values are capped at 4 KB by the engine, and `table`
outputs carry a name, not rows — the rows live in the lakehouse.
"""
import sqlalchemy as sa
from alembic import op

revision = "0010_step_outputs"
down_revision = "0009_pipelines"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("step_runs", sa.Column("output_json", sa.Text(), nullable=True))

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_id VARCHAR(36) NOT NULL,
            key VARCHAR(160) NOT NULL,
            value_json TEXT NOT NULL,
            updated_at DATETIME NOT NULL,
            updated_by_run VARCHAR(36),
            PRIMARY KEY (pipeline_id, key),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines (id) ON DELETE CASCADE
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pipeline_state")
    with op.batch_alter_table("step_runs") as batch:
        batch.drop_column("output_json")
