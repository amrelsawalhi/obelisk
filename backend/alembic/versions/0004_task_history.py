"""Durable task history (Phase 1b, M3/H10).

Revision ID: 0004_task_history
Revises: 0003_connection_drivers
Create Date: 2026-10-24

Ingest jobs, dbt runs and deploys were kept only in process memory, so every
restart erased the run history and left anything mid-flight looking perpetually
"running". One table covers all three producers: same access pattern, no
relationships, discriminated by ``kind``.

``payload_json`` keeps the producer's own record shape, so adding a field to a job
needs no migration — these are display records, not relational facts.
"""
from alembic import op

revision = "0004_task_history"
down_revision = "0003_connection_drivers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS task_history (
            kind TEXT NOT NULL,
            id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (kind, id)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_task_history_kind_started "
        "ON task_history (kind, started_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS task_history")
