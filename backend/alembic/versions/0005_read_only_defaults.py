"""Correct read-only defaults per driver (Phase 1b).

Revision ID: 0005_read_only_defaults
Revises: 0004_task_history
Create Date: 2026-10-24

0003 added ``read_only`` with ``DEFAULT 1``, so every existing row — including
MongoDB/MySQL/HTTP sources — came back read-only even though nobody chose that. Only
an *attached* source is structurally read-only: it is queried live through DuckDB and
Obelisk never writes back. A pulled source may be written into the lakehouse, so it
defaults to writable.

0003 is left untouched on purpose: it has already run on installations, and editing
an applied revision is how histories diverge.
"""
from alembic import op

revision = "0005_read_only_defaults"
down_revision = "0004_task_history"
branch_labels = None
depends_on = None

_PULL_TYPES = (
    "mysql",
    "mariadb",
    "mongodb",
    "mongo",
    "oracle",
    "sqlserver",
    "mssql",
    "api",
    "rest",
    "rest_api",
    "erp",
)


def upgrade() -> None:
    quoted = ", ".join(f"'{value}'" for value in _PULL_TYPES)
    op.execute(
        "UPDATE connections SET read_only = 0 "
        "WHERE lower(COALESCE(driver, '')) IN ('sdk', 'http') "
        f"   OR (driver IS NULL AND lower(source_type) IN ({quoted}))"
    )


def downgrade() -> None:
    # Nothing to restore: the previous values were an artefact of the column default,
    # not a decision anyone made.
    pass
