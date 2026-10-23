"""Connection driver contract (Phase 1b).

Revision ID: 0003_connection_drivers
Revises: 0002_catalog_and_layers
Create Date: 2026-10-24

Turns "how do we reach this source?" from a string comparison repeated in three
modules into a stored property plus one resolver (``app.core.sources``).

``driver`` is nullable on purpose: NULL means "infer from source_type", so any row
written by a code path that predates this column still resolves correctly. Rows
that exist today are backfilled so the stored value and the inference agree.
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_connection_drivers"
down_revision = "0002_catalog_and_layers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("connections")}

    if "driver" not in columns:
        op.add_column("connections", sa.Column("driver", sa.String(32), nullable=True))
    if "capabilities_json" not in columns:
        op.add_column("connections", sa.Column("capabilities_json", sa.JSON(), nullable=True))
    if "read_only" not in columns:
        op.add_column(
            "connections",
            sa.Column("read_only", sa.Boolean(), nullable=False, server_default="1"),
        )

    # Backfill: postgres can be attached live, client-library sources must be
    # pulled, HTTP sources are pulled a request at a time.
    op.execute(
        "UPDATE connections SET driver = 'attach', read_only = 1 "
        "WHERE driver IS NULL AND lower(source_type) IN ('postgres', 'postgresql')"
    )
    op.execute(
        "UPDATE connections SET driver = 'sdk' WHERE driver IS NULL AND "
        "lower(source_type) IN ('mysql', 'mariadb', 'mongodb', 'mongo', 'oracle', "
        "'sqlserver', 'mssql')"
    )
    op.execute(
        "UPDATE connections SET driver = 'http' WHERE driver IS NULL AND "
        "lower(source_type) IN ('api', 'rest', 'rest_api', 'erp')"
    )

    if "ix_connections_driver" not in {i["name"] for i in inspector.get_indexes("connections")}:
        op.create_index("ix_connections_driver", "connections", ["driver"])


def downgrade() -> None:
    with op.batch_alter_table("connections") as batch:
        batch.drop_index("ix_connections_driver")
        batch.drop_column("read_only")
        batch.drop_column("capabilities_json")
        batch.drop_column("driver")
