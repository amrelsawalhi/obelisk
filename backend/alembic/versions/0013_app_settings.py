"""Add ``app_settings`` and the service link on connections.

Two changes that belong together: a service that provides a database now owns its
Connection row (``connections.service_name``), and the platform keeps the one decision
it used to guess — which connection is the warehouse — in ``app_settings``.
"""
from alembic import op
import sqlalchemy as sa

revision = "0013_app_settings"
down_revision = "0012_no_layers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    with op.batch_alter_table("connections") as batch:
        batch.add_column(sa.Column("service_name", sa.String(50), nullable=True))
    op.create_index(
        "ix_connections_service_name", "connections", ["service_name"], unique=True
    )


def downgrade() -> None:
    op.drop_index("ix_connections_service_name", table_name="connections")
    with op.batch_alter_table("connections") as batch:
        batch.drop_column("service_name")
    op.drop_table("app_settings")
