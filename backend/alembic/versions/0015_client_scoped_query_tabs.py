"""Give Query Studio tabs an owner, so two windows stop overwriting each other.

Every browser window numbers its tabs from ``tab-1``, and the tab id was the primary
key, so the second window to save won and the first window's SQL came back replaced.
The key becomes ``<client>:<tab>``: the API still speaks unprefixed ids, the prefix is
added on the way in and stripped on the way out, so nothing above this layer changes.
Rows that predate the migration are re-keyed under ``default`` instead of being
orphaned — they are what the single global client was looking at.
"""
from alembic import op
import sqlalchemy as sa

revision = "0015_client_scoped_query_tabs"
down_revision = "0014_attachable_drivers"
branch_labels = None
depends_on = None

_DEFAULT_CLIENT = "default"


def upgrade() -> None:
    # batch_alter_table because SQLite cannot add a NOT NULL column with a default in
    # place; the same form is used in 0013 for this reason.
    with op.batch_alter_table("open_query_tabs") as batch:
        batch.add_column(
            sa.Column("client_id", sa.String(length=64), nullable=False, server_default="default")
        )
    op.create_index("ix_open_query_tabs_client_id", "open_query_tabs", ["client_id"])
    # Existing rows belong to whoever was using the single global namespace.
    op.execute(
        "UPDATE open_query_tabs SET id = '" + _DEFAULT_CLIENT + ":' || id "
        "WHERE id NOT LIKE '" + _DEFAULT_CLIENT + ":%'"
    )


def downgrade() -> None:
    # A global namespace has room for one owner. Keeping the default client's tabs
    # (and re-keying them below) is what the pre-0015 table held; other clients' tabs
    # would collide with them on id, so they are dropped rather than silently merged.
    op.execute("DELETE FROM open_query_tabs WHERE client_id <> '" + _DEFAULT_CLIENT + "'")
    op.execute(
        "UPDATE open_query_tabs SET id = substr(id, " + str(len(_DEFAULT_CLIENT) + 2) + ") "
        "WHERE id LIKE '" + _DEFAULT_CLIENT + ":%'"
    )
    op.drop_index("ix_open_query_tabs_client_id", table_name="open_query_tabs")
    with op.batch_alter_table("open_query_tabs") as batch:
        batch.drop_column("client_id")