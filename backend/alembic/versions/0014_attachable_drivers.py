"""Re-point MySQL and MongoDB connections at the scanners that can read them live.

Migration ``0003`` inferred ``sdk`` for these types because no attach path existed, and
that value was persisted. DuckDB now has a scanner for both (``mysql`` in core, ``mongo``
in its community repository), so the stored driver is the stale half of the old decision:
without this, every existing MySQL/Mongo connection stays pull-only while a newly created
one attaches. Rows an operator deliberately pinned back to ``sdk`` are indistinguishable,
which is why the downgrade is recorded here rather than guessed at runtime.
"""
from alembic import op

revision = "0014_attachable_drivers"
down_revision = "0013_app_settings"
branch_labels = None
depends_on = None

_ATTACHABLE = "('mysql', 'mariadb', 'mongodb', 'mongo')"


def upgrade() -> None:
    op.execute(
        "UPDATE connections SET driver = 'attach' "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'sdk'"
    )
    op.execute(
        "UPDATE connections SET capabilities_json = "
        "'[\"sql\", \"pushdown\", \"live\", \"preview\", \"federated\"]' "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'attach'"
    )
    # ``read_only`` was written ``0`` for a pulled source; an attached one is read-only in
    # SQL by construction (the platform has no write path into MySQL or Mongo), so keeping
    # the old value would put a WRITABLE pill on a source that cannot be written.
    op.execute(
        "UPDATE connections SET read_only = 1 "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'attach'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE connections SET driver = 'sdk' "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'attach'"
    )
    op.execute(
        "UPDATE connections SET capabilities_json = '[\"sql\", \"pull\", \"preview\"]' "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'sdk'"
    )
    op.execute(
        "UPDATE connections SET read_only = 0 "
        f"WHERE lower(source_type) IN {_ATTACHABLE} AND driver = 'sdk'"
    )
