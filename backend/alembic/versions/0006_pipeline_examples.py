"""Rename the seeded pipeline examples.

The five seeded definitions were named after medallion tiers ("All Medallion
Assets", "Gold Marts Only"). The seeds now use neutral names, but an installation
that already has the rows keeps the old ones — seeding only runs on an empty table.

Each update matches the exact name *and* selector that was seeded, so a definition
the operator renamed or retargeted is left alone.
"""
from alembic import op
from sqlalchemy import text

revision = "0006_pipeline_examples"
down_revision = "0005_read_only_defaults"
branch_labels = None
depends_on = None

# (old name, selector, new name, new description)
_RENAMES = [
    (
        "All Medallion Assets",
        "*",
        "Everything",
        "Runs every model in the project",
    ),
    (
        "Orders & Revenue Flow",
        "stg_orders+",
        "Orders & Revenue",
        "Builds the order staging models and the revenue marts that depend on them",
    ),
    (
        "Customer 360 Analytics",
        "stg_customers+",
        "Customer Analytics",
        "Builds the customer staging models and the lifetime-value marts",
    ),
    (
        "Staging Tier Only",
        "models/staging",
        "Staging Models Only",
        "Materialises and tests the staging folder without building marts",
    ),
    (
        "Gold Marts Only",
        "models/marts",
        "Marts Only",
        "Rebuilds the marts from already-materialised staging models",
    ),
]

_STATEMENT = text(
    "UPDATE pipeline_definitions SET name = :new_name, description = :description"
    " WHERE name = :old_name AND selector = :selector"
)


def _apply(pairs) -> None:
    for old_name, selector, new_name, description in pairs:
        op.execute(
            _STATEMENT.bindparams(
                new_name=new_name,
                description=description,
                old_name=old_name,
                selector=selector,
            )
        )


def upgrade() -> None:
    _apply(_RENAMES)


def downgrade() -> None:
    _apply(
        [(new, selector, old, "") for old, selector, new, _ in _RENAMES]
    )
