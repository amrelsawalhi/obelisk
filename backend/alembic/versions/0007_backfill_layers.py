"""Give every layer name that already exists in the catalog a row in the tree.

Before the layer tree existed, a table's layer was whatever folder name the caller
passed. Those folders are still on disk with data in them, so they are real layers —
they just have no row. This backfills one row per distinct `assets.layer`, with the
same sanitising rule the application uses, so the tree describes what is actually
there instead of only what was created through the UI.

Idempotent: existing paths are left untouched.
"""
import re

from alembic import op

revision = "0007_backfill_layers"
down_revision = "0006_pipeline_examples"
branch_labels = None
depends_on = None

# Frozen copy of app.core.layers.safe_layer_path. A migration must keep behaving the
# same way forever, so it does not import code that is free to change.
_UNSAFE = re.compile(r"[^A-Za-z0-9_-]+")


def _sanitise(value: str) -> str:
    cleaned = _UNSAFE.sub("_", str(value or "")).strip("_")
    return cleaned or "raw"


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.exec_driver_sql(
        "SELECT DISTINCT layer FROM assets WHERE layer IS NOT NULL AND TRIM(layer) <> ''"
    ).fetchall()
    known = {
        row[0]
        for row in bind.exec_driver_sql("SELECT path FROM layers").fetchall()
    }

    for (layer,) in rows:
        path = _sanitise(layer)
        if path in known:
            continue
        bind.exec_driver_sql(
            "INSERT INTO layers (path, name, parent_path, position, description,"
            " is_default, created_at, updated_at)"
            " VALUES (?, ?, NULL, 10, 'Discovered from existing tables', 0,"
            " CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (path, path),
        )
        known.add(path)


def downgrade() -> None:
    # The rows describe data that is still on disk; removing them would only make the
    # tree lie again.
    pass
