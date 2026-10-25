"""Remove layers: the warehouse directory is the only structure there is.

Layers were a table of folders with settings (default target, order, colour) and a
``layer_path`` on every catalog row. Both were a second description of what the
filesystem already says, and the two drifted — catalog rows for folders nothing held,
folders nothing catalogued.

This revision deletes that second description. A table's address is its path
(``lakehouse.<folder>.<name>``), its folder is a directory, and the catalog row is a
projection of the file. Nothing needs to be backfilled: the projection rebuilds every
lakehouse row from the files on the next catalog read.

Existing rows whose files still exist are kept (their ``fqn`` is already the address);
rows for files that are gone are removed by the projection.
"""
from alembic import op
import sqlalchemy as sa

revision = "0012_no_layers"
down_revision = "0011_run_lineage"
branch_labels = None
depends_on = None

_LAYERS = """
CREATE TABLE IF NOT EXISTS layers (
    path VARCHAR(255) NOT NULL,
    name VARCHAR(100) NOT NULL,
    parent_path VARCHAR(255),
    position INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    color VARCHAR(32),
    icon VARCHAR(64),
    is_default BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (path)
)
"""

# `assets` is the compatibility projection over catalog_objects. It used to expose
# `layer_path AS layer`; with the column gone, the folder is read back out of the
# file's own path (everything before the final "/"). Root-level files have no folder.
_ASSETS_VIEW = """
CREATE VIEW assets AS
SELECT
    id,
    name,
    CASE
        WHEN instr(location, '/') > 0
        THEN rtrim(rtrim(location, replace(location, '/', '')), '/')
    END AS layer,
    location AS storage_uri,
    CASE kind
        WHEN 'model' THEN 'postgres_table'
        WHEN 'shortcut' THEN 'external'
        WHEN 'external' THEN 'external'
        ELSE kind
    END AS format,
    COALESCE(row_count, 0) AS record_count,
    schema_json AS schema_metadata,
    created_at,
    last_materialized_at
FROM catalog_objects
WHERE kind IN ('parquet', 'model')
"""

_LEGACY_ASSETS_VIEW = """
CREATE VIEW assets AS
SELECT
    id,
    name,
    layer_path AS layer,
    location AS storage_uri,
    CASE kind
        WHEN 'model' THEN 'postgres_table'
        WHEN 'shortcut' THEN 'external'
        WHEN 'external' THEN 'external'
        ELSE kind
    END AS format,
    COALESCE(row_count, 0) AS record_count,
    schema_json AS schema_metadata,
    created_at,
    last_materialized_at
FROM catalog_objects
WHERE kind IN ('parquet', 'model')
"""


def upgrade() -> None:
    # SQLite rebuilds a table by renaming it, which fails while a view still points at
    # the old name — and the old view also selects the column being dropped.
    op.execute("DROP VIEW IF EXISTS assets")
    op.execute("DROP INDEX IF EXISTS ix_catalog_objects_layer_path")
    with op.batch_alter_table("catalog_objects") as batch:
        batch.drop_column("layer_path")
    op.execute("DROP INDEX IF EXISTS ix_layers_parent_path")
    op.execute("DROP TABLE IF EXISTS layers")
    op.execute(_ASSETS_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS assets")
    with op.batch_alter_table("catalog_objects") as batch:
        batch.add_column(sa.Column("layer_path", sa.String(255), nullable=True))
    op.execute("CREATE INDEX IF NOT EXISTS ix_catalog_objects_layer_path ON catalog_objects (layer_path)")
    op.execute(_LEGACY_ASSETS_VIEW)
    op.execute(_LAYERS)
    op.execute("CREATE INDEX IF NOT EXISTS ix_layers_parent_path ON layers (parent_path)")
