"""Make `catalog_objects` the only table that knows what exists.

`assets` was the original catalog: one row per Parquet file or materialised dbt model,
keyed on ``(layer, name)``. `catalog_objects` arrived beside it with a richer shape and
a unique ``fqn``, and for a while both were written.

This revision ends that: any row still only in `assets` is copied across (deduplicated
by fqn), the table is dropped, and `assets` becomes a read-only projection over
`catalog_objects` for the callers and tests that still read the old column names. Writes
go to `catalog_objects` from here on.
"""
from alembic import op

revision = "0008_catalog_is_the_table"
down_revision = "0007_backfill_layers"
branch_labels = None
depends_on = None

_BACKFILL = """
INSERT OR IGNORE INTO catalog_objects (
    id, fqn, name, layer_path, kind, backend, location, schema_json,
    row_count, size_bytes, version, created_at, last_materialized_at
)
SELECT
    a.id,
    'lakehouse.' || COALESCE(a.layer, 'raw') || '.' || a.name,
    a.name,
    COALESCE(a.layer, 'raw'),
    CASE WHEN a.format = 'postgres_table' THEN 'model' ELSE 'parquet' END,
    CASE
        WHEN a.format = 'postgres_table' THEN 'postgres'
        WHEN a.storage_uri LIKE 's3://%'    THEN 's3'
        WHEN a.storage_uri LIKE 'gs://%'    THEN 'gcs'
        WHEN a.storage_uri LIKE 'abfss://%' THEN 'azure'
        ELSE 'local'
    END,
    a.storage_uri,
    a.schema_metadata,
    a.record_count,
    NULL,
    1,
    a.created_at,
    a.last_materialized_at
FROM assets a
"""

# The old column names, projected from the registry. Read-only: SQLite views reject
# writes, which is the point — nothing should be writing `assets` any more.
_VIEW = """
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

_TABLE = """
CREATE TABLE IF NOT EXISTS assets (
    id VARCHAR(36) NOT NULL,
    name VARCHAR(200) NOT NULL,
    layer VARCHAR(50),
    storage_uri TEXT NOT NULL,
    format VARCHAR(50) NOT NULL,
    record_count INTEGER NOT NULL,
    schema_metadata JSON,
    created_at DATETIME NOT NULL,
    last_materialized_at DATETIME,
    PRIMARY KEY (id),
    CONSTRAINT uq_assets_layer_name UNIQUE (layer, name)
)
"""


def upgrade() -> None:
    op.execute(_BACKFILL)
    op.execute("DROP TABLE IF EXISTS assets")
    op.execute("DROP VIEW IF EXISTS assets")
    op.execute(_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS assets")
    op.execute(_TABLE)
    op.execute(
        """
        INSERT OR IGNORE INTO assets
            (id, name, layer, storage_uri, format, record_count, schema_metadata,
             created_at, last_materialized_at)
        SELECT id, name, layer_path, location,
               CASE WHEN kind = 'model' THEN 'postgres_table' ELSE 'parquet' END,
               COALESCE(row_count, 0), schema_json, created_at, last_materialized_at
        FROM catalog_objects
        """
    )
