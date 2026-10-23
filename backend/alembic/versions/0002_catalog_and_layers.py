"""Catalog objects and layers (Phase 1a).

Revision ID: 0002_catalog_and_layers
Revises: 0001_baseline
Create Date: 2026-10-24

Introduces the two tables the rest of the architecture hangs off:

* ``layers`` keeps the user's nested layer tree as a materialised path. A single
  flat ``raw`` layer is seeded so the app always has a valid landing target.
* ``catalog_objects`` becomes the authoritative registry. This revision only
  backfills it from the legacy ``assets`` table; ``assets`` stays in place (and
  authoritative for writes) until Phase 4 flips the write path.
"""
from alembic import op

revision = "0002_catalog_and_layers"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


_LAYERS_DDL = """
CREATE TABLE IF NOT EXISTS layers (
    path VARCHAR(255) NOT NULL,
    name VARCHAR(100) NOT NULL,
    parent_path VARCHAR(255),
    position INTEGER DEFAULT '0' NOT NULL,
    description TEXT,
    color VARCHAR(32),
    icon VARCHAR(64),
    is_default BOOLEAN DEFAULT '0' NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (path)
)
"""

_CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS catalog_objects (
    id VARCHAR(36) NOT NULL,
    fqn VARCHAR(512) NOT NULL,
    name VARCHAR(200) NOT NULL,
    layer_path VARCHAR(255),
    kind VARCHAR(32) DEFAULT 'table' NOT NULL,
    backend VARCHAR(32) DEFAULT 'local' NOT NULL,
    location TEXT,
    connection_id VARCHAR(36),
    schema_json JSON,
    capabilities_json JSON,
    watermark_json JSON,
    row_count INTEGER,
    size_bytes INTEGER,
    produced_by VARCHAR(255),
    tags TEXT,
    docs TEXT,
    version INTEGER DEFAULT '1' NOT NULL,
    created_at DATETIME NOT NULL,
    last_materialized_at DATETIME,
    PRIMARY KEY (id)
)
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_layers_parent_path ON layers (parent_path)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_catalog_objects_fqn ON catalog_objects (fqn)",
    "CREATE INDEX IF NOT EXISTS ix_catalog_objects_layer_path ON catalog_objects (layer_path)",
    "CREATE INDEX IF NOT EXISTS ix_catalog_objects_connection_id ON catalog_objects (connection_id)",
    "CREATE INDEX IF NOT EXISTS ix_catalog_objects_kind ON catalog_objects (kind)",
]


def upgrade() -> None:
    """Creates the registry and converges it from whatever state it is in.

    Written with ``IF NOT EXISTS`` / ``OR IGNORE`` because an appliance can be
    killed mid-startup: a previous run may have created a table without reaching
    the version stamp. Re-running must finish the job rather than fail on its own
    leftovers — that half-applied state was observed while building this phase.
    """
    op.execute(_LAYERS_DDL)
    op.execute(_CATALOG_DDL)
    for statement in _INDEXES:
        op.execute(statement)

    # Seed exactly one flat layer. Users rename or extend it from the UI.
    op.execute(
        """
        INSERT OR IGNORE INTO layers
            (path, name, parent_path, position, description, is_default, created_at, updated_at)
        VALUES
            ('raw', 'raw', NULL, 1, 'Landing layer — rename it or add your own', 1,
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )

    # Backfill the registry from the legacy assets table. `rowid IN (MIN ...)`
    # collapses the one case UNIQUE(layer, name) cannot catch — duplicate NULL
    # layers — so one fqn can never produce two catalog rows.
    op.execute(
        """
        INSERT INTO catalog_objects (
            id, fqn, name, layer_path, kind, backend, location, schema_json,
            row_count, size_bytes, version, created_at, last_materialized_at
        )
        SELECT
            lower(hex(randomblob(16))),
            'lakehouse.' || COALESCE(a.layer, 'raw') || '.' || a.name,
            a.name,
            COALESCE(a.layer, 'raw'),
            'parquet',
            CASE
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
        WHERE a.rowid IN (
            SELECT MIN(rowid) FROM assets
            GROUP BY 'lakehouse.' || COALESCE(layer, 'raw') || '.' || name
        )
        AND NOT EXISTS (
            SELECT 1 FROM catalog_objects c
            WHERE c.fqn = 'lakehouse.' || COALESCE(a.layer, 'raw') || '.' || a.name
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS catalog_objects")
    op.execute("DROP TABLE IF EXISTS layers")
