"""Baseline schema (audit M16).

Revision ID: 0001_baseline
Revises:
Create Date: 2026-10-24

The baseline is intentionally idempotent: an installation that predates Alembic
already has these tables (and possibly real data), so it is stamped without
re-creating anything. Fresh databases get exactly the DDL the models described
when this revision was written; later schema changes belong in new revisions,
never here.

Raw DDL is used on purpose — a baseline that called `Base.metadata.create_all()`
would silently change meaning every time a model changes.
"""
from alembic import op
import sqlalchemy as sa

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None

_TABLES = [
    """
    CREATE TABLE connections (
        id VARCHAR(36) NOT NULL,
        name VARCHAR(100) NOT NULL,
        source_type VARCHAR(50) NOT NULL,
        host VARCHAR(255) NOT NULL,
        port INTEGER NOT NULL,
        "database" VARCHAR(100) NOT NULL,
        username VARCHAR(100) NOT NULL,
        encrypted_credentials TEXT NOT NULL,
        options JSON,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE service_configs (
        id VARCHAR(36) NOT NULL,
        service_name VARCHAR(50) NOT NULL,
        deployment_type VARCHAR(20) NOT NULL,
        status VARCHAR(50) NOT NULL,
        container_id VARCHAR(100),
        host VARCHAR(255) NOT NULL,
        port INTEGER NOT NULL,
        allocated_memory_mb INTEGER NOT NULL,
        config_json JSON,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE assets (
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
    """,
    """
    CREATE TABLE query_history (
        id VARCHAR(36) NOT NULL,
        title VARCHAR(200),
        sql_text TEXT NOT NULL,
        status VARCHAR(50) NOT NULL,
        executed_at DATETIME NOT NULL,
        duration_ms INTEGER,
        row_count INTEGER,
        error_message TEXT,
        is_saved BOOLEAN NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE cron_jobs (
        id VARCHAR(36) NOT NULL,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        target_type VARCHAR(30) NOT NULL,
        target VARCHAR(255) NOT NULL,
        cron_expression VARCHAR(100) NOT NULL,
        is_enabled BOOLEAN NOT NULL,
        last_run_at DATETIME,
        last_status VARCHAR(30) NOT NULL,
        last_duration_seconds FLOAT,
        last_logs TEXT,
        next_run_at DATETIME,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE open_query_tabs (
        id VARCHAR(64) NOT NULL,
        title VARCHAR(200) NOT NULL,
        sql_text TEXT NOT NULL,
        target_id VARCHAR(100) NOT NULL,
        schema_name VARCHAR(100),
        tab_order INTEGER NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE pipeline_definitions (
        id VARCHAR(36) NOT NULL,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        selector VARCHAR(200) NOT NULL,
        schedule VARCHAR(100),
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE variables (
        id VARCHAR NOT NULL,
        "key" VARCHAR NOT NULL,
        value TEXT NOT NULL,
        is_secret BOOLEAN NOT NULL,
        description VARCHAR,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    )
    """,
]

_INDEXES = [
    "CREATE INDEX ix_assets_name ON assets (name)",
    "CREATE UNIQUE INDEX ix_connections_name ON connections (name)",
    "CREATE UNIQUE INDEX ix_cron_jobs_name ON cron_jobs (name)",
    "CREATE UNIQUE INDEX ix_pipeline_definitions_name ON pipeline_definitions (name)",
    "CREATE UNIQUE INDEX ix_service_configs_service_name ON service_configs (service_name)",
    'CREATE UNIQUE INDEX ix_variables_key ON variables ("key")',
]

_DROP_ORDER = [
    "variables",
    "pipeline_definitions",
    "open_query_tabs",
    "cron_jobs",
    "query_history",
    "assets",
    "service_configs",
    "connections",
]


def upgrade() -> None:
    """Creates the baseline schema on a fresh database, and only stamps an old one."""
    bind = op.get_bind()
    if sa.inspect(bind).has_table("assets"):
        return  # pre-existing installation: keep its data untouched
    for statement in _TABLES:
        op.execute(statement)
    for statement in _INDEXES:
        op.execute(statement)


def downgrade() -> None:
    for table in _DROP_ORDER:
        op.execute(f"DROP TABLE IF EXISTS {table}")
