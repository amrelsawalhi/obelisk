"""Built-in pipeline orchestrator (replaces cron + Dagster).

Revision ID: 0009_pipelines
Revises: 0008_catalog_is_the_table
Create Date: 2026-10-24

Obelisk had two schedulers: an in-process cron engine writing one flat row per job,
and an optional Dagster service that brought its own GraphQL API, workspace files and
container. Neither could express "step B runs only if step A failed", and both lost
the shape of the work: cron stored a single ``last_logs`` blob and Dagster kept its
history outside the brain database.

These tables are the durable state of the one orchestrator that replaces them: the
graph the operator drew, the schedule anchor, and a per-step execution record that
survives a crash mid-run. WAL already guarantees every committed transition outlives
the process, so "resume after restart" is a property of the schema, not of any code
path.

``cron_jobs`` and ``pipeline_definitions`` are dropped with their contents: there is
no customer data to migrate (all schedules in the field were test fixtures) and the
seeded definitions are re-created by ``app.database.seed_defaults``.
"""
from alembic import op

revision = "0009_pipelines"
down_revision = "0008_catalog_is_the_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipelines (
            id VARCHAR(36) NOT NULL,
            name VARCHAR(120) NOT NULL,
            description TEXT,
            cron_expression VARCHAR(100) NOT NULL DEFAULT '0 2 * * *',
            timezone VARCHAR(60) NOT NULL DEFAULT 'UTC',
            is_enabled BOOLEAN NOT NULL DEFAULT 1,
            overlap_policy VARCHAR(20) NOT NULL DEFAULT 'skip',
            max_parallel INTEGER NOT NULL DEFAULT 3,
            keep_runs INTEGER NOT NULL DEFAULT 100,
            timeout_seconds INTEGER NOT NULL DEFAULT 3600,
            max_retries INTEGER NOT NULL DEFAULT 0,
            retry_delay_seconds INTEGER NOT NULL DEFAULT 60,
            auto_rerun_interrupted BOOLEAN NOT NULL DEFAULT 0,
            next_run_at DATETIME,
            last_status VARCHAR(30) NOT NULL DEFAULT 'idle',
            last_run_at DATETIME,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uq_pipelines_name UNIQUE (name)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_pipelines_name ON pipelines (name)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_pipelines_next_run_at ON pipelines (next_run_at)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_params (
            id VARCHAR(36) NOT NULL,
            pipeline_id VARCHAR(36) NOT NULL,
            name VARCHAR(80) NOT NULL,
            type VARCHAR(20) NOT NULL DEFAULT 'string',
            default_value TEXT,
            required BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_param UNIQUE (pipeline_id, name),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_pipeline_params_pipeline_id ON pipeline_params (pipeline_id)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_steps (
            id VARCHAR(36) NOT NULL,
            pipeline_id VARCHAR(36) NOT NULL,
            name VARCHAR(120) NOT NULL,
            step_type VARCHAR(20) NOT NULL DEFAULT 'dbt_run',
            target TEXT NOT NULL DEFAULT '',
            args_json TEXT,
            timeout_seconds INTEGER,
            max_retries INTEGER,
            retry_delay_seconds INTEGER,
            position_x FLOAT NOT NULL DEFAULT 0,
            position_y FLOAT NOT NULL DEFAULT 0,
            PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_step_name UNIQUE (pipeline_id, name),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_pipeline_steps_pipeline_id ON pipeline_steps (pipeline_id)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_edges (
            id VARCHAR(36) NOT NULL,
            pipeline_id VARCHAR(36) NOT NULL,
            from_step VARCHAR(36) NOT NULL,
            to_step VARCHAR(36) NOT NULL,
            condition VARCHAR(20) NOT NULL DEFAULT 'success',
            PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_edge UNIQUE (from_step, to_step),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines (id) ON DELETE CASCADE,
            FOREIGN KEY (from_step) REFERENCES pipeline_steps (id) ON DELETE CASCADE,
            FOREIGN KEY (to_step) REFERENCES pipeline_steps (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_pipeline_edges_pipeline_id ON pipeline_edges (pipeline_id)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id VARCHAR(36) NOT NULL,
            pipeline_id VARCHAR(36) NOT NULL,
            kind VARCHAR(20) NOT NULL DEFAULT 'manual',
            triggered_by VARCHAR(80),
            params_json TEXT,
            status VARCHAR(30) NOT NULL DEFAULT 'pending',
            scheduled_for DATETIME,
            started_at DATETIME,
            ended_at DATETIME,
            duration_seconds FLOAT,
            error TEXT,
            PRIMARY KEY (id),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_pipeline_id ON pipeline_runs (pipeline_id)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_pipeline_runs_status ON pipeline_runs (status)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_pipeline_started "
        "ON pipeline_runs (pipeline_id, started_at)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS step_runs (
            id VARCHAR(36) NOT NULL,
            run_id VARCHAR(36) NOT NULL,
            step_id VARCHAR(36) NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            status VARCHAR(30) NOT NULL DEFAULT 'pending',
            claimed_at DATETIME,
            started_at DATETIME,
            ended_at DATETIME,
            duration_seconds FLOAT,
            next_retry_at DATETIME,
            logs TEXT,
            error TEXT,
            PRIMARY KEY (id),
            FOREIGN KEY (run_id) REFERENCES pipeline_runs (id) ON DELETE CASCADE
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_step_runs_run_id ON step_runs (run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_step_runs_step_id ON step_runs (step_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_step_runs_status ON step_runs (status)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_step_runs_next_retry_at ON step_runs (next_retry_at)"
    )

    # The two schedulers this replaces. Nothing in the field holds live data: every
    # row was a test fixture (see the module docstring).
    op.execute("DROP TABLE IF EXISTS cron_jobs")
    op.execute("DROP TABLE IF EXISTS pipeline_definitions")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS step_runs")
    op.execute("DROP TABLE IF EXISTS pipeline_runs")
    op.execute("DROP TABLE IF EXISTS pipeline_edges")
    op.execute("DROP TABLE IF EXISTS pipeline_steps")
    op.execute("DROP TABLE IF EXISTS pipeline_params")
    op.execute("DROP TABLE IF EXISTS pipelines")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cron_jobs (
            id VARCHAR(36) NOT NULL,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            target_type VARCHAR(30) NOT NULL DEFAULT 'dbt',
            target VARCHAR(255) NOT NULL DEFAULT '*',
            cron_expression VARCHAR(100) NOT NULL DEFAULT '0 2 * * *',
            is_enabled BOOLEAN NOT NULL DEFAULT 1,
            last_run_at DATETIME,
            last_status VARCHAR(30) NOT NULL DEFAULT 'idle',
            last_duration_seconds FLOAT,
            last_logs TEXT,
            next_run_at DATETIME,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uq_cron_jobs_name UNIQUE (name)
        )
        """
    )
    # 0006 rewrites these seeded rows, so a downgrade must restore the table or the
    # next upgrade fails before it can rename anything.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_definitions (
            id VARCHAR(36) NOT NULL,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            selector VARCHAR(200) NOT NULL DEFAULT '*',
            schedule VARCHAR(100),
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_definitions_name UNIQUE (name)
        )
        """
    )

