import asyncio
import os
from pathlib import Path
from typing import AsyncGenerator
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool
from app.config import settings

BACKEND_DIR = Path(__file__).resolve().parent.parent


def run_migrations() -> None:
    """Applies every pending Alembic revision to the brain database.

    Blocking by design — Alembic has no async API. Callers use
    ``asyncio.to_thread`` so the event loop keeps serving requests during startup.
    Alembic's own ``env.py`` derives the URL from Settings, so this always targets
    the same file as the async engine.
    """
    from alembic import command
    from alembic.config import Config

    config = Config(str(BACKEND_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    command.upgrade(config, "head")


def _resolve_engine_url() -> str:
    """Returns the SQLAlchemy URL for the internal brain database.

    For SQLite the URL is rebuilt from ``settings.sqlite_db_path`` so that the
    engine, the SDK and every subprocess (pipeline steps, dbt project) open
    the exact same file. A relative DATABASE_URL would otherwise be resolved
    against each process's own cwd, producing split-brain empty databases.
    """
    url = settings.DATABASE_URL
    if settings.is_sqlite:
        sqlite_path = settings.sqlite_db_path
        Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{sqlite_path}"
    return url


engine = create_async_engine(
    _resolve_engine_url(),
    echo=settings.DEBUG,
    future=True,
    # NullPool for SQLite on purpose. A pooled aiosqlite connection is bound to the
    # event loop that created it and to a live transaction; if a request or an engine
    # task is cancelled mid-transaction the rollback may never run, and the zombie
    # connection keeps SQLite's single write lock forever — every later writer then
    # stalls on busy_timeout. Opening per session costs microseconds and cannot leak a
    # lock. (Pooling is right for a networked database, not for a local file.)
    poolclass=NullPool if settings.is_sqlite else None,
)

@event.listens_for(engine.sync_engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Enforce WAL journal mode and foreign key constraints on SQLite connections.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    # Commit connections wait for a writer instead of failing immediately.
    # Several processes (API, SDK, step children) share this file.
    cursor.execute("PRAGMA busy_timeout=15000;")
    cursor.close()

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

async def seed_defaults(session: AsyncSession) -> None:
    """
    Idempotently seeds default system variables and pipeline definitions on startup.

    This used to live inside GET /api/variables and GET /api/pipelines handlers,
    which raced under concurrent page-load requests (both saw empty tables, both
    committed seeds) causing SQLite UNIQUE constraint / locking errors. Seeding
    now happens exactly once, here, during the startup lifespan.
    """
    from sqlalchemy import select
    from app.models.variable import Variable
    from app.models.pipeline import Pipeline, PipelineEdge, PipelineStep

    # Seed default variables if table is empty
    v_res = await session.execute(select(Variable.id).limit(1))
    if not v_res.scalars().first():
        session.add_all([
            Variable(
                key="ENV",
                value="development",
                is_secret=False,
                description="Active runtime environment mode (development / staging / production)",
            ),
            Variable(
                key="DEFAULT_CURRENCY",
                value="USD",
                is_secret=False,
                description="Base currency for analytical financial data marts",
            ),
            Variable(
                key="BATCH_SIZE",
                value="1000",
                is_secret=False,
                description="Default batch chunk size for pipeline data extraction",
            ),
        ])

    # Seed one starter pipeline if the table is empty. It is disabled: a fresh install
    # should not run dbt at 02:00 because a row was seeded. Enable it, retarget the
    # selectors, or delete it — the orchestrator has no privileged pipelines.
    p_res = await session.execute(select(Pipeline.id).limit(1))
    if not p_res.scalars().first():
        starter = Pipeline(
            id=str(__import__("uuid").uuid4()),
            name="Nightly dbt build",
            description="Runs every dbt model, then tests the project. Disabled until you enable it.",
            cron_expression="0 2 * * *",
            timezone="UTC",
            is_enabled=False,
            next_run_at=None,
        )
        run_step_id = str(__import__("uuid").uuid4())
        test_step_id = str(__import__("uuid").uuid4())
        session.add(starter)
        session.add_all(
            [
                PipelineStep(
                    id=run_step_id,
                    pipeline_id=starter.id,
                    name="dbt run",
                    step_type="dbt_run",
                    target="*",
                    position_x=80,
                    position_y=120,
                ),
                PipelineStep(
                    id=test_step_id,
                    pipeline_id=starter.id,
                    name="dbt test",
                    step_type="dbt_test",
                    target="*",
                    position_x=400,
                    position_y=120,
                ),
            ]
        )
        # Flush the foreign-key side first: these mappers declare no relationships,
        # so the unit of work would otherwise be free to insert the edge before the
        # steps it points at.
        await session.flush()
        session.add(
            PipelineEdge(
                pipeline_id=starter.id,
                from_step=run_step_id,
                to_step=test_step_id,
                condition="success",
            )
        )

    await session.commit()

async def _migrate_assets_unique_constraint(conn) -> None:
    """Rebuild `assets` with UNIQUE(layer, name) when a legacy schema is found.

    `Asset.name` used to be unique on its own, so a table existing in two tiers
    (e.g. bronze.orders and silver.orders) could not be catalogued twice. SQLite
    cannot drop an index-backed UNIQUE constraint in place, so the table is
    rebuilt and its rows copied over. Idempotent: a database that already has the
    composite key (including every freshly created one) is left untouched.
    """
    if not settings.is_sqlite:
        return

    # `assets` is a read-only view over catalog_objects now; there is no constraint to
    # rebuild, and PRAGMA on a view is meaningless.
    object_row = (
        await conn.exec_driver_sql("SELECT type FROM sqlite_master WHERE name = 'assets'")
    ).fetchone()
    if object_row is None or object_row[0] != "table":
        return

    index_rows = (await conn.exec_driver_sql("PRAGMA index_list('assets')")).fetchall()
    unique_columns = []
    for row in index_rows:
        index_name, is_unique = row[1], row[2]
        if not is_unique:
            continue
        info = (await conn.exec_driver_sql(f"PRAGMA index_info('{index_name}')")).fetchall()
        unique_columns.append(tuple(col[2] for col in info))

    if ("layer", "name") in unique_columns or ("name", "layer") in unique_columns:
        return  # already keyed on (layer, name)
    if ("name",) not in unique_columns:
        return  # unexpected schema — leave it alone rather than guess

    from sqlalchemy import MetaData
    from sqlalchemy.dialects import sqlite as sqlite_dialect
    from sqlalchemy.schema import CreateTable
    from app.models.asset import Asset

    staging = "assets_uq_migration"
    await conn.exec_driver_sql(f"DROP TABLE IF EXISTS {staging}")
    # Build the staging table from the model so a migrated database ends up with
    # exactly the schema `create_all` produces on a fresh install.
    staging_table = Asset.__table__.to_metadata(MetaData(), name=staging)
    await conn.exec_driver_sql(
        str(CreateTable(staging_table).compile(dialect=sqlite_dialect.dialect()))
    )

    existing = {row[1] for row in (await conn.exec_driver_sql("PRAGMA table_info('assets')")).fetchall()}
    columns = ", ".join(c.name for c in Asset.__table__.columns if c.name in existing)
    await conn.exec_driver_sql(f"INSERT INTO {staging} ({columns}) SELECT {columns} FROM assets")
    await conn.exec_driver_sql("DROP TABLE assets")
    await conn.exec_driver_sql(f"ALTER TABLE {staging} RENAME TO assets")
    # The legacy unique index is dropped with the old table; recreate it as the
    # plain (non-unique) lookup index the model declares.
    await conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_assets_name ON assets (name)")


async def init_db() -> None:
    """Initializes the brain database and enforces WAL mode.

    Schema changes are owned by Alembic (audit M16): ``create_all`` is gone, so a
    fresh install and an upgraded one follow the same path — ``upgrade head``.
    Migrations run synchronously in a worker thread against the same SQLite file
    the async engine uses, because Alembic's API is blocking.
    """
    if settings.is_sqlite:
        path_str = settings.sqlite_db_path
        Path(path_str).parent.mkdir(parents=True, exist_ok=True)

    await asyncio.to_thread(run_migrations)

    async with engine.begin() as conn:
        # Explicitly verify PRAGMA in async session
        await conn.run_sync(lambda sync_conn: sync_conn.execute(
            # ensures WAL pragma is executed
            __import__("sqlalchemy").text("PRAGMA journal_mode=WAL;")
        ))
        # Workstream 3.1: legacy databases key assets on `name` alone, which
        # collides across tiers. Runs after migrations so databases created by
        # the baseline (already correct) short-circuit.
        await _migrate_assets_unique_constraint(conn)

    # Seed default system data exactly once at startup (idempotent).
    async with AsyncSessionLocal() as session:
        await seed_defaults(session)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for obtaining an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
