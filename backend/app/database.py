import os
from pathlib import Path
from typing import AsyncGenerator
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from app.config import settings
from app.models.base import Base

# Ensure parent directory for database exists
db_url = settings.DATABASE_URL
if db_url.startswith("sqlite+aiosqlite:///"):
    sqlite_path = db_url.replace("sqlite+aiosqlite:///", "")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
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
    from app.models.pipeline_definition import PipelineDefinition

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

    # Seed default pipeline definitions if table is empty
    p_res = await session.execute(select(PipelineDefinition.id).limit(1))
    if not p_res.scalars().first():
        session.add_all([
            PipelineDefinition(
                name="All Medallion Assets",
                description="Full end-to-end Medallion pipeline from Bronze raw Parquet through Silver staging to Gold marts",
                selector="*",
            ),
            PipelineDefinition(
                name="Orders & Revenue Flow",
                description="Processes transactional orders from Bronze staging to financial and enriched fact marts",
                selector="stg_orders+",
            ),
            PipelineDefinition(
                name="Customer 360 Analytics",
                description="Cleans customer dimensions and updates lifetime value & metric marts",
                selector="stg_customers+",
            ),
            PipelineDefinition(
                name="Staging Tier Only",
                description="Rapid materialization and validation of Silver staging models without gold aggregations",
                selector="models/staging",
            ),
            PipelineDefinition(
                name="Gold Marts Only",
                description="Refreshes analytical business marts and metrics from materialized silver staging models",
                selector="models/marts",
            ),
        ])

    await session.commit()

async def init_db() -> None:
    """Initialize database tables and enforce WAL mode."""
    # Ensure directory exists
    if settings.DATABASE_URL.startswith("sqlite+aiosqlite:///"):
        path_str = settings.DATABASE_URL.replace("sqlite+aiosqlite:///", "")
        Path(path_str).parent.mkdir(parents=True, exist_ok=True)

    async with engine.begin() as conn:
        # Explicitly verify PRAGMA in async session
        await conn.run_sync(lambda sync_conn: sync_conn.execute(
            # ensures WAL pragma is executed
            __import__("sqlalchemy").text("PRAGMA journal_mode=WAL;")
        ))
        await conn.run_sync(Base.metadata.create_all)

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
