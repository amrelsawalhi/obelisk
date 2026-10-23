"""Alembic environment for the Obelisk brain.

Migrations run synchronously against the resolved SQLite path; the API itself
uses the async driver. Deriving the URL from Settings here (instead of hardcoding
it in alembic.ini) guarantees `alembic upgrade` and the running process always
touch the same file, regardless of the working directory.
"""
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, event, pool

from app.config import settings
from app.models.base import Base
import app.models  # noqa: F401  — registers every model on Base.metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _sync_url() -> str:
    """Returns a synchronous SQLAlchemy URL for the brain database.

    An explicit ``sqlalchemy.url`` on the config wins, which is how tests point a
    single migration run at a throwaway file without mutating global settings.
    """
    override = config.get_main_option("sqlalchemy.url", None)
    if override:
        return override
    if settings.is_sqlite:
        return f"sqlite:///{settings.sqlite_db_path}"
    return settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")


def run_migrations_online() -> None:
    url = _sync_url()
    is_sqlite = url.startswith("sqlite")

    engine = create_engine(
        url,
        poolclass=pool.NullPool,
        # The API may be holding the brain open (WAL). Waiting beats failing.
        connect_args={"timeout": 30} if is_sqlite else {},
    )

    if is_sqlite:
        # Python's sqlite3 driver defers BEGIN until the first DML, which would let
        # each DDL statement autocommit: a migration that fails halfway would leave
        # tables behind with no version stamp. Taking over transaction control is
        # the documented way to get atomic DDL on SQLite.
        @event.listens_for(engine, "connect")
        def _disable_implicit_begin(dbapi_connection, _record):
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def _explicit_begin(connection):
            connection.exec_driver_sql("BEGIN")

    try:
        with engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_as_batch=True,
                # SQLite does have transactional DDL; telling Alembic so makes the
                # whole revision — tables, seed and version stamp — commit or roll
                # back as one unit.
                transactional_ddl=is_sqlite,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


def run_migrations_offline() -> None:
    context.configure(
        url=_sync_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
