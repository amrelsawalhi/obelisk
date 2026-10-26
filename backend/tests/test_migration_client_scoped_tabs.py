"""Migration 0015, up and down, against a throwaway brain.

The unit tests prove the API scopes tabs. This proves the *data*: rows written by the
single-client version of Obelisk survive the upgrade under ``default`` (they are what
that client was looking at), and the downgrade leaves a table the pre-0015 code can
read — one global namespace, no ``client_id`` column, no leftover index.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config

BACKEND_DIR = Path(__file__).resolve().parent.parent
BASELINE_REVISION = "0014_attachable_drivers"
SCOPED_REVISION = "0015_client_scoped_query_tabs"
CLIENT_INDEX = "ix_open_query_tabs_client_id"


def _config(db_path: Path) -> Config:
    config = Config(str(BACKEND_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    # env.py lets an explicit url win, which is what keeps this off the real brain.
    config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path.as_posix()}")
    return config


def _columns(db_path: Path, table: str = "open_query_tabs") -> set[str]:
    with sqlite3.connect(db_path) as con:
        return {row[1] for row in con.execute(f"PRAGMA table_info({table})")}


def _indexes(db_path: Path, table: str = "open_query_tabs") -> set[str]:
    with sqlite3.connect(db_path) as con:
        return {row[1] for row in con.execute(f"PRAGMA index_list({table})")}


def _tabs(db_path: Path) -> list[tuple[str, str]]:
    with sqlite3.connect(db_path) as con:
        return list(con.execute("SELECT id, client_id FROM open_query_tabs ORDER BY id"))


def _ids(db_path: Path) -> list[str]:
    """Tab ids only: the downgraded table has no ``client_id`` to select."""
    with sqlite3.connect(db_path) as con:
        return [row[0] for row in con.execute("SELECT id FROM open_query_tabs ORDER BY id")]


def _insert_legacy_tab(db_path: Path, tab_id: str) -> None:
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO open_query_tabs "
            "(id, title, sql_text, target_id, schema_name, tab_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tab_id, "Query 1", "select 1;", "duckdb", None, 0,
             "2025-01-01 00:00:00", "2025-01-01 00:00:00"),
        )
        con.commit()


def test_0015_rekeys_existing_tabs_then_reverses_cleanly(tmp_path):
    db_path = tmp_path / "brain.db"
    config = _config(db_path)

    command.upgrade(config, BASELINE_REVISION)
    _insert_legacy_tab(db_path, "tab-legacy")
    _insert_legacy_tab(db_path, "tab-2")
    assert "client_id" not in _columns(db_path)

    command.upgrade(config, "head")

    assert "client_id" in _columns(db_path)
    assert CLIENT_INDEX in _indexes(db_path)
    assert _tabs(db_path) == [
        ("default:tab-2", "default"),
        ("default:tab-legacy", "default"),
    ], "pre-0015 rows belong to the single global client, not to nobody"

    # A second client can now coexist with the migrated rows.
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO open_query_tabs "
            "(id, client_id, title, sql_text, target_id, schema_name, tab_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("teamb:tab-1", "teamb", "Query 1", "select 2;", "duckdb", None, 0,
             "2025-01-02 00:00:00", "2025-01-02 00:00:00"),
        )
        con.commit()
    command.upgrade(config, "head")  # idempotent at head: nothing to apply
    assert len(_tabs(db_path)) == 3

    command.downgrade(config, BASELINE_REVISION)

    assert "client_id" not in _columns(db_path)
    assert CLIENT_INDEX not in _indexes(db_path)
    assert _ids(db_path) == ["tab-2", "tab-legacy"], (
        "the downgrade restores the pre-0015 keys and drops the extra clients' tabs, "
        "which could not exist in a global namespace"
    )


def test_0015_upgrade_does_not_double_prefix_an_already_scoped_row(tmp_path):
    """Re-running the re-key (a retried migration) must not produce 'default:default:x'."""
    db_path = tmp_path / "brain.db"
    config = _config(db_path)

    command.upgrade(config, SCOPED_REVISION)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO open_query_tabs "
            "(id, client_id, title, sql_text, target_id, schema_name, tab_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("default:tab-1", "default", "Query 1", "select 1;", "duckdb", None, 0,
             "2025-01-01 00:00:00", "2025-01-01 00:00:00"),
        )
        con.commit()
    command.downgrade(config, BASELINE_REVISION)
    command.upgrade(config, "head")

    assert _tabs(db_path) == [("default:tab-1", "default")]
    assert _columns(db_path) >= {"id", "client_id", "sql_text"}
