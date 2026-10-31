"""The two SQLite files honour the operator's durability modes.

The control-plane database used to have ``PRAGMA journal_mode=WAL`` forced in
``init_db()`` — after the connect listener had already applied the operator's choice —
and the DuckLake catalog applied the journal mode but never the synchronous mode. On a
filesystem whose ``fsync`` is unsupported (9p, some NFS/SMB mounts) that combination
turned a documented configuration into ``sqlite3.OperationalError: disk I/O error``.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.config import settings
from app.core import catalog as catalog_module
from app.core.sqlite_pragmas import pragma_values


def test_the_shared_validator_accepts_the_documented_modes() -> None:
    original = (settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS)
    try:
        settings.SQLITE_JOURNAL_MODE = "WAL"
        settings.SQLITE_SYNCHRONOUS = "NORMAL"
        assert pragma_values() == ("WAL", "NORMAL")
        settings.SQLITE_JOURNAL_MODE = "delete"
        settings.SQLITE_SYNCHRONOUS = "off"
        assert pragma_values() == ("DELETE", "OFF")
    finally:
        settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS = original


@pytest.mark.parametrize(
    "journal, synchronous, expected",
    [
        ("not-a-mode", "NORMAL", "OBELISK_SQLITE_JOURNAL_MODE"),
        ("WAL", "sometimes", "OBELISK_SQLITE_SYNCHRONOUS"),
    ],
)
def test_an_invalid_mode_names_the_setting(journal, synchronous, expected) -> None:
    original = (settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS)
    try:
        settings.SQLITE_JOURNAL_MODE = journal
        settings.SQLITE_SYNCHRONOUS = synchronous
        with pytest.raises(RuntimeError, match=expected):
            pragma_values()
    finally:
        settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS = original


def test_the_catalog_takes_the_configured_journal_mode(tmp_path) -> None:
    """``DELETE`` on a virtual filesystem must reach the DuckLake metadata file."""
    original = (settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS)
    catalog_file = tmp_path / "catalog.sqlite"
    try:
        settings.SQLITE_JOURNAL_MODE = "DELETE"
        settings.SQLITE_SYNCHRONOUS = "OFF"
        catalog_module._prepared_catalogs.discard(str(catalog_file))
        catalog_module._prepare_catalog_file(catalog_file)
    finally:
        settings.SQLITE_JOURNAL_MODE, settings.SQLITE_SYNCHRONOUS = original
        catalog_module._prepared_catalogs.discard(str(catalog_file))

    with sqlite3.connect(catalog_file) as conn:
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    assert str(mode).lower() == "delete"


def test_init_db_does_not_force_wal_over_the_operator_choice() -> None:
    """A source-level guard: the hardcoded PRAGMA is what broke the DELETE fallback.

    The executable form ended with a semicolon; the docstring above ``init_db`` can
    (and does) mention the old statement as history without tripping this.
    """
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1] / "app" / "database.py").read_text(
        encoding="utf-8"
    )
    assert "journal_mode=WAL;" not in source, (
        "init_db must not override OBELISK_SQLITE_JOURNAL_MODE; the connect listener "
        "applies the validated setting to every connection"
    )
