"""`catalog_objects` is the table; `assets` is a projection of it.

The catalog used to be two tables kept in step by hand. This pins the end state: one
table receives every write, and the old name survives only as a read-only view with the
old column names, so nothing can drift back.
"""
import shutil
import sqlite3
import uuid

import polars as pl
import pytest

from app.config import settings
from app.sdk import obelisk


def _cleanup(layer: str, name: str) -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute("DELETE FROM catalog_objects WHERE fqn = ?", (f"lakehouse.{layer}.{name}",))
        con.execute("DELETE FROM layers WHERE path = ?", (layer,))
        con.commit()
    # The write also landed a Parquet file in the lakehouse; remove it so later tests
    # do not discover a probe table.
    shutil.rmtree(settings.lakehouse_root_path / layer, ignore_errors=True)


def test_assets_is_a_view_not_a_table() -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        kind = con.execute("SELECT type FROM sqlite_master WHERE name = 'assets'").fetchone()

    assert kind == ("view",)


def test_writing_through_the_view_is_refused() -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        with pytest.raises(sqlite3.OperationalError):
            con.execute(
                "INSERT INTO assets (id, name, layer, storage_uri, format, record_count,"
                " created_at) VALUES ('x', 'x', 'raw', 'x', 'parquet', 0,"
                " CURRENT_TIMESTAMP)"
            )


def test_the_view_projects_what_a_write_recorded() -> None:
    layer = f"zz{uuid.uuid4().hex[:6]}"
    name = f"probe_{uuid.uuid4().hex[:4]}"

    try:
        obelisk.save(pl.DataFrame({"id": [1, 2, 3]}), f"{layer}.{name}", sync_postgres=False)

        with sqlite3.connect(settings.sqlite_db_path) as con:
            row = con.execute(
                "SELECT name, layer, record_count, format FROM assets WHERE name = ?",
                (name,),
            ).fetchone()

        assert row == (name, layer, 3, "parquet")
    finally:
        _cleanup(layer, name)


def test_external_sources_do_not_appear_as_lakehouse_assets() -> None:
    """A shortcut is a live pointer, not a file: the lakehouse view must not list it."""
    fqn = f"probe.{uuid.uuid4().hex[:6]}.orders"
    try:
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute(
                "INSERT INTO catalog_objects (id, fqn, name, kind, backend, layer_path,"
                " version, created_at) VALUES (?, ?, 'orders', 'shortcut', 'postgres',"
                " NULL, 1, CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), fqn),
            )
            con.commit()
            visible = con.execute(
                "SELECT COUNT(*) FROM assets WHERE name = 'orders' AND layer IS NULL"
            ).fetchone()[0]

        assert visible == 0
    finally:
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM catalog_objects WHERE fqn = ?", (fqn,))
            con.commit()


def test_model_rows_keep_the_old_postgres_format_label() -> None:
    """Callers that switched on format == 'postgres_table' still see it."""
    with sqlite3.connect(settings.sqlite_db_path) as con:
        formats = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT format FROM assets WHERE format IS NOT NULL"
            )
        }
    assert formats <= {"parquet", "postgres_table", "external"}
