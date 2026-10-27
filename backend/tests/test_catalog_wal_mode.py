"""The DuckLake catalog file must not serialise every reader behind one writer.

The catalog is a plain sqlite database that DuckLake opens itself, so its journal mode is
whatever the file says. In the default rollback-journal mode a write (a dbt run, an ingest)
blocks readers (Query Studio attaching) and the wait surfaces as "database is locked"; in
WAL they proceed together, and the file keeps that mode for every future connection.

WAL is persistent in the file header, which is why it can be set once, here, and why these
tests can check it after the fact.
"""
from __future__ import annotations

import sqlite3

from app.config import settings
from app.core.catalog import lakehouse_connection


def test_attaching_puts_the_catalog_file_in_wal_mode():
    conn = lakehouse_connection()  # creates the catalog on first use
    try:
        conn.execute("SELECT 1").fetchall()
    finally:
        conn.close()

    with sqlite3.connect(settings.lakehouse_catalog_path) as con:
        mode = con.execute("PRAGMA journal_mode;").fetchone()[0]

    assert str(mode).lower() == "wal", (
        "without WAL every catalog reader queues behind the current writer"
    )


def test_a_second_connection_can_read_while_the_first_holds_the_catalog():
    """Two attaches at once: the second is not refused, and sees a table the first wrote."""
    reader = lakehouse_connection()
    writer = lakehouse_connection()
    try:
        writer.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.zzwal")
        writer.execute("CREATE OR REPLACE TABLE lakehouse.zzwal.t AS SELECT 1 AS n")

        rows = reader.execute("SELECT count(*) FROM lakehouse.zzwal.t").fetchall()

        assert rows[0][0] == 1, "a reader held the catalog while the writer committed"
    finally:
        try:
            writer.execute("DROP SCHEMA IF EXISTS lakehouse.zzwal CASCADE")
        finally:
            writer.close()
            reader.close()
