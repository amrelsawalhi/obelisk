"""Reporting a load's row count must not scan the table again.

After every load the engine asked the table ``count(*)`` — for a 100 M-row ingest that is a
second full pass over the Parquet, paid only to fill in a number the response prints. The
append path asked twice (before and after, to diff them).

DuckLake records each data file's ``record_count``, so the answer is already in the
catalog. These tests check the two agree on a real lakehouse table, and that the fallback
still exists for tables the catalog cannot describe.
"""
from __future__ import annotations

import uuid

import pytest

from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine


@pytest.fixture()
def table_in_the_lake():
    """A real lakehouse table with rows, in a schema of its own."""
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    schema = f"zzrows{uuid.uuid4().hex[:6]}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    target = engine._target_sql(schema, "counted")
    conn.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT range AS n FROM range(1234)")
    try:
        yield engine, conn, target
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def test_the_catalog_knows_the_row_count(table_in_the_lake):
    """The statistic and the scan agree — the fast one is not a different answer."""
    engine, conn, target = table_in_the_lake

    from_catalog = engine._catalog_row_count(conn, target)
    from_scan = int(conn.execute(f"SELECT count(*) FROM {target}").fetchall()[0][0])

    assert from_catalog == 1234
    assert from_catalog == from_scan


def test_the_row_count_helper_uses_the_catalog(table_in_the_lake):
    """``_row_count`` returns the real number for a lakehouse table."""
    engine, conn, target = table_in_the_lake

    assert engine._row_count(conn, target) == 1234


def test_an_empty_table_falls_back_instead_of_claiming_zero():
    """Zero rows and "DuckLake kept them inline" look identical; the count decides."""
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    schema = f"zzrows{uuid.uuid4().hex[:6]}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    target = engine._target_sql(schema, "empty")
    conn.execute(f"CREATE TABLE {target} (n INTEGER)")
    try:
        assert engine._catalog_row_count(conn, target) is None
        assert engine._row_count(conn, target) == 0
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def test_a_non_lakehouse_relation_is_counted_normally():
    """A plain DuckDB table (a test's staging area) has no catalog entry to read."""
    engine = IngestEngine()
    conn = duckdb_engine.get_connection()
    try:
        conn.execute("CREATE OR REPLACE TEMP TABLE zz_not_lake AS SELECT range AS n FROM range(7)")
        assert engine._catalog_row_count(conn, "zz_not_lake") is None
        assert engine._row_count(conn, "zz_not_lake") == 7
    finally:
        conn.execute("DROP TABLE IF EXISTS zz_not_lake")
        conn.close()
