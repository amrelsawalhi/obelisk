"""What an ingest must leave behind when something goes wrong.

These pin the ways a run used to look successful while losing or corrupting data: a
watermark read that failed and was treated as "nothing stored yet", an incremental
append that half-committed into the live table, a mixed-type batch that stringified
every numeric column, an Oracle LOB that arrived as its repr, and statement detection
that mistook a semicolon inside a literal for a second statement.
"""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from app.core.catalog import LOAD_TABLE_SUFFIX, quote_ident
from app.core.security import is_loopback_client
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine, _normalize_cell, _order_by
from app.engines.native_query_engine import _has_multiple_statements


@pytest.fixture()
def lake():
    """A uniquely named schema in the test lakehouse, removed afterwards."""
    engine = IngestEngine()
    schema = f"zz{uuid.uuid4().hex[:6]}"
    conn = duckdb_engine.get_connection()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    try:
        yield engine, conn, schema
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def test_a_watermark_that_cannot_be_read_fails_the_run(lake) -> None:
    """``None`` means "no rows yet", never "the read failed".

    An unreadable watermark used to come back as ``None``, which an incremental append
    reads as "the table is empty" and answers by copying the whole source in again.
    """
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id, 10 AS updated_at")

    assert engine._max_watermark(conn, target, "updated_at") == 10

    with pytest.raises(RuntimeError, match="watermark"):
        engine._max_watermark(conn, target, "no_such_column")


def test_a_failed_append_leaves_the_target_exactly_as_it_was(lake) -> None:
    """The delta is applied in one transaction, so a bad batch changes nothing."""
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    staged = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id, 10 AS amount")
    conn.execute(f"CREATE TABLE {staged} AS SELECT 2 AS order_id, 'not-a-number' AS amount")

    with pytest.raises(Exception):
        engine._append_from_load(
            conn, schema=schema, staged=staged, target=target, table="orders"
        )

    assert conn.execute(f"SELECT count(*) FROM {target}").fetchone()[0] == 1
    assert conn.execute(f"SELECT sum(amount) FROM {target}").fetchone()[0] == 10
    assert engine._exists(conn, schema, f"orders{LOAD_TABLE_SUFFIX}"), (
        "a rolled-back append must not have dropped the stage either"
    )


def test_an_append_from_a_load_table_commits_and_removes_the_stage(lake) -> None:
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    staged = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id, 10 AS amount")
    conn.execute(f"CREATE TABLE {staged} AS SELECT 2 AS order_id, 20 AS amount")

    engine._append_from_load(
        conn, schema=schema, staged=staged, target=target, table="orders"
    )

    rows = conn.execute(f"SELECT order_id FROM {target} ORDER BY order_id").fetchall()
    assert [row[0] for row in rows] == [1, 2]
    assert not engine._exists(conn, schema, f"orders{LOAD_TABLE_SUFFIX}")



def test_one_drifting_column_does_not_stringify_the_others(lake) -> None:
    """Only the column whose type actually changed is widened, and only for the merge."""
    engine, conn, schema = lake
    sink = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    first = pa.table(
        {
            "order_id": pa.array([1], pa.int64()),
            "amount": pa.array([1.5], pa.float64()),
            "note": pa.array(["text"], pa.string()),
        }
    )
    second = pa.table(
        {
            "order_id": pa.array([2], pa.int64()),
            "amount": pa.array([2.5], pa.float64()),
            "note": pa.array([{"a": 1}], pa.struct([("a", pa.int64())])),
        }
    )

    engine._stage_batches(conn, [first, second], sink=sink, create=True, commit_rows=10)

    described = {
        str(row[0]): str(row[1]).upper()
        for row in conn.execute(f"DESCRIBE {sink}").fetchall()
    }
    assert described["amount"] == "DOUBLE", "a drifting column must not turn amounts into text"
    assert described["order_id"] == "BIGINT"
    assert described["note"] in ("VARCHAR", "STRING", "LARGE_STRING")
    assert conn.execute(f"SELECT count(*) FROM {sink}").fetchone()[0] == 2


def test_deduping_is_narrow_and_deterministic(lake) -> None:
    """The winner is picked from a narrow ranking, and a re-run picks the same one."""
    engine, conn, schema = lake
    staged = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(
        f"CREATE TABLE {staged} AS SELECT * FROM (VALUES "
        "(1, 10, 100), (1, 11, 200), (2, 20, NULL), (2, 21, NULL)"
        ") v(order_id, amount, updated_at)"
    )

    sql = engine._deduped("order_id", "updated_at", relation=staged)

    assert "amount" not in sql, "the ranking must not read payload columns"
    first = conn.execute(sql).fetchall()
    second = conn.execute(sql).fetchall()
    assert sorted(first) == sorted(second), "the same staged rows must pick the same winners"
    assert dict((row[0], row[1]) for row in first) == {1: 11, 2: 21}


class LOB:
    """Stands in for ``oracledb.LOB``: a locator whose value is behind ``read()``."""

    def __init__(self, value: str) -> None:
        self._value = value

    def read(self) -> str:
        return self._value


def test_an_oracle_lob_is_read_rather_than_stringified() -> None:
    assert _normalize_cell(LOB("clob text")) == "clob text"


def test_a_semicolon_inside_a_literal_is_not_a_second_statement() -> None:
    """The named server cursor must stay available for a query like ``LIKE '%;%'``."""
    assert _has_multiple_statements("SELECT * FROM t WHERE note LIKE '%;%'") is False
    assert _has_multiple_statements("SELECT ';' AS semicolon;") is False
    assert _has_multiple_statements("SELECT 1;   ") is False
    assert _has_multiple_statements("-- a; comment\nSELECT 1") is False
    assert _has_multiple_statements("DO $$ BEGIN PERFORM 1; END $$;") is False
    assert _has_multiple_statements("SELECT 1; SELECT 2") is True
    assert _has_multiple_statements("SELECT 1; DROP TABLE t") is True


def test_a_bounded_incremental_read_is_ordered_by_its_watermark() -> None:
    """A ``LIMIT`` without ``ORDER BY`` is not a prefix of history."""
    assert _order_by("updated_at", 100, lambda name: f"[{name}]") == " ORDER BY [updated_at] ASC"
    assert _order_by("updated_at", 100, quote_ident) == " ORDER BY updated_at ASC"
    assert _order_by("updated_at", None, quote_ident) == ""
    assert _order_by(None, 100, quote_ident) == ""


def test_only_loopback_clients_open_an_unauthenticated_terminal() -> None:
    assert is_loopback_client("127.0.0.1") is True
    assert is_loopback_client("::1") is True
    assert is_loopback_client("::ffff:127.0.0.1") is True
    assert is_loopback_client("10.1.2.3") is False
    assert is_loopback_client("testclient") is False
    assert is_loopback_client(None) is False
