"""A native query result is capped, and says so.

The DuckDB path capped interactive results at ``DUCKDB_MAX_QUERY_ROWS``; the Postgres and
MySQL paths did not. A ``SELECT *`` against a wide table therefore built one Python dict
per row of the *entire* result — the response model then dropped nothing, and the process
died before the UI could render anything. The frontend already understands ``truncated``
(the grid says "showing first N rows"), it just never received the flag from these paths.

``_fetch_bounded`` is the shared reader. The DB-API fetch protocol it relies on
(``fetchmany(n)`` returning at most ``n`` rows) is exercised here against a real cursor,
and the Postgres path is checked end to end when a real server is reachable.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.engines.native_query_engine import NativeQueryEngine, _fetch_bounded


def _cursor_with(rows: int) -> sqlite3.Cursor:
    """A real DB-API cursor over a real table, holding ``rows`` rows."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (n INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(rows)])
    cur = conn.cursor()
    cur.execute("SELECT n FROM t ORDER BY n")
    return cur


def test_fetch_bounded_stops_at_the_cap_and_reports_it() -> None:
    rows, truncated = _fetch_bounded(_cursor_with(25), 10)

    assert len(rows) == 10
    assert truncated is True, "more rows existed, so the caller must be told"


def test_fetch_bounded_does_not_lie_when_the_result_fits_exactly() -> None:
    """Exactly ``max_rows`` rows is not truncated — the cap is not "more than N"."""
    rows, truncated = _fetch_bounded(_cursor_with(10), 10)

    assert len(rows) == 10
    assert truncated is False


def test_fetch_bounded_reads_everything_when_no_cap_is_given() -> None:
    """No cap keeps the old behaviour for callers that genuinely want it all."""
    rows, truncated = _fetch_bounded(_cursor_with(25), None)

    assert len(rows) == 25
    assert truncated is False


def test_a_zero_cap_means_no_cap() -> None:
    """``DUCKDB_MAX_QUERY_ROWS=0`` disables the cap on the DuckDB path; keep that parity."""
    rows, truncated = _fetch_bounded(_cursor_with(5), 0)

    assert len(rows) == 5
    assert truncated is False


def test_a_postgres_result_is_capped_and_flagged(source_postgres_params) -> None:
    """End to end against a real server: 50 rows requested, 10 returned, truncated set."""
    p = source_postgres_params

    res = NativeQueryEngine.execute_postgres(
        host=p["host"],
        port=p["port"],
        database=p["database"],
        username=p["user"],
        password=p["password"],
        sql="SELECT g AS n FROM generate_series(1, 50) AS g",
        timeout_seconds=10,
        sslmode=p.get("sslmode", "disable"),
        max_rows=10,
    )

    assert res["status"] == "success", res
    assert res["row_count"] == 10
    assert len(res["rows"]) == 10
    assert res["truncated"] is True
