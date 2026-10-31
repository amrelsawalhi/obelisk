"""The cheap-query path: classification, the light pool, and writer exclusion.

The benchmark measured a ~150 ms point lookup taking 4.4 s while a dbt build held the
whole heavy pool. These tests pin the two mechanisms that fixed it: a conservative
classifier that routes only provably-cheap statements to a separate pool, and a writer
lock that serializes lakehouse writers without blocking readers.
"""
from __future__ import annotations

import threading
import time

import pytest

from app.config import settings
from app.engines.duckdb_engine import classify_light_query, duckdb_engine


# --------------------------------------------------------------- classification

@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM fact_orders LIMIT 100",
        "select order_id, amount from fact_orders where order_id = 42 limit 1",
        "SELECT * FROM fact_orders WHERE status = 'paid' LIMIT 25;",
        "EXPLAIN SELECT * FROM fact_orders",
        "DESCRIBE SELECT * FROM fact_orders",
        "SHOW TABLES",
        "SUMMARIZE fact_orders",
        "PRAGMA table_info('fact_orders')",
        # A keyword inside a string literal must not demote the query.
        "SELECT 'GROUP BY' AS joke, 'JOIN' AS b FROM fact_orders LIMIT 5",
        # ...nor inside a comment.
        "-- this used to be a JOIN\nSELECT * FROM fact_orders LIMIT 5",
    ],
)
def test_provably_cheap_queries_are_light(sql: str) -> None:
    assert classify_light_query(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM fact_orders",  # no LIMIT: a full scan
        "SELECT * FROM fact_orders LIMIT 5001",  # above the cap
        "SELECT * FROM a JOIN b ON a.id = b.id LIMIT 5",
        "SELECT * FROM a, b LIMIT 5",  # implicit comma join
        "SELECT status, count(*) FROM fact_orders GROUP BY status LIMIT 10",
        "SELECT * FROM fact_orders ORDER BY amount LIMIT 10",
        "SELECT DISTINCT status FROM fact_orders LIMIT 10",
        "SELECT sum(amount) FROM fact_orders LIMIT 1",
        "SELECT *, row_number() OVER () FROM fact_orders LIMIT 5",
        "SELECT * FROM (SELECT * FROM fact_orders) LIMIT 5",
        "WITH x AS (SELECT 1 AS a) SELECT * FROM x LIMIT 1",
        "SELECT 1 AS a UNION ALL SELECT 2 LIMIT 5",
        "SELECT * FROM a LIMIT 5; SELECT * FROM b LIMIT 5",  # two statements
        "INSERT INTO t SELECT * FROM fact_orders LIMIT 5",
        "DELETE FROM fact_orders",
        "CREATE TABLE t AS SELECT 1",
        "SELECT * FROM t LIMIT ?",
        "",
        None,
    ],
)
def test_everything_else_stays_heavy(sql) -> None:
    assert classify_light_query(sql) is False


# --------------------------------------------------------------- the light pool

def test_a_light_query_runs_while_every_heavy_permit_is_held() -> None:
    """The starved point lookup from the benchmark: heavy pool full, lookups still run."""
    slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
    held = duckdb_engine.begin_heavy(slots, kind="dbt")
    try:
        started = time.monotonic()
        result = duckdb_engine.execute_query("SELECT 1 AS ok LIMIT 1", light=True)
        elapsed = time.monotonic() - started
    finally:
        duckdb_engine.end_heavy(held)

    assert result["rows"] == [{"ok": 1}]
    assert elapsed < 10.0, "a light query must not wait for the heavy pool"


def test_light_and_heavy_permits_are_counted_separately() -> None:
    telemetry = duckdb_engine.get_telemetry()
    assert telemetry["max_concurrent_light"] >= 1
    assert telemetry["max_concurrent_light"] <= max(
        1, telemetry["max_concurrent_heavy"]
    )

    before = duckdb_engine.get_telemetry()
    with duckdb_engine.heavy_slot(kind="query_light", light=True):
        during = duckdb_engine.get_telemetry()
        assert during["active_light_queries"] == before["active_light_queries"] + 1
        assert during["active_heavy_queries"] == before["active_heavy_queries"]
    assert (
        duckdb_engine.get_telemetry()["active_light_queries"]
        == before["active_light_queries"]
    )


def test_the_classifier_selects_the_pool_for_execute_query() -> None:
    """A classified-light statement executes normally through the public entry point."""
    result = duckdb_engine.execute_query("SELECT 5 AS v LIMIT 5")
    assert result["rows"] == [{"v": 5}]


# --------------------------------------------------------------- writer exclusion

def test_writers_serialize_and_a_reader_does_not_wait_for_the_lock() -> None:
    order: list[str] = []
    first_inside = threading.Event()
    release_first = threading.Event()

    def writer(name: str, hold: threading.Event | None = None) -> None:
        reserved = duckdb_engine.begin_heavy(1, kind="ingest")
        try:
            order.append(name)
            if hold is not None:
                hold.set()
                release_first.wait(15)
        finally:
            duckdb_engine.end_heavy(reserved)

    first = threading.Thread(target=writer, args=("first",), kwargs={"hold": first_inside})
    second = threading.Thread(target=writer, args=("second",))
    first.start()
    assert first_inside.wait(15), "the first writer never started"
    second.start()

    # The second writer is blocked on the lock while the first holds it.
    time.sleep(0.2)
    assert order == ["first"], "two lakehouse writers must never overlap"

    # A reader takes no writer lock, so it runs beside the writer.
    reader_ok = threading.Event()

    def reader() -> None:
        result = duckdb_engine.execute_query("SELECT 1 AS ok LIMIT 1", light=True)
        if result["rows"] == [{"ok": 1}]:
            reader_ok.set()

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    assert reader_ok.wait(15), "a reader must run beside a writer"

    release_first.set()
    first.join(20)
    second.join(20)
    reader_thread.join(20)
    assert order == ["first", "second"]
