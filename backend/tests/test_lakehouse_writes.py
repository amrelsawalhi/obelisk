"""Writing to the lakehouse: replace, append, merge, and what the catalog then says.

These replace the old merge-on-write tests. The mechanism changed (SQL instead of Arrow
concatenation), so the tests assert the *outcomes* users depend on — the newest row wins,
a row with no timestamp never displaces a real one, and the table appears in the catalog
without anything being registered by hand.
"""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from app.core.catalog import scan
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine


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


def _batch(ids, amounts, stamps):
    return pa.table(
        {
            "order_id": ids,
            "amount": amounts,
            "updated_at": pa.array(stamps, pa.int64()),
        }
    )


def _write(engine, conn, schema, batch, mode, strategy="append", key=None, watermark=None):
    """Streams one batch into the sink the strategy picks, then applies it.

    That is the path an ingest takes: every strategy streams into a load table beside
    the target, and only then is the delta swapped in (snapshot, first load), appended
    in one transaction (incremental append) or merged (upsert).
    """
    engine._require_primary_key(strategy, key)
    target = f"lakehouse.{schema}.orders"
    exists = mode != "snapshot"
    sink, create, load = engine._batch_sink(
        schema=schema,
        table="orders",
        target=target,
        mode=mode,
        exists=exists,
        write_strategy=strategy,
    )
    assert engine._stage_batches(conn, [batch], sink=sink, create=create) == batch.num_rows
    assert load is not None, "a batched load always stages beside its target"
    if strategy == "upsert":
        engine._upsert_from_load(
            conn,
            schema=schema,
            staged=sink,
            target=target,
            table="orders",
            mode=mode,
            exists=exists,
            primary_key=key,
            watermark_column=watermark,
        )
    elif mode == "incremental" and exists:
        engine._append_from_load(
            conn, schema=schema, staged=sink, target=target, table="orders"
        )
    else:
        engine._swap_into_place(
            conn, schema=schema, staged=sink, target=target, table="orders"
        )


def test_snapshot_writes_every_row(lake):
    engine, conn, schema = lake
    _write(engine, conn, schema, _batch([1, 2], [10.0, 20.0], [1, 2]), "snapshot")
    assert conn.execute(f"SELECT count(*) FROM lakehouse.{schema}.orders").fetchone()[0] == 2


def test_incremental_append_keeps_history(lake):
    engine, conn, schema = lake
    _write(engine, conn, schema, _batch([1], [10.0], [1]), "snapshot")
    _write(engine, conn, schema, _batch([2], [20.0], [2]), "incremental")
    rows = conn.execute(
        f"SELECT order_id FROM lakehouse.{schema}.orders ORDER BY order_id"
    ).fetchall()
    assert [row[0] for row in rows] == [1, 2]


def test_upsert_updates_and_inserts(lake):
    engine, conn, schema = lake
    _write(engine, conn, schema, _batch([1, 2], [10.0, 20.0], [1, 2]), "snapshot")
    _write(
        engine, conn, schema, _batch([2, 3], [99.0, 30.0], [10, 10]),
        "incremental", strategy="upsert", key="order_id", watermark="updated_at",
    )
    rows = dict(
        conn.execute(f"SELECT order_id, amount FROM lakehouse.{schema}.orders").fetchall()
    )
    assert rows == {1: 10.0, 2: 99.0, 3: 30.0}


def test_a_row_without_a_watermark_never_overwrites_a_real_one(lake):
    """Audit F-02: the old merge let a NULL timestamp win. The guard lives in the SQL."""
    engine, conn, schema = lake
    _write(engine, conn, schema, _batch([1], [10.0], [5]), "snapshot")
    _write(
        engine, conn, schema, _batch([1], [0.0], [None]),
        "incremental", strategy="upsert", key="order_id", watermark="updated_at",
    )
    kept = conn.execute(f"SELECT amount FROM lakehouse.{schema}.orders").fetchone()[0]
    assert kept == 10.0, "a missing watermark must not displace a real row"


def test_upsert_requires_a_primary_key(lake):
    engine, conn, schema = lake
    with pytest.raises(ValueError, match="primary_key"):
        _write(
            engine, conn, schema, _batch([1], [10.0], [1]),
            "snapshot", strategy="upsert", key=None,
        )


def test_the_catalog_sees_the_table_without_registering_it(lake):
    engine, conn, schema = lake
    _write(engine, conn, schema, _batch([1, 2, 3], [10.0, 20.0, 30.0], [1, 2, 3]), "snapshot")

    result = scan()
    match = next((t for t in result.tables if t.fqn == f"lakehouse.{schema}.orders"), None)
    assert match is not None, "the written table must appear in the catalog"
    assert match.num_rows == 3
    # Byte size is not asserted: a small write may be *inlined* in the catalog rather
    # than written to Parquet, which is exactly why data_inlining_row_limit is a decision
    # (see docs/ducklake_migration_plan.md) instead of an implementation detail.
    assert match.size_bytes >= 0
    assert [c["name"] for c in match.columns] == ["order_id", "amount", "updated_at"]
