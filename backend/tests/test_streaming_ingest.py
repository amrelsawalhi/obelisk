"""Streaming ingestion: rows land in the lake as they arrive, not at the end.

A load used to collect every row of every source in ``TEMP TABLE staging`` before one
statement wrote it, so the ceiling on a table was ``DUCKDB_TEMP_SIZE_LIMIT`` (50 GB by
default) rather than the machine's disk. These tests pin the paths that replaced it — a
chunked write into the lake, a direct DuckDB-to-DuckLake transfer, the swap that keeps a
snapshot atomic while it is being written, and an upsert that deduplicates in a load table
instead of the temp directory.

Every case is real: real DuckLake tables for a schema, a real HTTP server for the REST
driver, and Arrow batches built from real rows.
"""
from __future__ import annotations

import importlib.util
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.core.catalog import LOAD_TABLE_SUFFIX, scan
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine, _arrow_batch, _cells
from app.main import app

MAINTENANCE_SCRIPT = (
    Path(__file__).resolve().parent.parent / "scripts" / "lakehouse_maintenance.py"
)


def _maintenance():
    """The maintenance script by path: it is a script, not a module of the package."""
    spec = importlib.util.spec_from_file_location("lakehouse_maintenance", MAINTENANCE_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


@pytest.fixture()
def json_api():
    """A real HTTP endpoint whose payload a test can change between two loads."""

    class Handler(BaseHTTPRequestHandler):
        payload: list[dict] = []

        def do_GET(self) -> None:  # noqa: N802 - the name is the HTTP method
            body = json.dumps(Handler.payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, Handler
    finally:
        server.shutdown()
        server.server_close()


def _rows(conn, relation: str) -> int:
    return int(conn.execute(f"SELECT count(*) FROM {relation}").fetchall()[0][0])


def _live_tables(conn, schema: str) -> list[str]:
    """The table names the catalog reports as live in one schema."""
    rows = conn.execute(
        "SELECT t.table_name "
        "FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
        "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s "
        "  ON s.schema_id = t.schema_id "
        "WHERE t.end_snapshot IS NULL AND s.schema_name = ?",
        [schema],
    ).fetchall()
    return sorted(str(row[0]) for row in rows)


def _snapshots(conn) -> int:
    """How many snapshots the catalog holds. Read with fetchall: fetchone leaves the
    result open on the connection, and a later ``register`` is refused while it is."""
    return int(
        conn.execute("SELECT count(*) FROM ducklake_snapshots('lakehouse')").fetchall()[0][0]
    )


def _temp_staging_exists(conn) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'staging'"
        ).fetchall()[0][0]
    )



# ---------------------------------------------------------------------------
# 2.2 — building a batch, and where batches go
# ---------------------------------------------------------------------------


def test_arrow_batch_transposes_rows_into_typed_columns() -> None:
    """Rows go in as tuples and come out as columns, with their types intact."""
    batch = _arrow_batch(["order_id", "amount"], [(1, 2.5), (2, 3.5)])

    assert batch.column_names == ["order_id", "amount"]
    assert batch.to_pydict() == {"order_id": [1, 2], "amount": [2.5, 3.5]}
    assert batch.schema.field("order_id").type == pa.int64()
    assert batch.schema.field("amount").type == pa.float64()


def test_arrow_batch_without_rows_keeps_the_columns() -> None:
    """An empty batch still describes the table it would have filled."""
    batch = _arrow_batch(["order_id", "amount"], [])

    assert batch.num_rows == 0
    assert batch.column_names == ["order_id", "amount"]


def test_cells_turn_nested_values_into_json_text() -> None:
    """A driver that returns a dict or a list gets text, so the column stays queryable."""
    assert _cells((1, {"a": 1}, [1, 2], "x")) == (1, '{"a": 1}', "[1, 2]", "x")


def test_batches_stream_into_the_lake_instead_of_the_temp_table(lake) -> None:
    """A sink writes Parquet; no temp table is created on the way."""
    engine, conn, schema = lake
    load = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    batches = [pa.table({"order_id": [n], "amount": [1.0]}) for n in range(1, 4)]

    assert engine._stage_batches(conn, batches, sink=load, create=True) == 3

    assert _rows(conn, load) == 3
    assert _temp_staging_exists(conn) is False


def test_chunks_are_committed_together_not_once_per_batch(lake) -> None:
    """Commits are grouped, so a long load does not leave thousands of files behind.

    The first batch creates the table (one commit). Six batches of ten rows, committed
    every twenty rows, need three more: 10+10, 10+10, then the tail of 10.
    """
    engine, conn, schema = lake
    load = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    batches = [pa.table({"n": list(range(10))}) for _ in range(6)]
    before = _snapshots(conn)

    engine._stage_batches(conn, batches, sink=load, create=True, commit_rows=20)

    assert _rows(conn, load) == 60
    assert _snapshots(conn) - before == 4


def test_the_sink_follows_the_mode_and_the_write_strategy(lake) -> None:
    """An upsert and a snapshot load beside their target; an append goes straight in."""
    engine, _conn, schema = lake
    target = engine._target_sql(schema, "orders")
    load = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")

    assert engine._batch_sink(
        schema=schema, table="orders", target=target,
        mode="snapshot", exists=True, write_strategy="upsert",
    ) == (load, True, f"orders{LOAD_TABLE_SUFFIX}")

    assert engine._batch_sink(
        schema=schema, table="orders", target=target,
        mode="incremental", exists=True, write_strategy="upsert",
    ) == (load, True, f"orders{LOAD_TABLE_SUFFIX}")

    assert engine._batch_sink(
        schema=schema, table="orders", target=target,
        mode="snapshot", exists=True, write_strategy="append",
    ) == (load, True, f"orders{LOAD_TABLE_SUFFIX}")

    assert engine._batch_sink(
        schema=schema, table="orders", target=target,
        mode="incremental", exists=False, write_strategy="append",
    ) == (load, True, f"orders{LOAD_TABLE_SUFFIX}")

    assert engine._batch_sink(
        schema=schema, table="orders", target=target,
        mode="incremental", exists=True, write_strategy="append",
    ) == (target, False, None)


# ---------------------------------------------------------------------------
# the swap that makes a streamed snapshot atomic
# ---------------------------------------------------------------------------


def test_the_swap_replaces_the_table_and_consumes_the_load(lake) -> None:
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    load = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id")
    conn.execute(f"CREATE TABLE {load} AS SELECT 2 AS order_id")

    engine._swap_into_place(
        conn, schema=schema, staged=load, target=target, table="orders"
    )

    assert conn.execute(f"SELECT order_id FROM {target}").fetchall() == [(2,)]
    assert _live_tables(conn, schema) == ["orders"]


def test_a_failed_swap_leaves_the_previous_table_exactly_as_it_was(lake) -> None:
    """The drop happens inside the transaction, so a missing load table changes nothing."""
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id")

    with pytest.raises(Exception):
        engine._swap_into_place(
            conn,
            schema=schema,
            staged=engine._target_sql(schema, f"nope{LOAD_TABLE_SUFFIX}"),
            target=target,
            table="orders",
        )

    assert conn.execute(f"SELECT order_id FROM {target}").fetchall() == [(1,)]


def test_the_catalog_never_offers_a_table_that_is_still_loading(lake) -> None:
    engine, conn, schema = lake
    conn.execute(
        f"CREATE TABLE {engine._target_sql(schema, f'orders{LOAD_TABLE_SUFFIX}')} "
        "AS SELECT 1 AS order_id"
    )

    assert [t.name for t in scan().tables if t.folder == schema] == []


# ---------------------------------------------------------------------------
# upsert: deduplication happens in the lake, not in the temp directory
# ---------------------------------------------------------------------------


def test_a_first_upsert_dedupes_the_load_table_and_swaps_it_in(lake, json_api) -> None:
    """No target yet: one row per key, newest watermark wins, and it is swapped in."""
    engine, conn, schema = lake
    server, handler = json_api
    target = engine._target_sql(schema, "orders")
    handler.payload = [
        {"id": 1, "amount": 10.0, "updated_at": 100},
        {"id": 1, "amount": 11.0, "updated_at": 200},
        {"id": 2, "amount": 20.0, "updated_at": 100},
    ]

    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        write_strategy="upsert",
        primary_key="id",
        watermark_column="updated_at",
        target_folder=schema,
    )

    assert result["num_rows"] == 2, "three rows in, two keys out"
    assert dict(conn.execute(f"SELECT id, amount FROM {target}").fetchall()) == {
        1: 11.0,
        2: 20.0,
    }
    assert _live_tables(conn, schema) == ["orders"], "the load table must not survive"
    assert _temp_staging_exists(conn) is False


def test_a_leftover_load_table_from_a_killed_run_does_not_poison_the_next_one(
    lake, json_api
) -> None:
    """A retry owns the load table name, whatever the last attempt left in it."""
    engine, conn, schema = lake
    server, handler = json_api
    target = engine._target_sql(schema, "orders")
    leftover = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    # What an unclean kill cannot clean up: a load table still holding its rows, with a
    # column this load's source does not have.
    conn.execute(
        f"CREATE TABLE {leftover} AS SELECT 999 AS id, -1.0 AS amount, 1 AS updated_at, "
        "'junk' AS leftover_column"
    )
    handler.payload = [{"id": 1, "amount": 10.0, "updated_at": 100}]

    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        write_strategy="upsert",
        primary_key="id",
        watermark_column="updated_at",
        target_folder=schema,
    )

    assert result["num_rows"] == 1
    assert conn.execute(f"SELECT id, amount FROM {target}").fetchall() == [(1, 10.0)]
    assert engine._columns(conn, target) == ["amount", "id", "updated_at"], (
        "the swapped-in table describes this load, not the one that was killed"
    )
    assert _live_tables(conn, schema) == ["orders"]


def test_an_empty_source_still_clears_a_leftover_load_table(lake, json_api) -> None:
    """The drop is the run's first act, so it does not wait for a batch to arrive."""
    engine, conn, schema = lake
    server, handler = json_api
    target = engine._target_sql(schema, "orders")
    leftover = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(
        f"CREATE TABLE {target} AS SELECT 1 AS id, 10.0 AS amount, 100 AS updated_at"
    )
    conn.execute(
        f"CREATE TABLE {leftover} AS SELECT 999 AS id, -1.0 AS amount, 1 AS updated_at"
    )
    handler.payload = []

    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        write_strategy="upsert",
        primary_key="id",
        target_folder=schema,
    )

    assert result["num_rows"] == 1, "an empty source keeps the table it would have replaced"
    assert _live_tables(conn, schema) == ["orders"], "and the leftover load table still goes"


# ---------------------------------------------------------------------------
# 2.1 — one statement from a source DuckDB can read to the lakehouse
# ---------------------------------------------------------------------------


def test_a_direct_transfer_replaces_then_appends(lake) -> None:
    """The two shapes the Postgres driver uses, against a real source relation."""
    engine, conn, schema = lake
    source = engine._target_sql(schema, "source_orders")
    target = engine._target_sql(schema, "orders")
    conn.execute(
        f"CREATE TABLE {source} AS "
        "SELECT * FROM (VALUES (1, 10.0), (2, 20.0)) AS t(order_id, amount)"
    )

    written = engine._transfer_from(
        conn=conn, source_ref=source, target=target, mode="snapshot", exists=False
    )
    assert written == 2
    assert _rows(conn, target) == 2
    assert _temp_staging_exists(conn) is False

    appended = engine._transfer_from(
        conn=conn, source_ref=source, target=target, mode="incremental", exists=True
    )
    assert appended == 2
    assert _rows(conn, target) == 4


def test_a_direct_transfer_refuses_to_replace_with_an_empty_source(lake) -> None:
    """An empty source keeps the table it would have destroyed."""
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id, 2.0 AS amount")
    empty = engine._target_sql(schema, "empty_source")
    conn.execute(f"CREATE TABLE {empty} AS SELECT * FROM {target} WHERE 1 = 0")

    written = engine._transfer_from(
        conn=conn, source_ref=empty, target=target, mode="snapshot", exists=True
    )

    assert written == 0
    assert _rows(conn, target) == 1



# ---------------------------------------------------------------------------
# end to end, through the REST driver
# ---------------------------------------------------------------------------


def _api_params(server) -> dict:
    return {"host": "127.0.0.1", "port": server.server_address[1], "options": {}}


def test_an_api_snapshot_streams_through_a_load_table_and_swaps(lake, json_api) -> None:
    engine, conn, schema = lake
    server, handler = json_api
    handler.payload = [
        {"id": 1, "amount": 10.0, "updated_at": 100},
        {"id": 2, "amount": 20.0, "updated_at": 200},
        {"id": 3, "amount": 30.0, "updated_at": 300},
    ]

    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        target_folder=schema,
    )

    assert result["num_rows"] == 3
    assert result["new_rows_extracted"] == 3
    assert _live_tables(conn, schema) == ["orders"], "the load table must not survive"
    assert conn.execute(
        f"SELECT sum(amount) FROM {engine._target_sql(schema, 'orders')}"
    ).fetchone()[0] == 60.0


def test_an_incremental_append_lands_in_the_target_and_reads_its_watermark(
    lake, json_api
) -> None:
    engine, conn, schema = lake
    server, handler = json_api
    target = engine._target_sql(schema, "orders")
    handler.payload = [{"id": 1, "amount": 10.0, "updated_at": 100}]
    engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        target_folder=schema,
    )

    handler.payload = [
        {"id": 1, "amount": 10.0, "updated_at": 100},
        {"id": 2, "amount": 20.0, "updated_at": 200},
        {"id": 3, "amount": 30.0, "updated_at": 300},
    ]
    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="incremental",
        watermark_column="updated_at",
        target_folder=schema,
    )

    assert result["new_rows_extracted"] == 2, "the row already stored must not come back"
    assert result["num_rows"] == 3
    assert _live_tables(conn, schema) == ["orders"]
    assert conn.execute(f"SELECT sum(amount) FROM {target}").fetchone()[0] == 60.0


def test_an_upsert_into_an_existing_table_merges_from_a_load_table(lake, json_api) -> None:
    """The delta is deduplicated in the lake and merged into what is already stored."""
    engine, conn, schema = lake
    server, handler = json_api
    target = engine._target_sql(schema, "orders")
    handler.payload = [
        {"id": 1, "amount": 10.0, "updated_at": 100},
        {"id": 2, "amount": 20.0, "updated_at": 100},
    ]
    engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="snapshot",
        target_folder=schema,
    )

    handler.payload = [
        {"id": 2, "amount": 99.0, "updated_at": 200},
        {"id": 2, "amount": 50.0, "updated_at": 150},
    ]
    result = engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        mode="incremental",
        watermark_column="updated_at",
        write_strategy="upsert",
        primary_key="id",
        target_folder=schema,
    )

    assert result["num_rows"] == 2
    assert dict(conn.execute(f"SELECT id, amount FROM {target}").fetchall()) == {
        1: 10.0,
        2: 99.0,
    }, "the newest row of the load wins, and the key it did not mention is untouched"
    assert _live_tables(conn, schema) == ["orders"], "the load table must not survive"


def test_an_upsert_without_a_primary_key_fails_before_the_source_is_touched(lake) -> None:
    """Refusing early means a 100 GB load cannot be wasted on an impossible write."""
    engine, _conn, schema = lake

    with pytest.raises(ValueError, match="primary_key"):
        engine.ingest_entity(
            source_type="api",
            connection_params={"host": "127.0.0.1", "port": 9},
            entity="orders",
            write_strategy="upsert",
            target_folder=schema,
        )


# ---------------------------------------------------------------------------
# housekeeping reclaims what a killed run could not
# ---------------------------------------------------------------------------


def test_the_maintenance_sweep_drops_orphaned_load_tables_and_nothing_else(lake) -> None:
    """A killed run leaves its load table behind; the sweep is what reclaims it.

    The sweep is catalog-wide by design — a load table is never something a finished run
    leaves — so this asserts on its own schema: the load table goes and the table beside it
    does not.
    """
    engine, conn, schema = lake
    target = engine._target_sql(schema, "orders")
    orphan = engine._target_sql(schema, f"orders{LOAD_TABLE_SUFFIX}")
    conn.execute(f"CREATE TABLE {target} AS SELECT 1 AS order_id")
    conn.execute(f"CREATE TABLE {orphan} AS SELECT 2 AS order_id")
    maintenance = _maintenance()

    reported = maintenance.drop_orphaned_load_tables(conn, dry_run=True)

    assert (schema, f"orders{LOAD_TABLE_SUFFIX}") in reported
    assert _live_tables(conn, schema) == ["orders", f"orders{LOAD_TABLE_SUFFIX}"], (
        "a dry run names what it would drop and drops nothing"
    )

    dropped = maintenance.drop_orphaned_load_tables(conn)

    assert (schema, f"orders{LOAD_TABLE_SUFFIX}") in dropped
    assert _live_tables(conn, schema) == ["orders"], "the table beside it is untouched"
    assert conn.execute(f"SELECT order_id FROM {target}").fetchall() == [(1,)]


# ----------------------------------------------------------------------------
# the tree lists folders, not the directories DuckLake makes for table files
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_tree_shows_folders_and_never_a_table_s_data_directory(lake, json_api) -> None:
    """A folder is a schema. The directory DuckLake gives a table's Parquet is not one.

    A walk of the warehouse cannot tell them apart, which is how every table used to
    arrive in the tree as a folder of its own — and how a killed run's
    ``__obelisk_load`` directory stayed there long after the table it fed was gone.
    """
    engine, _conn, schema = lake
    server, handler = json_api
    # Enough rows that DuckLake writes Parquet instead of inlining them: a data file is
    # what puts a directory on disk, and that directory is what the tree used to list.
    handler.payload = [{"order_id": row, "amount": float(row)} for row in range(5000)]

    engine.ingest_entity(
        source_type="api",
        connection_params=_api_params(server),
        entity="orders",
        target_folder=schema,
    )

    root = settings.lakehouse_root_path
    user_folder = f"{schema}/staging"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/api/catalog/lakehouse/directories", json={"path": user_folder}
        )
        assert created.status_code == 200, created.text
        try:
            res = await client.get("/api/catalog/tree?refresh=true")
        finally:
            await client.delete(
                "/api/catalog/lakehouse/directories", params={"path": user_folder}
            )

    assert res.status_code == 200, res.text
    node = next(n for n in res.json() if n["id"] == "lakehouse")
    folders = {directory["path"]: directory for directory in node["directories"]}

    assert schema in folders, "the schema the ingest wrote to is a folder"
    assert user_folder in folders, "a folder a user created is still a folder"
    assert [t["name"] for t in folders[schema]["tables"]] == ["orders"], (
        "the table is listed in the folder it belongs to"
    )
    assert f"{schema}/orders" not in folders, "a table's data directory is not a folder"
    assert not any(path.endswith(LOAD_TABLE_SUFFIX) for path in folders), (
        "an ingest's working directory is never a folder"
    )
    # The Parquet sits in a directory of its own — a swapped-in table keeps the load
    # table's name, because a rename moves the catalog entry, not the files. A walk of the
    # warehouse could only describe those directories as folders; the tree must not.
    data_dirs = {path.name for path in (root / schema).iterdir() if path.is_dir()}
    assert data_dirs, "the table's files are in a directory of their own"
    assert data_dirs.isdisjoint({path.rsplit("/", 1)[-1] for path in folders}), (
        "no table directory is presented as a folder"
    )


def test_a_load_table_s_directory_is_swept_when_the_run_ends(lake) -> None:
    """The directory outlives its table, so the run that ends is what removes it."""
    engine, _conn, schema = lake
    load = f"orders{LOAD_TABLE_SUFFIX}"
    load_dir = settings.lakehouse_root_path / schema / load
    load_dir.mkdir(parents=True, exist_ok=True)

    engine._prune_load_directory(schema, load)

    assert not load_dir.exists(), "an empty load directory goes with the load table"

