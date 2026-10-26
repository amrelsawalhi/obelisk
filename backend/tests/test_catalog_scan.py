"""The warehouse is a directory; the catalog is what a walk of it finds.

These tests pin the rule the whole refactor rests on: a table exists because a file
exists, and its address is its path. Nothing here touches the real lakehouse — every
case builds a scratch directory and writes real Parquet into it, because the point is
to read genuine footers rather than a stand-in.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.core.catalog import (
    LOAD_TABLE_SUFFIX,
    SOURCE,
    InvalidTablePath,
    fqn_for,
    path_for,
    quote_ident,
    scan,
    split_table_path,
    sql_name_for,
)
from app.core.storage import FileInfo, InvalidPath, LocalWarehouse, normalize_rel


def write_parquet(path, rows: int = 3, extra_column: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "id": list(range(1, rows + 1)),
        "name": [f"row-{i}" for i in range(1, rows + 1)],
    }
    if extra_column:
        data["amount"] = [1.5 * i for i in range(1, rows + 1)]
    pq.write_table(pa.table(data), str(path), compression="SNAPPY")


def parquet_bytes(rows: int = 1) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": list(range(1, rows + 1))}), buffer)
    return buffer.getvalue()


# ----------------------------------------------------------------- storage layer


def test_paths_must_stay_inside_the_warehouse(tmp_path):
    store = LocalWarehouse(tmp_path)
    for bad in ("../outside.parquet", "a/../../x.parquet", "/absolute.parquet", "C:/x.parquet"):
        with pytest.raises(InvalidPath):
            normalize_rel(bad)
        with pytest.raises(InvalidPath):
            store.stat(bad)


def test_a_write_creates_folders_and_leaves_no_temporaries(tmp_path):
    store = LocalWarehouse(tmp_path)
    store.write_atomic("supabase/auth/table1.parquet", parquet_bytes(rows=4))

    assert (tmp_path / "supabase" / "auth" / "table1.parquet").is_file()
    assert list(tmp_path.rglob("*.tmp")) == []
    info = store.stat("supabase/auth/table1.parquet")
    assert info.size_bytes > 0
    assert info.modified_at.tzinfo is not None


def test_hidden_files_and_folders_are_not_tables(tmp_path):
    store = LocalWarehouse(tmp_path)
    write_parquet(tmp_path / "orders.parquet")
    write_parquet(tmp_path / ".hidden" / "secret.parquet")
    write_parquet(tmp_path / ".staging.parquet")
    (tmp_path / "notes.txt").write_text("not a table", encoding="utf-8")

    paths = [info.path for info in store.list_parquet()]
    assert paths == ["orders.parquet"]


# ------------------------------------------------------------------- addressing


def test_a_path_is_the_address():
    assert fqn_for("orders.parquet") == "lakehouse.orders"
    assert fqn_for("supabase/table1.parquet") == "lakehouse.supabase.table1"
    assert fqn_for("supabase/auth/table1.parquet") == "lakehouse.supabase.auth.table1"


def test_sql_names_quote_only_where_sql_demands_it():
    assert sql_name_for("orders.parquet") == "lakehouse.orders"
    assert sql_name_for("supabase/table1.parquet") == "lakehouse.supabase.table1"
    # Deeper than one folder: one schema whose name carries the dots.
    assert sql_name_for("supabase/auth/table1.parquet") == 'lakehouse."supabase.auth".table1'
    # Characters SQL cannot read unquoted, and reserved words.
    assert sql_name_for("my-folder/table1.parquet") == 'lakehouse."my-folder".table1'
    assert sql_name_for("order.parquet") == 'lakehouse."order"'
    assert sql_name_for("select.parquet") == 'lakehouse."select"'


def test_quote_ident_leaves_plain_names_alone():
    assert quote_ident("orders") == "orders"
    assert quote_ident("order") == '"order"'  # ORDER BY
    assert quote_ident("select") == '"select"'
    assert quote_ident("weird name") == '"weird name"'


def test_three_spellings_of_the_same_table():
    assert split_table_path("supabase/auth/table1") == ("supabase/auth", "table1")
    assert split_table_path("lakehouse.supabase.auth.table1") == ("supabase/auth", "table1")
    assert split_table_path("supabase/auth/table1.parquet") == ("supabase/auth", "table1")
    assert split_table_path("orders") == ("", "orders")


def test_path_for_is_the_inverse_of_split():
    assert path_for("supabase/auth", "table1") == "supabase/auth/table1.parquet"
    assert path_for("", "orders") == "orders.parquet"


@pytest.mark.parametrize(
    "bad", ["", "   ", "supabase//table1", "a/../b", "sales.orders/table1", "-bad"]
)
def test_illegal_paths_are_refused(bad):
    with pytest.raises(InvalidTablePath):
        split_table_path(bad)


# ------------------------------------------------------------------------ scan


def test_the_source_slot_names_the_warehouse():
    assert SOURCE == "lakehouse"


# --------------------------------------------------------------- scan (the catalog)

def write_table(schema: str, table: str, rows: int = 3, extra_column: bool = False) -> None:
    """Creates a table through the writer, which is how tables exist now."""
    from app.engines.duckdb_engine import duckdb_engine
    from app.engines.ingest_engine import ingest_engine

    data = {"id": list(range(1, rows + 1)), "name": [f"row-{i}" for i in range(1, rows + 1)]}
    if extra_column:
        data["amount"] = [1.5 * i for i in range(1, rows + 1)]

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS lakehouse."{schema}";')
        target = f'lakehouse."{schema}".{table}'
        load = ingest_engine._target_sql(schema, f"{table}{LOAD_TABLE_SUFFIX}")
        assert ingest_engine._stage_batches(conn, [pa.table(data)], sink=load, create=True) == rows
        ingest_engine._swap_into_place(
            conn, schema=schema, staged=load, target=target, table=table
        )
    finally:
        conn.close()


@pytest.fixture
def schema():
    """A unique schema in the test lakehouse, dropped afterwards."""
    import uuid

    from app.engines.duckdb_engine import duckdb_engine

    name = f"zzscan{uuid.uuid4().hex[:6]}"
    yield name
    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f'DROP SCHEMA IF EXISTS lakehouse."{name}" CASCADE')
    finally:
        conn.close()


def test_scan_reads_what_the_catalog_holds(schema):
    write_table(schema, "orders", rows=5, extra_column=True)

    result = scan()

    by_fqn = {table.fqn: table for table in result.tables}
    table = by_fqn[f"lakehouse.{schema}.orders"]
    assert table.num_rows == 5
    assert table.folder == schema
    assert [column["name"] for column in table.columns] == ["id", "name", "amount"]
    assert table.modified_at.tzinfo == timezone.utc
    assert result.problems == (), "a healthy catalog reports no problems"


def test_the_bulk_column_read_answers_exactly_what_describe_would(schema):
    """One query for the whole catalog, spelling types the way DuckDB spells them.

    DuckLake's own metadata stores ``int32`` and ``varchar``; the catalog has always shown
    what ``DESCRIBE`` shows, so the fast path is pinned to the per-table read it replaced —
    and to the names a user reads in the column list.
    """
    from app.core.catalog import lakehouse_all_columns, lakehouse_columns
    from app.engines.duckdb_engine import duckdb_engine

    write_table(schema, "orders", rows=3, extra_column=True)
    write_table(schema, "no_amount", rows=1)

    conn = duckdb_engine.get_connection()
    try:
        bulk = lakehouse_all_columns(conn)
        assert bulk[(schema, "orders")] == lakehouse_columns(conn, schema, "orders")
        assert bulk[(schema, "no_amount")] == lakehouse_columns(conn, schema, "no_amount")
    finally:
        conn.close()

    table = next(t for t in scan().tables if t.fqn == f"lakehouse.{schema}.orders")
    assert [column["data_type"] for column in table.columns] == [
        "BIGINT",
        "VARCHAR",
        "DOUBLE",
    ]


def test_a_dropped_table_disappears_from_the_catalog(schema):
    from app.engines.duckdb_engine import duckdb_engine

    write_table(schema, "gone", rows=2)
    assert any(t.fqn == f"lakehouse.{schema}.gone" for t in scan().tables)

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f'DROP TABLE lakehouse."{schema}".gone')
    finally:
        conn.close()

    assert not any(t.fqn == f"lakehouse.{schema}.gone" for t in scan().tables)
