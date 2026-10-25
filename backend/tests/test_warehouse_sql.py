"""The editor queries the warehouse by address, however deeply it is nested.

A table's SQL name is still three slots — ``lakehouse.folder.table``, with a nested folder
path carried as one quoted schema. DuckDB's parser stops at three parts, so the engine
rewrites the address a person types into the form it parses; these tests pin both
spellings, the bare-name convenience, and the fact that neither can be fooled by a string
literal or a name that is not a table.

What changed: the table is created through the ingest writer, because a Parquet file lying
on disk is no longer a table — the catalog is.
"""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import ingest_engine


def write_table(rel_path: str, rows: int = 2) -> None:
    """Creates (or replaces) the table through the writer, as ingestion would."""
    folder, name = rel_path.rsplit("/", 1) if "/" in rel_path else ("", rel_path)
    name = name[: -len(".parquet")] if name.endswith(".parquet") else name
    schema, table = ingest_engine._address(folder, name, None)

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS lakehouse."{schema}";')
        batch = pa.table({"id": list(range(1, rows + 1))})
        assert ingest_engine._stage_batches(conn, [batch]) == rows
        ingest_engine._write_staged(
            conn,
            target=f'lakehouse."{schema}".{table}',
            mode="snapshot",
            exists=False,
            write_strategy="append",
            primary_key=None,
            watermark_column=None,
        )
    finally:
        conn.close()


def drop_schemas(prefix: str) -> None:
    """Removes every schema this test created, including nested ones."""
    conn = duckdb_engine.get_connection()
    try:
        rows = conn.execute(
            "SELECT schema_name FROM __ducklake_metadata_lakehouse.ducklake_schema "
            "WHERE schema_name LIKE ?",
            [f"{prefix}%"],
        ).fetchall()
        for (schema,) in rows:
            conn.execute(f'DROP SCHEMA lakehouse."{schema}" CASCADE')
        for (table,) in conn.execute(
            "SELECT table_name FROM __ducklake_metadata_lakehouse.ducklake_table "
            "WHERE table_name LIKE ?",
            [f"{prefix}%"],
        ).fetchall():
            conn.execute(f'DROP TABLE IF EXISTS lakehouse."main".{table}')
    finally:
        conn.close()


def count(sql: str) -> int:
    return duckdb_engine.execute_query(sql)["rows"][0]["n"]


@pytest.fixture
def prefix():
    """A unique folder namespace, removed (schema and tables) afterwards."""
    name = f"zzsql{uuid.uuid4().hex[:6]}"
    yield name
    drop_schemas(name)


def test_a_table_in_a_folder_is_three_slots(prefix):
    write_table(f"{prefix}/orders.parquet", rows=3)

    assert count(f"select count(*) as n from lakehouse.{prefix}.orders") == 3


def test_a_table_at_the_root_is_two_slots(prefix):
    name = f"{prefix}_loose"
    write_table(f"{name}.parquet", rows=4)

    assert count(f"select count(*) as n from lakehouse.{name}") == 4


def test_a_nested_path_answers_to_both_spellings(prefix):
    write_table(f"{prefix}/a/b/table1.parquet", rows=5)

    # What the UI shows (quoted, because the schema name carries the dots) ...
    assert count(f'select count(*) as n from lakehouse."{prefix}.a.b".table1') == 5
    # ... and what a person types (unquoted, four parts, rewritten before parsing).
    assert count(f"select count(*) as n from lakehouse.{prefix}.a.b.table1") == 5


def test_the_rewrite_reaches_through_joins_and_aliases(prefix):
    write_table(f"{prefix}/a/b/left_side.parquet", rows=2)
    write_table(f"{prefix}/c/right_side.parquet", rows=2)

    sql = (
        f"select count(*) as n from lakehouse.{prefix}.a.b.left_side l "
        f"join lakehouse.{prefix}.c.right_side r on l.id = r.id"
    )
    assert count(sql) == 2


def test_the_folder_shorthand_resolves_without_the_lakehouse_prefix(prefix):
    write_table(f"{prefix}/shorthand.parquet", rows=6)

    # Pipeline SQL and the docs address a table by its folder ("bronze.orders"). The
    # engine maps that shorthand onto the canonical address rather than requiring the
    # "lakehouse." prefix, and must not rewrite the canonical form a second time.
    assert count(f"select count(*) as n from {prefix}.shorthand") == 6
    assert count(f"select count(*) as n from lakehouse.{prefix}.shorthand") == 6


def test_the_bare_name_works_while_it_is_unique(prefix):
    write_table(f"{prefix}/solo.parquet", rows=2)

    assert count("select count(*) as n from solo") == 2


def test_a_duplicate_name_gets_no_bare_alias(prefix):
    write_table(f"{prefix}/x/dup.parquet", rows=1)
    write_table(f"{prefix}/y/dup.parquet", rows=1)

    # Guessing which "dup" was meant would be a silent wrong answer.
    with pytest.raises(Exception):
        duckdb_engine.execute_query("select count(*) as n from dup")

    # Both remain reachable by their real addresses.
    assert count(f"select count(*) as n from lakehouse.{prefix}.x.dup") == 1
    assert count(f"select count(*) as n from lakehouse.{prefix}.y.dup") == 1


def test_a_table_written_a_moment_ago_is_queryable_at_once(prefix):
    write_table(f"{prefix}/fresh.parquet", rows=1)
    assert count(f"select count(*) as n from lakehouse.{prefix}.fresh") == 1

    # Replacing the table must be visible immediately too: the catalog is the truth now,
    # so there is no file timestamp to go stale behind.
    write_table(f"{prefix}/fresh.parquet", rows=6)
    assert count(f"select count(*) as n from lakehouse.{prefix}.fresh") == 6


def test_rewriting_touches_code_only(prefix):
    write_table(f"{prefix}/a/b/table1.parquet", rows=1)
    target = f"lakehouse.{prefix}.a.b.table1"
    quoted = f'lakehouse."{prefix}.a.b".table1'
    mapping = {target.lower(): quoted}

    rewritten = duckdb_engine._rewrite_names(
        f"select '{target}', \"{target}\" from {target} -- {target}\n"
        f"/* {target} */ where x = 'y'",
        mapping,
    )

    assert f"select '{target}', \"{target}\" from {quoted}" in rewritten
    assert f"-- {target}" in rewritten
    assert f"/* {target} */" in rewritten


def test_rewriting_leaves_names_the_warehouse_does_not_have(prefix):
    mapping = {f"lakehouse.{prefix}.a.b.table1".lower(): "x"}

    untouched = duckdb_engine._rewrite_names(
        "select * from lakehouse.other.schema.table1", mapping
    )
    assert untouched == "select * from lakehouse.other.schema.table1"

    # A prefix of a real name is a different name.
    longer = duckdb_engine._rewrite_names(
        f"select * from xlakehouse.{prefix}.a.b.table1", mapping
    )
    assert longer == f"select * from xlakehouse.{prefix}.a.b.table1"

