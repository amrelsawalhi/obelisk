"""Shared helpers for tests that need a table to exist.

A Parquet file on disk is no longer a table — the catalog is — so fixtures that used to
write a file now call :func:`write_table`. Kept in one place so every test file agrees on
what "create a table" means.
"""
from __future__ import annotations

import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from app.core.catalog import SOURCE, quote_ident
from app.engines.duckdb_engine import duckdb_engine


def parse_parquet(data: bytes) -> pa.Table:
    """Reads Parquet bytes into an Arrow table (what a fixture already has)."""
    return pq.read_table(io.BytesIO(data))


def split_rel_path(rel_path: str) -> tuple[str, str]:
    """``"raw/orders.parquet"`` -> ``("raw", "orders")``; a root file -> ``("main", name)``."""
    folder, name = rel_path.rsplit("/", 1) if "/" in rel_path else ("", rel_path)
    if name.endswith(".parquet"):
        name = name[: -len(".parquet")]
    return (folder.replace("/", ".") if folder else "main"), name


def write_table(
    rel_path: str,
    table: pa.Table | bytes,
    *,
    replace: bool = True,
) -> str:
    """Creates the table the catalog will report, and returns its SQL address."""
    if isinstance(table, (bytes, bytearray)):
        table = parse_parquet(bytes(table))
    schema, name = split_rel_path(rel_path)
    address = f"{SOURCE}.{quote_ident(schema)}.{quote_ident(name)}"

    conn = duckdb_engine.get_connection()
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema)};")
        conn.register("_test_batch", table)
        try:
            verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
            conn.execute(f"{verb} {address} AS SELECT * FROM _test_batch")
        finally:
            conn.unregister("_test_batch")
    finally:
        conn.close()
    return address


def drop_schema(prefix: str) -> None:
    """Drops every schema and root table whose name starts with ``prefix``."""
    conn = duckdb_engine.get_connection()
    try:
        rows = conn.execute(
            "SELECT schema_name FROM __ducklake_metadata_lakehouse.ducklake_schema "
            "WHERE schema_name LIKE ?",
            [f"{prefix}%"],
        ).fetchall()
        for (schema,) in rows:
            conn.execute(f"DROP SCHEMA IF EXISTS {SOURCE}.{quote_ident(schema)} CASCADE")
        for (table,) in conn.execute(
            "SELECT table_name FROM __ducklake_metadata_lakehouse.ducklake_table "
            "WHERE table_name LIKE ?",
            [f"{prefix}%"],
        ).fetchall():
            conn.execute(f"DROP TABLE IF EXISTS {SOURCE}.{quote_ident('main')}.{quote_ident(table)}")
    finally:
        conn.close()


def unique_name(prefix: str = "zz") -> str:
    return f"{prefix}{uuid.uuid4().hex[:6]}"
