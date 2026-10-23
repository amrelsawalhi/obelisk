"""Every layer a table lives in is a layer the tree knows about.

Ingest and the SDK used to write into whatever folder name they were given, so a
table could end up somewhere the layer tree had never heard of. These tests pin the
rule that closed that: writing data creates the layer, and the catalog never names a
layer that does not exist.
"""
import sqlite3
import uuid

import polars as pl
import pytest

from app.config import settings
from app.core.layers import resolve_layer
from app.sdk import obelisk
from app.services.layers import default_layer_path, ensure_layer, layer_paths


def _cleanup(layer: str, name: str) -> None:
    with sqlite3.connect(settings.sqlite_db_path) as con:
        con.execute("DELETE FROM assets WHERE layer = ? AND name = ?", (layer, name))
        con.execute("DELETE FROM catalog_objects WHERE fqn = ?", (f"lakehouse.{layer}.{name}",))
        con.commit()


def test_ensure_layer_creates_missing_parents() -> None:
    root = f"zz{uuid.uuid4().hex[:6]}"
    child = f"{root}.sales"
    leaf = f"{root}.sales.daily"

    try:
        assert ensure_layer(leaf) == leaf

        with sqlite3.connect(settings.sqlite_db_path) as con:
            rows = {
                path: (name, parent)
                for path, name, parent in con.execute(
                    "SELECT path, name, parent_path FROM layers WHERE path IN (?, ?, ?)",
                    (root, child, leaf),
                )
            }

        assert rows[root] == (root, None)
        assert rows[child] == ("sales", root)
        assert rows[leaf] == ("daily", child)

        # Idempotent: a second call adds nothing and changes no parent.
        before = len(layer_paths())
        ensure_layer(leaf)
        assert len(layer_paths()) == before
    finally:
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM layers WHERE path IN (?, ?, ?)", (root, child, leaf))
            con.commit()


def test_ensure_layer_keeps_exactly_one_default() -> None:
    root = f"zz{uuid.uuid4().hex[:6]}"
    try:
        ensure_layer(root)
        with sqlite3.connect(settings.sqlite_db_path) as con:
            defaults = con.execute(
                "SELECT COUNT(*) FROM layers WHERE is_default = 1"
            ).fetchone()[0]
        assert defaults == 1
    finally:
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM layers WHERE path = ?", (root,))
            con.commit()


def test_save_creates_its_layer_and_registers_the_object() -> None:
    root = f"zz{uuid.uuid4().hex[:6]}"
    layer = f"{root}.sales"
    name = f"orders_{uuid.uuid4().hex[:4]}"

    try:
        frame = pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.0]})
        result = obelisk.save(frame, f"{layer}.{name}", sync_postgres=False)

        assert result["layer"] == layer
        assert result["name"] == name
        assert result["warehouse_schema"] == resolve_layer(layer).warehouse_schema
        assert result["file_path"].endswith(f"{layer}/{name}.parquet")

        with sqlite3.connect(settings.sqlite_db_path) as con:
            paths = {row[0] for row in con.execute("SELECT path FROM layers")}
            asset = con.execute(
                "SELECT layer, record_count FROM assets WHERE layer = ? AND name = ?",
                (layer, name),
            ).fetchone()
            catalog = con.execute(
                "SELECT layer_path, kind FROM catalog_objects WHERE fqn = ?",
                (f"lakehouse.{layer}.{name}",),
            ).fetchone()

        # The nested path arrives with both of its layers, not just the leaf.
        assert {root, layer} <= paths
        assert asset == (layer, 2)
        assert catalog == (layer, "parquet")
    finally:
        _cleanup(layer, name)
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM layers WHERE path IN (?, ?)", (root, layer))
            con.commit()


def test_save_splits_on_the_last_dot() -> None:
    """`a.b.c` is layer `a.b` plus table `c`, not layer `a` plus table `b.c`."""
    root = f"zz{uuid.uuid4().hex[:6]}"
    name = f"t{uuid.uuid4().hex[:4]}"
    try:
        result = obelisk.save(pl.DataFrame({"id": [1]}), f"{root}.inner.{name}", sync_postgres=False)
        assert result["layer"] == f"{root}.inner"
        assert result["name"] == name
    finally:
        _cleanup(f"{root}.inner", name)
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM layers WHERE path IN (?, ?)", (root, f"{root}.inner"))
            con.commit()


def test_catalogued_tables_only_name_existing_layers() -> None:
    """The invariant the backfill and the write paths exist to keep true."""
    with sqlite3.connect(settings.sqlite_db_path) as con:
        orphans = con.execute(
            "SELECT DISTINCT layer FROM assets WHERE layer IS NOT NULL AND layer <> ''"
            " AND layer NOT IN (SELECT path FROM layers)"
        ).fetchall()

    assert orphans == [], f"assets rows reference layers that do not exist: {orphans}"


def test_default_layer_is_a_real_layer() -> None:
    assert default_layer_path() in layer_paths()
