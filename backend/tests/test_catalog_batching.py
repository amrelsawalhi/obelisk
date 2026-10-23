"""The lakehouse is read once per refresh, not once per directory.

The catalog tree used to call the single-directory introspector for every folder it
found, so each folder re-walked the tree and paid its own thread hop. These tests pin
the batched shape: one walk, one footer read per file, grouped by directory.
"""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.engines.introspector import introspector


def _write(path: Path, rows: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": list(range(rows))}), path)


def test_introspect_lakehouse_groups_by_directory(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    _write(root / "loose.parquet", rows=2)
    _write(root / "raw" / "orders.parquet", rows=3)
    _write(root / "marts" / "sales" / "daily.parquet", rows=4)
    _write(root / ".hidden" / "skipped.parquet")

    directories, tables = introspector.introspect_lakehouse(root)

    assert directories == ["marts", "marts/sales", "raw"]

    assert [t["name"] for t in tables[""]] == ["loose"]
    assert [t["name"] for t in tables["raw"]] == ["orders"]
    assert [t["name"] for t in tables["marts/sales"]] == ["daily"]

    # Files at the root keep the legacy "main" schema.
    assert tables[""][0]["schema_name"] == "main"
    assert tables[""][0]["fqn"] == "lakehouse.main.loose"

    # A flat folder is unchanged …
    assert tables["raw"][0]["schema_name"] == "raw"
    assert tables["raw"][0]["fqn"] == "lakehouse.raw.orders"

    # … and a nested one carries its whole path, so it lines up with a layer path.
    assert tables["marts/sales"][0]["schema_name"] == "marts.sales"
    assert tables["marts/sales"][0]["fqn"] == "lakehouse.marts.sales.daily"

    # Hidden folders are not part of the tree.
    assert all(".hidden" not in key for key in tables)


def test_introspect_lakehouse_records_sizes_and_rows(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    _write(root / "raw" / "orders.parquet", rows=7)

    _, tables = introspector.introspect_lakehouse(root)
    entry = tables["raw"][0]

    assert entry["row_count"] == 7
    assert entry["size_bytes"] > 0
    assert entry["size_is_estimate"] is False
    assert entry["columns"][0]["name"] == "id"


def test_empty_directories_still_appear(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    (root / "raw").mkdir(parents=True)
    (root / "marts").mkdir()

    directories, tables = introspector.introspect_lakehouse(root)

    assert directories == ["marts", "raw"]
    assert tables == {}


def test_missing_root_is_not_an_error(tmp_path: Path) -> None:
    directories, tables = introspector.introspect_lakehouse(tmp_path / "does-not-exist")
    assert directories == [] and tables == {}
