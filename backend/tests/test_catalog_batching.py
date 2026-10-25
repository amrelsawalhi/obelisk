"""The warehouse is read once, by one walker.

The tree used to be built by an introspector that walked the lakehouse and read each
footer; the catalog scan does exactly that now, so there is one definition of what a
table is. These tests pin the batched shape that both the tree and the projection rely
on: a single walk, one footer read per file, grouped by folder.
"""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.core.catalog import scan
from app.core.storage import LocalWarehouse


def _write(path: Path, rows: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": list(range(rows))}), str(path))


def _by_folder(root: Path) -> tuple[list[str], dict[str, list]]:
    store = LocalWarehouse(root)
    result = scan(store)
    grouped: dict[str, list] = {}
    for table in result.tables:
        grouped.setdefault(table.folder, []).append(table)
    return store.list_directories(), grouped


def test_one_walk_groups_tables_by_folder(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    _write(root / "loose.parquet", rows=2)
    _write(root / "raw" / "orders.parquet", rows=3)
    _write(root / "marts" / "sales" / "daily.parquet", rows=4)
    _write(root / ".hidden" / "skipped.parquet")

    directories, tables = _by_folder(root)

    assert directories == ["marts", "marts/sales", "raw"]

    assert [table.name for table in tables[""]] == ["loose"]
    assert [table.name for table in tables["raw"]] == ["orders"]
    assert [table.name for table in tables["marts/sales"]] == ["daily"]

    # A file at the root has no folder, so its address is two slots.
    assert tables[""][0].fqn == "lakehouse.loose"

    # A flat folder is the schema …
    assert tables["raw"][0].fqn == "lakehouse.raw.orders"
    assert tables["raw"][0].sql_name == "lakehouse.raw.orders"

    # … and a nested one carries its whole path, quoted, because SQL has three slots.
    assert tables["marts/sales"][0].fqn == "lakehouse.marts.sales.daily"
    assert tables["marts/sales"][0].sql_name == 'lakehouse."marts.sales".daily'

    # Hidden folders are not part of the tree.
    assert all(".hidden" not in key for key in tables)


def test_rows_and_sizes_come_from_the_footer(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    _write(root / "raw" / "orders.parquet", rows=7)

    _, tables = _by_folder(root)
    table = tables["raw"][0]

    assert table.num_rows == 7
    assert table.size_bytes > 0
    assert table.columns[0]["name"] == "id"


def test_empty_directories_still_appear(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    (root / "raw").mkdir(parents=True)
    (root / "marts").mkdir()

    directories, tables = _by_folder(root)

    assert directories == ["marts", "raw"]
    assert tables == {}


def test_missing_root_is_not_an_error(tmp_path: Path) -> None:
    directories, tables = _by_folder(tmp_path / "does-not-exist")
    assert directories == [] and tables == {}
