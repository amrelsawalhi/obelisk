"""The catalog is read once, in one query, and the tree is grouped from that answer.

The tree used to be built by walking the lakehouse and reading a footer per file. Now the
catalog answers the question — schema, row count and byte size all come from its metadata —
so there is one definition of what a table is and no per-file work.

What remains here: the shape both the tree and the projection depend on (grouped by
folder, addresses in three slots), plus the storage layer's own directory behaviour, which
is still a filesystem concern.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pyarrow as pa
import pytest

from app.core.catalog import scan
from app.core.storage import LocalWarehouse

from lakehouse_helpers import drop_schema, write_table


def _by_folder() -> dict[str, list]:
    grouped: dict[str, list] = {}
    for table in scan().tables:
        grouped.setdefault(table.folder, []).append(table)
    return grouped


@pytest.fixture
def prefix():
    """A unique folder namespace, removed afterwards."""
    name = f"zzbatch{uuid.uuid4().hex[:6]}"
    yield name
    drop_schema(name)


def test_the_catalog_groups_tables_by_folder(prefix) -> None:
    # Unique schemas on purpose: the session lakehouse is shared, and a table called
    # `raw.orders` here would be read by the shipped dbt models in another test.
    write_table(f"{prefix}_raw/orders.parquet", pa.table({"id": [1, 2, 3]}))
    write_table(f"{prefix}_marts/sales/daily.parquet", pa.table({"id": [1, 2, 3, 4]}))
    write_table(f"{prefix}/loose.parquet", pa.table({"id": [1, 2]}))
    write_table(f"root_{prefix}.parquet", pa.table({"id": [1]}))

    grouped = _by_folder()

    assert [table.name for table in grouped[f"{prefix}_raw"]] == ["orders"]
    # A folder is a schema, so a nested path arrives with dots, not slashes.
    assert [table.name for table in grouped[f"{prefix}_marts.sales"]] == ["daily"]

    loose = grouped[prefix][0]
    assert loose.fqn == f"lakehouse.{prefix}.loose"

    orders = grouped[f"{prefix}_raw"][0]
    assert orders.fqn == f"lakehouse.{prefix}_raw.orders"
    assert orders.sql_name == f"lakehouse.{prefix}_raw.orders"

    # A nested folder is one quoted schema, because SQL has three slots.
    daily = grouped[f"{prefix}_marts.sales"][0]
    assert daily.fqn == f"lakehouse.{prefix}_marts.sales.daily"
    assert daily.sql_name == f'lakehouse."{prefix}_marts.sales".daily'

    # A table in the catalog's default schema is a two-slot address.
    assert grouped[""][0].fqn == f"lakehouse.root_{prefix}"


def test_rows_and_columns_come_from_the_catalog(prefix) -> None:
    write_table(f"{prefix}/orders.parquet", pa.table({"id": list(range(7)), "name": ["a"] * 7}))

    table = _by_folder()[prefix][0]

    assert table.num_rows == 7
    assert [column["name"] for column in table.columns] == ["id", "name"]
    # Size is whatever the catalog recorded for the table's files; a small table may be
    # inlined in the catalog itself, so only the lower bound is meaningful.
    assert table.size_bytes >= 0


def test_empty_directories_still_appear(tmp_path: Path) -> None:
    """A folder is where tables go; the storage layer still reports empty ones."""
    (tmp_path / "raw").mkdir(parents=True)
    (tmp_path / "marts").mkdir()

    assert LocalWarehouse(tmp_path).list_directories() == ["marts", "raw"]


def test_missing_root_is_not_an_error(tmp_path: Path) -> None:
    assert LocalWarehouse(tmp_path / "does-not-exist").list_directories() == []
