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


def test_scan_reads_the_tree_it_finds(tmp_path):
    write_parquet(tmp_path / "orders.parquet", rows=5)
    write_parquet(tmp_path / "supabase" / "table1.parquet", rows=2, extra_column=True)
    write_parquet(tmp_path / "supabase" / "auth" / "table1.parquet", rows=7)
    (tmp_path / "empty" / "nested").mkdir(parents=True)

    result = scan(LocalWarehouse(tmp_path))

    assert result.problems == ()
    assert [table.fqn for table in result.tables] == [
        "lakehouse.orders",
        "lakehouse.supabase.auth.table1",
        "lakehouse.supabase.table1",
    ]
    by_path = {table.path: table for table in result.tables}
    assert by_path["orders.parquet"].num_rows == 5
    assert by_path["orders.parquet"].folder == ""
    assert by_path["supabase/auth/table1.parquet"].folder == "supabase/auth"
    assert (
        by_path["supabase/auth/table1.parquet"].sql_name
        == 'lakehouse."supabase.auth".table1'
    )
    assert [column["name"] for column in by_path["supabase/table1.parquet"].columns] == [
        "id",
        "name",
        "amount",
    ]
    assert by_path["supabase/table1.parquet"].size_bytes > 0
    assert by_path["supabase/table1.parquet"].modified_at.tzinfo == timezone.utc


def test_an_empty_warehouse_scans_to_nothing(tmp_path):
    result = scan(LocalWarehouse(tmp_path))
    assert result.tables == ()
    assert result.problems == ()


def test_a_corrupt_file_is_reported_and_does_not_hide_the_rest(tmp_path):
    write_parquet(tmp_path / "orders.parquet")
    broken = tmp_path / "broken" / "table1.parquet"
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"this is not parquet")

    result = scan(LocalWarehouse(tmp_path))

    assert [table.fqn for table in result.tables] == ["lakehouse.orders"]
    assert len(result.problems) == 1
    assert "broken/table1.parquet" in result.problems[0]


def test_the_scan_never_writes(tmp_path):
    write_parquet(tmp_path / "orders.parquet")
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    scan(LocalWarehouse(tmp_path))
    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert before == after


class InMemoryWarehouse:
    """A warehouse whose files are a dict, so case-only collisions can be exercised.

    Windows cannot hold ``orders.parquet`` and ``Orders.parquet`` in one folder, but a
    bucket can, and the scan has to notice instead of letting one shadow the other.
    """

    def __init__(self, files: dict[str, bytes]):
        self.files = dict(files)

    def display_root(self) -> str:
        return "memory://warehouse"

    def list_parquet(self) -> list[FileInfo]:
        now = datetime.now(timezone.utc)
        return [
            FileInfo(path=path, size_bytes=len(data), modified_at=now)
            for path, data in sorted(self.files.items())
        ]

    def open_binary(self, rel: str):
        return io.BytesIO(self.files[rel])

    def stat(self, rel: str) -> FileInfo:
        return FileInfo(
            path=rel, size_bytes=len(self.files[rel]), modified_at=datetime.now(timezone.utc)
        )

    def exists(self, rel: str) -> bool:
        return rel in self.files

    def write_atomic(self, rel: str, data: bytes) -> None:
        self.files[rel] = data

    def delete(self, rel: str) -> None:
        self.files.pop(rel, None)

    def location(self, rel: str) -> str:
        return f"memory://warehouse/{rel}"


def test_two_files_differing_only_by_case_are_reported():
    store = InMemoryWarehouse(
        {"orders.parquet": parquet_bytes(), "Orders.parquet": parquet_bytes()}
    )

    result = scan(store)

    assert len(result.tables) == 1
    assert len(result.problems) == 1
    assert "case-insensitive" in result.problems[0]
    assert result.root == "memory://warehouse"


def test_the_source_slot_names_the_warehouse():
    assert SOURCE == "lakehouse"
