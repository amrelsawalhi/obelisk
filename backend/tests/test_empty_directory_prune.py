"""A table's directory is not a folder, and an empty one is nobody's.

DuckLake derives a table's data directory from its schema (``<root>/<schema>/<table>``),
and nothing removes it when the table goes: a drop, a rename or an expired snapshot leaves
an empty directory that a walk of the warehouse reports as a folder. These tests pin what
the sweep may remove and what it must leave alone — a folder a user created carries a
``.gitkeep``, and the platform's dot-directories are its own.
"""
from __future__ import annotations

from pathlib import Path

from app.core.storage import prune_empty_dirs


def _tree(root: Path, paths: list[str]) -> None:
    """Creates each directory, plus a file in any path that ends with a name."""
    for rel in paths:
        (root / rel).mkdir(parents=True, exist_ok=True)


def test_the_sweep_removes_empty_directories_bottom_up(tmp_path: Path) -> None:
    """A directory whose children all went is itself empty, so one pass clears the branch."""
    root = tmp_path / "lakehouse"
    _tree(root, ["raw/orders", "raw/nested/deep"])

    removed = prune_empty_dirs(root)

    assert removed == 4, "two leaves, the folder between them and the folder above"
    assert list(root.iterdir()) == []


def test_a_directory_holding_a_file_is_left_alone(tmp_path: Path) -> None:
    """Only empty directories go: Parquet belongs to a snapshot, and cleanup reclaims it."""
    root = tmp_path / "lakehouse"
    _tree(root, ["raw/orders"])
    (root / "raw" / "orders" / "ducklake-1.parquet").write_bytes(b"data")

    assert prune_empty_dirs(root) == 0
    assert (root / "raw" / "orders" / "ducklake-1.parquet").is_file()


def test_a_folder_marked_with_gitkeep_is_a_folder(tmp_path: Path) -> None:
    """``.gitkeep`` is how the API says "a user made this": it must survive the sweep."""
    root = tmp_path / "lakehouse"
    _tree(root, ["staging"])
    (root / "staging" / ".gitkeep").touch()

    assert prune_empty_dirs(root) == 0
    assert (root / "staging" / ".gitkeep").is_file()


def test_dot_directories_and_their_parents_are_kept(tmp_path: Path) -> None:
    """The platform's own bookkeeping is not a leftover, and its parent is not empty."""
    root = tmp_path / "lakehouse"
    _tree(root, [".obelisk/tmp"])

    assert prune_empty_dirs(root) == 0
    assert (root / ".obelisk" / "tmp").is_dir()


def test_start_bounds_the_sweep_to_one_subtree(tmp_path: Path) -> None:
    """An ingest or a delete knows where it worked; it must not sweep the whole lake."""
    root = tmp_path / "lakehouse"
    _tree(root, ["raw/orders", "sales/orders"])

    removed = prune_empty_dirs(root, root / "raw")

    assert removed == 2
    assert not (root / "raw").exists()
    assert (root / "sales" / "orders").is_dir(), "the other folder is untouched"


def test_a_start_outside_the_warehouse_removes_nothing(tmp_path: Path) -> None:
    """The warehouse root is the boundary, so a path that escapes it is refused."""
    root = tmp_path / "lakehouse"
    elsewhere = tmp_path / "elsewhere" / "empty"
    _tree(root, ["raw"])
    _tree(elsewhere.parent, [elsewhere.name])

    assert prune_empty_dirs(root, elsewhere) == 0
    assert elsewhere.is_dir()


def test_a_dry_run_counts_what_it_would_remove(tmp_path: Path) -> None:
    """Maintenance reports before it deletes: the count has to be the same either way."""
    root = tmp_path / "lakehouse"
    _tree(root, ["raw/orders", "raw/nested/deep"])

    would_remove = prune_empty_dirs(root, dry_run=True)

    assert would_remove == 4
    assert (root / "raw" / "nested" / "deep").is_dir(), "a dry run changes nothing"
    assert prune_empty_dirs(root) == would_remove
