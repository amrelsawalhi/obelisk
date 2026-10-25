"""Regression tests for storage-URI resolution and write-path atomicity.

Covers audit findings:
  B1 — ``s3://`` was silently rewritten onto local disk while ``s3://`` paths
       were recorded in the catalog.
  B2 — custom ``STORAGE_URI`` values were hijacked by substring heuristics, and
       the test harness itself wrote nested ``bronze/bronze/`` directories.
  H1 — a failed upsert deduplication silently fell back to ``UNION ALL`` and
       wrote duplicate primary keys into the live Parquet file.
  H2 — a missing watermark made Postgres ingest filter on the literal ``'None'``.
  M2 — snapshot runs ignored ``write_strategy="upsert"``.
  M14 — both SQLite URL spellings must resolve to one absolute file.

Zero mocks: every case builds real Parquet files and real Settings objects.
"""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.config import Settings
from app.engines.ingest_engine import IngestEngine

DB_URL = "sqlite+aiosqlite:///data/obelisk.db"
BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BACKEND_DIR.parent


def _settings(storage_uri: str) -> Settings:
    return Settings(STORAGE_URI=storage_uri, DATABASE_URL=DB_URL)


def _write_parquet(path: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="SNAPPY")


# --------------------------------------------------------------------------
# 0.1 — storage resolution
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/lakehouse",
        "gs://bucket/lakehouse",
        "abfss://account/lakehouse",
    ],
)
def test_remote_storage_uris_are_rejected(uri: str) -> None:
    """B1: object-store URIs must fail loudly, never degrade to local disk."""
    with pytest.raises(RuntimeError, match="not implemented yet"):
        _settings(uri).validate_storage_uri()


def test_custom_mount_ending_in_data_lakehouse_is_not_hijacked(tmp_path: Path) -> None:
    """B2: ``.../data/lakehouse`` must resolve where it says, not into the repo."""
    target = tmp_path / "custom" / "data" / "lakehouse"
    target.mkdir(parents=True)

    resolved = _settings(target.as_uri()).lakehouse_root_path

    assert resolved == target.resolve()
    assert resolved != (PROJECT_ROOT / "data" / "lakehouse").resolve()


def test_container_default_maps_into_checkout_only_when_absent() -> None:
    """B2: ``file:///data/lakehouse`` is the image default, not a real host path."""
    if Path("/data/lakehouse").exists():
        pytest.skip("/data/lakehouse exists on this host; the remap must not apply")

    resolved = _settings("file:///data/lakehouse").lakehouse_root_path
    assert resolved == (PROJECT_ROOT / "data" / "lakehouse").resolve()


def test_parent_traversal_is_refused() -> None:
    """B2: a STORAGE_URI cannot escape its own root."""
    with pytest.raises(RuntimeError, match=r"\.\."):
        _settings("file:///data/lakehouse/../../etc").lakehouse_root_path


def test_sqlite_urls_resolve_to_one_absolute_file() -> None:
    """M14: both SQLite spellings must be treated as the same brain."""
    for url in ("sqlite+aiosqlite:///data/obelisk.db", "sqlite:///data/obelisk.db"):
        settings = Settings(STORAGE_URI="file:///data/lakehouse", DATABASE_URL=url)
        assert settings.is_sqlite is True
        assert Path(settings.sqlite_db_path).is_absolute()

    non_sqlite = Settings(
        STORAGE_URI="file:///data/lakehouse",
        DATABASE_URL="postgresql+asyncpg://user@host/db",
    )
    assert non_sqlite.is_sqlite is False


# --------------------------------------------------------------------------
# 0.2 — write-path integrity
# --------------------------------------------------------------------------
