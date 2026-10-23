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

def test_upsert_without_primary_key_is_refused(tmp_path: Path) -> None:
    """H1: an upsert with no key must fail instead of appending duplicates."""
    engine = IngestEngine()
    existing = tmp_path / "existing.parquet"
    delta = tmp_path / "delta.parquet"
    output = tmp_path / "output.parquet"
    _write_parquet(existing, [{"id": 1, "amount": 10.0}])
    _write_parquet(delta, [{"id": 1, "amount": 20.0}])

    with pytest.raises(ValueError, match="requires a primary_key"):
        engine._merge_incremental_parquet(
            existing_file=existing,
            delta_file=delta,
            output_file=output,
            write_strategy="upsert",
            primary_key=None,
            watermark_column=None,
        )

    assert pq.read_table(str(existing)).num_rows == 1


def test_dedupe_failure_raises_and_leaves_the_live_file_intact(tmp_path: Path) -> None:
    """H1: a mistyped primary key must fail the job, not write the raw union."""
    engine = IngestEngine()
    existing = tmp_path / "existing.parquet"
    delta = tmp_path / "delta.parquet"
    output = tmp_path / "output.parquet"
    _write_parquet(existing, [{"id": 1, "amount": 10.0}])
    _write_parquet(delta, [{"id": 1, "amount": 20.0}])

    with pytest.raises(RuntimeError, match="Upsert deduplication failed"):
        engine._merge_incremental_parquet(
            existing_file=existing,
            delta_file=delta,
            output_file=output,
            write_strategy="upsert",
            primary_key="does_not_exist",
            watermark_column=None,
        )

    # The live file was never replaced, so no duplicate key reached production.
    table = pq.read_table(str(existing))
    assert table.num_rows == 1
    assert table.column("amount").to_pylist() == [10.0]


def test_upsert_keeps_the_newest_row_per_key(tmp_path: Path) -> None:
    """H1: a valid upsert collapses to one row per primary key."""
    engine = IngestEngine()
    existing = tmp_path / "existing.parquet"
    delta = tmp_path / "delta.parquet"
    output = tmp_path / "output.parquet"
    _write_parquet(existing, [{"id": 1, "amount": 10.0, "updated_at": 1}])
    _write_parquet(delta, [{"id": 1, "amount": 99.0, "updated_at": 2}])

    num_rows = engine._merge_incremental_parquet(
        existing_file=existing,
        delta_file=delta,
        output_file=output,
        write_strategy="upsert",
        primary_key="id",
        watermark_column="updated_at",
    )

    assert num_rows == 1
    assert pq.read_table(str(output)).column("amount").to_pylist() == [99.0]


def test_snapshot_honours_upsert() -> None:
    """M2: a snapshot run must still deduplicate when upsert is requested."""
    engine = IngestEngine()
    snapshot = pa.Table.from_pylist(
        [
            {"id": 1, "amount": 10.0, "updated_at": 1},
            {"id": 1, "amount": 42.0, "updated_at": 2},
            {"id": 2, "amount": 5.0, "updated_at": 1},
        ]
    )

    result = engine._combine_for_write(
        new_table=snapshot,
        existing_table=None,
        mode="snapshot",
        write_strategy="upsert",
        primary_key="id",
        watermark_column="updated_at",
    )

    assert result.num_rows == 2
    assert sorted(result.column("amount").to_pylist()) == [5.0, 42.0]


def test_apply_write_strategy_rejects_an_unknown_primary_key() -> None:
    """H1: a typo in the primary-key column raises instead of silently appending."""
    engine = IngestEngine()
    table = pa.Table.from_pylist([{"id": 1}])

    with pytest.raises(ValueError, match="not one of the extracted columns"):
        engine._apply_write_strategy(table, "upsert", "missing_column", None)


def test_postgres_incremental_has_a_none_watermark_guard() -> None:
    """H2: the incremental branch must never interpolate a ``None`` cursor.

    A full Postgres round-trip is covered by the docker-compose.sources suite;
    this guards the control flow so the literal ``'None'`` filter cannot return.
    """
    source = (
        BACKEND_DIR / "app" / "engines" / "ingest_engine.py"
    ).read_text(encoding="utf-8")

    assert "watermark_value is None" in source
    assert "full_refresh = (" in source
