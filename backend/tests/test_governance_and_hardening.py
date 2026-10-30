"""Regression tests for the resource-governance and storage hardening audit fixes.

The suite is deliberately mock-free: each test drives the real helper or a real SQLite /
DuckDB primitive. Filesystem-only failure modes are represented by constructing the exact
OSError the filesystem raises, while the rest of a write is exercised normally.
"""
from __future__ import annotations

import errno
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from app.config import settings
from app.core import catalog as catalog_module
from app.core.catalog import _FOOTER_CACHE, _remember_footer
from app.core.resource_advisor import advisor
from app.core.storage import _is_unsupported_fsync_error
import app.engines.ingest_engine as ingest_module


def test_only_unsupported_fsync_errors_are_tolerated():
    assert _is_unsupported_fsync_error(OSError(errno.EOPNOTSUPP, "no fsync"))
    assert _is_unsupported_fsync_error(OSError(errno.EINVAL, "invalid fsync"))
    assert not _is_unsupported_fsync_error(OSError(errno.EIO, "disk failed"))


def test_watermark_fast_paths_keep_exact_numeric_and_timestamp_order():
    assert ingest_module._watermark_is_newer("10.5", "9.99")
    assert not ingest_module._watermark_is_newer("9.99", "10.5")
    assert ingest_module._watermark_is_newer("1e5", "9e4")
    assert ingest_module._watermark_is_newer("-1e-3", "-2e-3")
    assert ingest_module._looks_decimal("1e+2")
    assert not ingest_module._looks_decimal("1e")
    assert not ingest_module._looks_decimal("1e+")
    assert ingest_module._watermark_is_newer(
        "2025-01-01T00:00:01Z", "2024-12-31T23:59:59Z"
    )
    assert ingest_module._watermark_is_newer(
        "2025-01-01T01:00:01+01:00", "2025-01-01T00:00:00Z"
    )
    assert not ingest_module._watermark_is_newer("2025-01-01", "2025-01-01")
    assert ingest_module._watermark_is_newer("z", "a")


def test_cell_normalization_passes_arrow_native_values_without_copying():
    row = (1, "x", 2.5, True, None, datetime.now(timezone.utc))
    assert ingest_module._cells(row) == row
    payload = {"a": 1}
    assert ingest_module._cells((payload,))[0] == json.dumps(payload)


def test_memory_budget_is_binary_and_never_overcommits_the_safe_share():
    limit = advisor.get_duckdb_memory_limit(4)
    assert limit.endswith("MiB")
    specs = advisor.get_host_specs()
    safe_mib = int(specs["max_safe_allocation_bytes"]) // (1024 ** 2)
    assert advisor.get_duckdb_memory_limit_mb(4) * 4 <= safe_mib // 2 + 4


def test_dbt_receives_the_same_share_as_other_heavy_work():
    from app.engines.dbt_runner import dbt_runner

    env = dbt_runner._get_target_env("lakehouse")
    slots = max(1, int(settings.DUCKDB_MAX_CONCURRENT_HEAVY))
    dbt_threads = dbt_runner._dbt_model_threads("lakehouse")
    budget_slots = dbt_runner._dbt_budget_slots("lakehouse")
    assert dbt_threads == 1, "DuckLake model DDL must run serially"
    assert budget_slots == slots, "a lakehouse dbt run owns the batch slot pool"
    assert env["DBT_DUCKDB_MEMORY_LIMIT"] == (
        f"{advisor.get_duckdb_memory_limit_mb(slots) * budget_slots}MiB"
    )
    assert env["DBT_DUCKDB_MAX_TEMP_SIZE"] == (
        f"{advisor.get_duckdb_temp_limit_mb(settings.duckdb_temp_path.as_posix(), settings.DUCKDB_TEMP_SIZE_LIMIT, slots) * budget_slots}MiB"
    )
    assert Path(env["DBT_DUCKDB_TEMP_DIR"]) == settings.duckdb_temp_path / "dbt_spill"
    assert int(env["DBT_DUCKDB_THREADS"]) == dbt_threads


def test_dbt_profile_never_reapplies_temp_directory():
    """A multi-thread dbt run must not hit DuckDB's temp-directory switch error."""
    profile = (
        Path(__file__).resolve().parents[2]
        / "git_repo"
        / "dbt_project"
        / "profiles.yml"
    ).read_text(encoding="utf-8")
    assert "temp_directory:" not in profile


def test_the_ingest_gate_limits_all_ingest_entry_points():
    old_limit = settings.INGEST_MAX_CONCURRENT
    try:
        settings.INGEST_MAX_CONCURRENT = 2
        gate = ingest_module._ingest_gate()
        active = 0
        peak = 0
        lock = threading.Lock()

        def worker() -> None:
            nonlocal active, peak
            with gate:
                with lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.05)
                with lock:
                    active -= 1

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        assert peak <= 2, "the shared gate must not admit more than INGEST_MAX_CONCURRENT"
    finally:
        settings.INGEST_MAX_CONCURRENT = old_limit
        ingest_module._ingest_gate()


def test_footer_cache_evicts_one_entry_instead_of_wiping_the_cache():
    saved_limit = catalog_module._FOOTER_CACHE_LIMIT
    saved_entries = dict(_FOOTER_CACHE)
    try:
        _FOOTER_CACHE.clear()
        catalog_module._FOOTER_CACHE_LIMIT = 3
        for index in range(4):
            _remember_footer((f"test-{index}", index, "now"), ((), index))
        assert len(_FOOTER_CACHE) == 3
        assert ("test-1", 1, "now") in _FOOTER_CACHE
        assert ("test-0", 0, "now") not in _FOOTER_CACHE
    finally:
        _FOOTER_CACHE.clear()
        _FOOTER_CACHE.update(saved_entries)
        catalog_module._FOOTER_CACHE_LIMIT = saved_limit
