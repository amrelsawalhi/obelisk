"""Slot derivation and the dynamic spill ceiling.

The heavy-slot count used to be a fixed 4 on every machine, and the spill ceiling a
fixed 50 GB wherever the process ran. Both are now derived from the host, so these pin
the arithmetic — including the shapes where the memory guard (not the CPU count) is
what limits the pool.
"""
from __future__ import annotations

import psutil
import pytest

from app.config import (
    MAX_AUTO_HEAVY_SLOTS,
    MIN_AUTO_HEAVY_SLOTS,
    derive_heavy_slots,
    derive_light_slots,
)
from app.core.resource_advisor import advisor


def test_small_and_mid_hosts_keep_the_historical_default() -> None:
    for cpus, pool_mib in ((2, 2200), (4, 7000), (8, 13900), (16, 27800)):
        assert derive_heavy_slots(cpus, pool_mib) == MIN_AUTO_HEAVY_SLOTS


def test_big_hosts_get_more_concurrent_slots() -> None:
    # The tiers the benchmark report describes, resolved by formula instead of by hand.
    assert derive_heavy_slots(32, 55700) == 8
    assert derive_heavy_slots(64, 111400) == 16
    assert derive_heavy_slots(96, 222800) == 24
    assert derive_heavy_slots(256, 890000) == MAX_AUTO_HEAVY_SLOTS


def test_memory_bounds_the_slot_count_before_the_floor_does() -> None:
    # 96 cores but a pool that only fits a handful of 4 GiB slots.
    assert derive_heavy_slots(96, 12000) == MIN_AUTO_HEAVY_SLOTS
    assert derive_heavy_slots(96, 20000) == 4


def test_the_thread_anchor_is_configurable() -> None:
    assert derive_heavy_slots(96, 222800, min_threads_per_slot=8) == 12
    assert derive_heavy_slots(96, 222800, min_threads_per_slot=2) == MAX_AUTO_HEAVY_SLOTS


def test_the_light_pool_is_half_the_heavy_pool_and_capped() -> None:
    assert derive_light_slots(1) == 1
    assert derive_light_slots(4) == 2
    assert derive_light_slots(8) == 4
    assert derive_light_slots(24) == 8
    assert derive_light_slots(32) == 8


def test_the_resolved_pool_matches_what_the_host_has() -> None:
    """``auto`` on this host must land inside the documented bounds."""
    from app.config import settings

    slots = int(settings.DUCKDB_MAX_CONCURRENT_HEAVY)
    assert MIN_AUTO_HEAVY_SLOTS <= slots <= MAX_AUTO_HEAVY_SLOTS
    pool_mib = advisor.get_duckdb_pool_mib()
    assert advisor.get_duckdb_memory_limit_mb(slots) >= 1
    assert advisor.get_duckdb_memory_limit_mb(slots) * slots <= pool_mib + slots


# --------------------------------------------------------------- spill ceiling

def test_the_spill_ceiling_follows_free_space_when_no_cap_is_set(tmp_path) -> None:
    """``DUCKDB_TEMP_SIZE_LIMIT=0`` means "use the disk", not "zero bytes"."""
    free_mib = psutil.disk_usage(str(tmp_path)).free // (1024 ** 2)
    expected = max(1, free_mib * 50 // 100)
    limit = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "0", 1, 50)

    assert limit >= 1024 or free_mib < 2048, "a usable host must never get a tiny ceiling"
    assert abs(limit - expected) <= 2, "the limit must track the free space it sees"


def test_the_free_space_share_is_configurable(tmp_path) -> None:
    quarter = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "", 1, 25)
    half = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "", 1, 50)
    assert quarter <= half


def test_a_configured_cap_still_wins_and_is_divided(tmp_path) -> None:
    assert advisor.get_duckdb_temp_limit_mb(str(tmp_path), "1MB", 4) == 1
    single = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "50GB", 1)
    four = advisor.get_duckdb_temp_limit_mb(str(tmp_path), "50GB", 4)
    assert four == max(1, single // 4)


@pytest.mark.parametrize("configured", ["", "0", "auto"])
def test_no_cap_forms_all_mean_dynamic(tmp_path, configured: str) -> None:
    assert advisor.get_duckdb_temp_limit(str(tmp_path), configured, 1).endswith("MiB")
