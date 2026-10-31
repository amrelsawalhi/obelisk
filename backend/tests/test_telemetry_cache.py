"""Every connected client shares one telemetry probe.

Each tab polled its own three probes (host specs, allocations, managed containers)
every two seconds; five windows meant ten Docker socket round-trips per second for
numbers that are identical in all of them. The payload is now shared for
``_TELEMETRY_CACHE_TTL_SECONDS`` (just under the two-second poll, so a single client
still gets a fresh sample per tick), and each caller gets its own copy because the
websocket stamps a ``timestamp`` on it.
"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as main_module
from app.main import app

TELEMETRY_KEYS = {
    "cpu_percent",
    "cpu_count",
    "ram_percent_used",
    "total_ram_gb",
    "available_ram_gb",
    "ram_used_gb",
    "max_safe_allocation_gb",
    "active_containers_count",
    "allocated_ram_gb",
    "active_heavy_queries",
    "waiting_heavy_queries",
    "max_concurrent_heavy",
    # The cheap-query pool: previews and point lookups are visible here rather than
    # being counted against the analytical slots.
    "active_light_queries",
    "waiting_light_queries",
    "max_concurrent_light",
    "active_writers",
    "active_by_kind",
    "active_reservations",
    "total_queries_executed",
    "memory_limit_per_slot",
    "active_memory_limit_mb",
    "threads_per_slot",
    "temp_directory",
    "temp_spill_bytes",
    "temp_spill_mb",
    "temp_spill_limit",
    "temp_spill_limit_mb",
    "active_temp_limit_mb",
    "temp_spill_scope",
    "lakehouse_free_gb",
    "lakehouse_percent_used",
    "active_load_tables",
}


@pytest.fixture(autouse=True)
def fresh_cache():
    """Each test starts with an empty cache, and leaves one behind for nobody."""
    main_module._telemetry_cache = None
    yield
    main_module._telemetry_cache = None


@pytest.mark.asyncio
async def test_the_probe_is_reused_within_the_ttl_and_copied_for_each_caller():
    first = await main_module._get_cached_telemetry()
    stamped_at = main_module._telemetry_cache[0]

    second = await main_module._get_cached_telemetry()

    assert main_module._telemetry_cache[0] == stamped_at, (
        "a second caller inside the TTL must not run the probes again"
    )
    assert second == first
    assert second is not first, "callers must not share one mutable dict"

    second["cpu_percent"] = -123.0
    third = await main_module._get_cached_telemetry()
    assert third["cpu_percent"] == first["cpu_percent"], (
        "mutating a returned payload must not corrupt the shared sample"
    )
    assert set(first) == TELEMETRY_KEYS
    assert first["cpu_count"] >= 1
    assert first["total_ram_gb"] > 0


@pytest.mark.asyncio
async def test_the_probe_is_recomputed_once_the_ttl_has_passed():
    await main_module._get_cached_telemetry()
    expired_at = main_module._telemetry_cache[0]

    await asyncio.sleep(main_module._TELEMETRY_CACHE_TTL_SECONDS + 0.2)
    await main_module._get_cached_telemetry()

    assert main_module._telemetry_cache[0] > expired_at


@pytest.mark.asyncio
async def test_concurrent_clients_share_one_sample_without_deadlocking():
    payloads = await asyncio.gather(
        *[main_module._get_cached_telemetry() for _ in range(8)]
    )

    assert all(payload == payloads[0] for payload in payloads)
    assert all(payload is not payloads[0] for payload in payloads[1:])
    assert set(payloads[0]) == TELEMETRY_KEYS
    assert main_module._telemetry_cache[1] == payloads[0], (
        "the sample the callers saw is the one that is cached"
    )


@pytest.mark.asyncio
async def test_http_fallback_serves_the_same_schema_as_the_websocket():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        services = await ac.get("/api/services/telemetry")
        system = await ac.get("/api/system/telemetry")

    assert services.status_code == 200 and system.status_code == 200
    assert set(services.json()) == TELEMETRY_KEYS
    assert set(system.json()) == TELEMETRY_KEYS
    assert "timestamp" not in services.json(), (
        "the timestamp belongs to the websocket frame, not to the shared payload"
    )
    assert services.json()["active_containers_count"] >= 0
