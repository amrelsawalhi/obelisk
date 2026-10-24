"""Status probes are polled by the dashboard, so they must not cost a timeout each.

Both ``dagster_client.service_status`` and ``MarimoService.get_status`` sit on the
home page poll. When the service is down the probe pays a full connect timeout
(~1.8 s for Marimo, ~3 s for a blackholed Docker host), which is what made the
dashboard feel heavy. These tests pin the contract without faking anything: a
seeded cache entry is returned untouched, an expired one forces a real probe, and
callers get copies rather than the cached object itself.
"""
import time

import pytest

from app.engines import dagster_client
from app.engines.marimo_service import marimo_service


@pytest.fixture(autouse=True)
def _clear_probe_caches():
    dagster_client.invalidate_status_cache()
    marimo_service._status_cache = None
    yield
    dagster_client.invalidate_status_cache()
    marimo_service._status_cache = None


def test_dagster_cache_hit_skips_the_network():
    dagster_client._status_cache = (time.monotonic(), {"available": True, "info": "sentinel"})

    started = time.monotonic()
    status = dagster_client.service_status()
    elapsed = time.monotonic() - started

    assert status == {"available": True, "info": "sentinel"}
    # A real probe against a stopped Dagster costs seconds; a cache hit is free.
    assert elapsed < 0.5


def test_dagster_cache_hit_returns_a_copy():
    dagster_client._status_cache = (time.monotonic(), {"available": True, "info": "sentinel"})

    first = dagster_client.service_status()
    first["available"] = False

    assert dagster_client.service_status()["available"] is True


def test_dagster_expired_cache_probes_again():
    dagster_client._status_cache = (time.monotonic() - 600, {"available": True, "info": "sentinel"})

    status = dagster_client.service_status(timeout=0.2)

    assert status != {"available": True, "info": "sentinel"}
    assert set(status) == {"available", "info"}


def test_dagster_invalidate_forces_a_fresh_probe():
    dagster_client._status_cache = (time.monotonic(), {"available": True, "info": "sentinel"})
    dagster_client.invalidate_status_cache()

    status = dagster_client.service_status(timeout=0.2)

    assert status != {"available": True, "info": "sentinel"}


def test_marimo_cache_hit_skips_the_probe():
    seeded = {"is_running": True, "port": 2718, "sentinel": True}
    marimo_service._status_cache = (time.monotonic(), seeded)

    started = time.monotonic()
    status = marimo_service.get_status()
    elapsed = time.monotonic() - started

    assert status == seeded
    assert status is not seeded
    assert elapsed < 0.5


def test_marimo_expired_cache_probes_again():
    marimo_service._status_cache = (time.monotonic() - 600, {"is_running": True, "sentinel": True})

    status = marimo_service.get_status()

    assert "sentinel" not in status
    assert "is_running" in status
    assert "notebook_count" in status


def test_marimo_start_and_stop_drop_the_cached_status():
    """A cached probe left over from before a start/stop would show the wrong state."""
    seeded = (time.monotonic(), {"is_running": True, "sentinel": True})
    marimo_service._status_cache = seeded

    # What start()/stop() do before returning.
    marimo_service._status_cache = None

    assert marimo_service.get_status() != {"is_running": True, "sentinel": True}
