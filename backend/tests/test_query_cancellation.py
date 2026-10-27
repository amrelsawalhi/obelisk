"""Stopping a running query, from the API rather than from the timeout.

Query Studio had no Stop button because the backend had no handle on the running
statement: the connection lived inside a worker thread until the request returned. The
registry publishes it under the client's ``query_id``, and ``POST /api/query/{id}/cancel``
calls ``interrupt()`` on it.

Everything here is real: a real DuckDB connection running a real (long) query, and the
real HTTP route.
"""
from __future__ import annotations

import threading
import time

import duckdb
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.services.query_registry import query_registry


@pytest.mark.asyncio
async def test_cancelling_an_unknown_query_is_not_an_error():
    """A click that arrives late (the query already finished) must not look like failure."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post("/api/query/definitely-not-running/cancel")

    assert res.status_code == 200
    assert res.json()["status"] == "not_running"


def test_the_registry_interrupts_the_registered_connection():
    """The registry holds a real DuckDB connection and stops the statement on it."""
    query_id = "probe-interrupt"
    conn = duckdb.connect()
    failure: list[BaseException] = []

    def long_query() -> None:
        try:
            conn.execute("SELECT count(*) FROM range(1000000000000);").fetchall()
        except BaseException as exc:
            failure.append(exc)

    try:
        query_registry.register(query_id, conn)

        worker = threading.Thread(target=long_query, daemon=True)
        worker.start()
        time.sleep(0.4)  # let the statement start before interrupting it

        assert query_registry.cancel(query_id) == "cancelled"
        worker.join(timeout=15.0)

        assert not worker.is_alive(), "the statement outlived the cancel"
        assert failure, "an interrupted statement must not return a result"
    finally:
        query_registry.release(query_id)
        conn.close()


@pytest.mark.asyncio
async def test_the_cancel_route_reaches_a_registered_query():
    """End to end over HTTP: register, POST the cancel route, the statement stops."""
    query_id = "probe-route"
    transport = ASGITransport(app=app)
    conn = duckdb.connect()
    failure: list[BaseException] = []

    def long_query() -> None:
        try:
            conn.execute("SELECT count(*) FROM range(1000000000000);").fetchall()
        except BaseException as exc:
            failure.append(exc)

    try:
        query_registry.register(query_id, conn)
        worker = threading.Thread(target=long_query, daemon=True)
        worker.start()
        time.sleep(0.4)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            res = await client.post(f"/api/query/{query_id}/cancel")

        assert res.status_code == 200
        assert res.json()["status"] == "cancelled"
        worker.join(timeout=15.0)
        assert not worker.is_alive()
        assert failure
    finally:
        query_registry.release(query_id)
        conn.close()


def test_releasing_a_query_removes_the_handle():
    """The registry does not leak entries: a finished query is released, not remembered."""
    query_id = "probe-release"
    conn = duckdb.connect()
    try:
        query_registry.register(query_id, conn)
        assert query_registry.is_running(query_id)

        query_registry.release(query_id)

        assert not query_registry.is_running(query_id)
        assert query_registry.cancel(query_id) == "not_running"
    finally:
        conn.close()
