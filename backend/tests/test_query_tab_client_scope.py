"""Query Studio tabs belong to the window that opened them.

Every window numbers its tabs from ``tab-1`` and the tab id used to be the primary
key, so two windows (or two team members on one Obelisk) overwrote each other's SQL.
The stored key is now ``<client>:<tab>`` while the API keeps speaking unprefixed
ids: the header is the only thing a client has to send.
"""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.api.routes_query import _client_scope, _scoped_tab_id, _tab_id_for_client
from app.database import AsyncSessionLocal
from app.main import app
from app.models.query import OpenQueryTab

CLIENT_HEADER = "X-Obelisk-Client-Id"


def _transport() -> ASGITransport:
    return ASGITransport(app=app)


def _client_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _tab(ac: AsyncClient, client_id: str | None, tab_id: str, sql: str, order: int = 0):
    headers = {CLIENT_HEADER: client_id} if client_id else {}
    return ac.post(
        "/api/query/tabs",
        json={"id": tab_id, "title": tab_id, "sql": sql, "tab_order": order},
        headers=headers,
    )


def _list(ac: AsyncClient, client_id: str | None):
    headers = {CLIENT_HEADER: client_id} if client_id else {}
    return ac.get("/api/query/tabs", headers=headers)


def _delete(ac: AsyncClient, client_id: str | None, tab_id: str):
    headers = {CLIENT_HEADER: client_id} if client_id else {}
    return ac.delete(f"/api/query/tabs/{tab_id}", headers=headers)


@pytest.mark.asyncio
async def test_two_clients_saving_the_same_tab_id_keep_their_own_sql():
    tab_id = f"tab-{uuid.uuid4().hex[:8]}"
    client_a, client_b = _client_id("a"), _client_id("b")
    async with AsyncClient(transport=_transport(), base_url="http://test") as ac:
        saved_a = await _tab(ac, client_a, tab_id, "SELECT 'client-a';")
        saved_b = await _tab(ac, client_b, tab_id, "SELECT 'client-b';")
        tabs_a = await _list(ac, client_a)
        tabs_b = await _list(ac, client_b)
        try:
            assert saved_a.status_code == 200 and saved_b.status_code == 200
            # The prefix is a storage detail: the client sees the id it sent.
            assert saved_a.json()["id"] == tab_id
            assert saved_b.json()["id"] == tab_id

            mine_a = [t for t in tabs_a.json() if t["id"] == tab_id]
            mine_b = [t for t in tabs_b.json() if t["id"] == tab_id]
            assert [t["sql"] for t in mine_a] == ["SELECT 'client-a';"]
            assert [t["sql"] for t in mine_b] == ["SELECT 'client-b';"]
        finally:
            await _delete(ac, client_a, tab_id)
            await _delete(ac, client_b, tab_id)

    async with AsyncSessionLocal() as session:
        left = (
            await session.execute(select(OpenQueryTab).where(OpenQueryTab.id.like(f"%{tab_id}")))
        ).scalars().all()
    assert left == [], "deleting a tab must remove both storage rows"


@pytest.mark.asyncio
async def test_a_client_without_the_header_uses_the_default_scope():
    tab_id = f"tab-{uuid.uuid4().hex[:8]}"
    other_client = _client_id("other")
    async with AsyncClient(transport=_transport(), base_url="http://test") as ac:
        saved = await _tab(ac, None, tab_id, "SELECT 'no-header';")
        hidden = await _list(ac, other_client)
        visible_default = await _list(ac, "default")
        try:
            assert saved.status_code == 200
            assert [t["sql"] for t in visible_default.json() if t["id"] == tab_id] == [
                "SELECT 'no-header';"
            ]
            assert [t for t in hidden.json() if t["id"] == tab_id] == []
        finally:
            await _delete(ac, None, tab_id)

    async with AsyncSessionLocal() as session:
        row = await session.get(OpenQueryTab, f"default:{tab_id}")
    assert row is None, "a tab in the default scope must be deleted with no header sent"


@pytest.mark.asyncio
async def test_deleting_a_tab_only_touches_the_callers_scope():
    tab_id = f"tab-{uuid.uuid4().hex[:8]}"
    client_a, client_b = _client_id("a"), _client_id("b")
    async with AsyncClient(transport=_transport(), base_url="http://test") as ac:
        await _tab(ac, client_a, tab_id, "SELECT 'client-a';")
        await _tab(ac, client_b, tab_id, "SELECT 'client-b';")
        try:
            response = await _delete(ac, client_a, tab_id)
            assert response.status_code == 200
            assert [t for t in (await _list(ac, client_a)).json() if t["id"] == tab_id] == []
            assert [t["sql"] for t in (await _list(ac, client_b)).json() if t["id"] == tab_id] == [
                "SELECT 'client-b';"
            ]
        finally:
            await _delete(ac, client_b, tab_id)


@pytest.mark.asyncio
async def test_saving_again_updates_the_tab_instead_of_duplicating_it():
    """The scoped upsert still updates: a window reorder must not create a second row."""
    tab_id = f"tab-{uuid.uuid4().hex[:8]}"
    client_id = _client_id("order")
    async with AsyncClient(transport=_transport(), base_url="http://test") as ac:
        await _tab(ac, client_id, tab_id, "SELECT 1;", order=0)
        await _tab(ac, client_id, tab_id, "SELECT 2;", order=7)
        try:
            tabs = [t for t in (await _list(ac, client_id)).json() if t["id"] == tab_id]
            assert len(tabs) == 1
            assert tabs[0]["sql"] == "SELECT 2;"
            assert tabs[0]["tab_order"] == 7
        finally:
            await _delete(ac, client_id, tab_id)


def test_client_scope_never_produces_an_unsafe_key_prefix():
    """The header is client-controlled: it is sanitised, capped and never empty."""
    assert _client_scope(None) == "default"
    assert _client_scope("") == "default"
    assert _client_scope("   ") == "default"
    assert _client_scope("team one/../../x!") == "teamone....x"
    assert _client_scope("c" * 120) == "c" * 64
    # Idempotent prefixing: a caller that echoes a stored key back cannot double it.
    assert _scoped_tab_id("default", "tab-1") == "default:tab-1"
    assert _scoped_tab_id("default", "default:tab-1") == "default:tab-1"
    # Another client's stored key is never rewritten into this client's namespace.
    assert _tab_id_for_client("other", "default:tab-1") == "default:tab-1"
    assert _tab_id_for_client("default", "default:tab-1") == "tab-1"
