"""REST ingestion that pages, retries and authenticates — against a real HTTP server.

The API driver used to fetch one page and call it a source. These tests stand up a real
``ThreadingHTTPServer`` (no mocks, per the suite's discipline) that paginates four
different ways, fails on demand, enforces a bearer token and records every request, then
drive ingestion through it end to end.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from app.engines.api_source import (
    ApiBudgetExceeded,
    ApiCancelled,
    ApiSource,
    ApiSourceError,
    build_plan,
)
from app.engines.duckdb_engine import duckdb_engine
from app.engines.ingest_engine import IngestEngine


class ApiState:
    """What the fake vendor API serves, and everything it was asked for."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict]] = []
        self.bodies: list[dict] = []
        self.rows: list[dict] = []
        self.failures_before_success = 0
        self.rate_limit_once = 0.0
        self.require_token = ""
        self.token_calls = 0

    def page(self, start: int, size: int) -> list[dict]:
        return self.rows[start : start + size]

    def last_query(self) -> dict:
        return self.requests[-1][1] if self.requests else {}


class _Handler(BaseHTTPRequestHandler):
    state: ApiState

    def log_message(self, *args) -> None:  # the suite does not want server chatter
        pass

    def _send(self, payload, status: int = 200, headers: dict | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _guard(self, query: dict) -> bool:
        """Auth, failure and rate-limit simulation, in the order a real API applies it."""
        if self.state.require_token:
            if self.headers.get("Authorization") != f"Bearer {self.state.require_token}":
                self._send({"error": "unauthorized"}, status=401)
                return False
        if self.state.failures_before_success > 0:
            self.state.failures_before_success -= 1
            self._send({"error": "boom"}, status=500)
            return False
        if self.state.rate_limit_once > 0:
            delay = self.state.rate_limit_once
            self.state.rate_limit_once = 0.0
            self._send(
                {"error": "slow down"}, status=429, headers={"Retry-After": str(delay)}
            )
            return False
        return True

    def _record(self, path: str, query: dict) -> None:
        self.state.requests.append((path, query))

    # -- routes ----------------------------------------------------------

    def do_POST(self) -> None:  # noqa: N802 - the name is the HTTP method
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            body = {}
        self.state.bodies.append(body)
        self._record(parsed.path, {})

        if parsed.path == "/token":
            self.state.token_calls += 1
            self._send({"access_token": "tok-abc", "expires_in": 3600, "token_type": "bearer"})
            return
        if parsed.path == "/graphql":
            if not self._guard({}):
                return
            cursor = str((body.get("variables") or {}).get("cursor") or "")
            start = 0 if not cursor else int(cursor.replace("c", ""))
            chunk = self.state.page(start, 2)
            end = start + 2 if start + 2 < len(self.state.rows) else None
            self._send(
                {
                    "data": {
                        "items": chunk,
                        "pageInfo": {"endCursor": f"c{end}" if end is not None else None},
                    }
                }
            )
            return
        self._send({"error": "not found"}, status=404)

    def do_GET(self) -> None:  # noqa: N802 - the name is the HTTP method
        parsed = urlparse(self.path)
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
        self._record(parsed.path, query)
        if not self._guard(query):
            return

        if parsed.path == "/offset/items":
            rows = self.state.rows
            since = query.get("since")
            if since is not None:
                try:
                    boundary = int(since)
                    rows = [row for row in rows if int(row["updated_at"]) > boundary]
                except (TypeError, ValueError):
                    pass
            start = int(query.get("offset") or 0)
            size = int(query.get("limit") or 100)
            self._send(rows[start : start + size])
            return
        if parsed.path == "/page/items":
            page = int(query.get("page") or 1)
            size = int(query.get("per_page") or 100)
            self._send(self.state.page((page - 1) * size, size))
            return
        if parsed.path == "/cursor/items":
            cursor = str(query.get("cursor") or "")
            start = 0 if not cursor else int(cursor.replace("c", ""))
            chunk = self.state.page(start, 2)
            end = start + 2 if start + 2 < len(self.state.rows) else None
            self._send(
                {"items": chunk, "next_cursor": f"c{end}" if end is not None else None}
            )
            return
        if parsed.path == "/link/items":
            page_no = int(query.get("p") or 1)
            start = (page_no - 1) * 2
            chunk = self.state.page(start, 2)
            headers = {}
            if start + 2 < len(self.state.rows):
                headers["Link"] = (
                    f"<http://127.0.0.1:{self.server.server_address[1]}"
                    f'/link/items?p={page_no + 1}>; rel="next"'
                )
            self._send(chunk, headers=headers)
            return
        if parsed.path == "/single/items":
            self._send(self.state.page(0, 100))
            return
        self._send({"error": "not found"}, status=404)


@pytest.fixture()
def api():
    """A real HTTP server; each test gets its own state."""
    state = ApiState()
    handler = type("BoundHandler", (_Handler,), {"state": state})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, state
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def lake():
    engine = IngestEngine()
    schema = f"zz{uuid.uuid4().hex[:6]}"
    conn = duckdb_engine.get_connection()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    try:
        yield engine, conn, schema
    finally:
        try:
            conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        finally:
            conn.close()


def _params(server, **options) -> dict:
    return {
        "host": "127.0.0.1",
        "port": server.server_address[1],
        "options": options,
    }


def _rows(count: int, *, start: int = 1, updated_at_from: int = 0) -> list[dict]:
    return [
        {
            "id": number,
            "amount": float(number * 10),
            "updated_at": updated_at_from + number,
        }
        for number in range(start, start + count)
    ]


# ------------------------------------------------------------------ pagination


def test_offset_pagination_loads_every_page(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(5)

    result = engine.ingest_entity(
        source_type="api",
        connection_params=_params(
            server,
            pagination={
                "type": "offset",
                "page_param": "offset",
                "size_param": "limit",
                "page_size": 2,
            },
        ),
        entity="offset_items",
        endpoint="offset/items",
        mode="snapshot",
        target_folder=schema,
    )

    assert result["num_rows"] == 5, "the whole endpoint, not just its first page"
    table = engine._target_sql(schema, "offset_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5
    assert conn.execute(f"SELECT sum(amount) FROM {table}").fetchone()[0] == 150.0
    assert len(state.requests) == 4, "three data pages plus the empty one that stops it"


def test_page_number_pagination(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(5)

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(
            server,
            pagination={
                "type": "page",
                "page_param": "page",
                "size_param": "per_page",
                "page_size": 2,
                "start": 1,
            },
        ),
        entity="page_items",
        endpoint="page/items",
        mode="snapshot",
        target_folder=schema,
    )

    table = engine._target_sql(schema, "page_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5


def test_cursor_pagination_follows_next_keys(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(5)

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(
            server,
            data_key="items",
            pagination={
                "type": "cursor",
                "cursor_param": "cursor",
                "next_key": "next_cursor",
            },
        ),
        entity="cursor_items",
        endpoint="cursor/items",
        mode="snapshot",
        target_folder=schema,
    )

    table = engine._target_sql(schema, "cursor_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5
    assert [query.get("cursor") for _, query in state.requests] == [None, "c2", "c4"]


def test_link_header_pagination(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(5)

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(server, pagination={"type": "link"}),
        entity="link_items",
        endpoint="link/items",
        mode="snapshot",
        target_folder=schema,
    )

    table = engine._target_sql(schema, "link_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5
    assert len(state.requests) == 3


def test_a_single_request_source_still_works_without_pagination_options(lake, api) -> None:
    """No options means one GET, exactly as before — the compatibility promise."""
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(4)

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(server),
        entity="single_items",
        endpoint="single/items",
        mode="snapshot",
        target_folder=schema,
    )

    table = engine._target_sql(schema, "single_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 4
    assert len(state.requests) == 1


# ------------------------------------------------------------------ resilience


def test_a_transient_failure_is_retried(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(3)
    state.failures_before_success = 2

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(
            server,
            retry={"max_attempts": 4, "backoff_seconds": 0.01, "max_backoff_seconds": 0.05},
            endpoint="single/items",
        ),
        entity="single_items",
        mode="snapshot",
        target_folder=schema,
    )

    table = engine._target_sql(schema, "single_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 3
    assert len(state.requests) == 3, "two failures, then the page that worked"


def test_rate_limit_retry_after_is_honoured(api) -> None:
    server, state = api
    state.rows = _rows(2)
    state.rate_limit_once = 0.2

    source = ApiSource(
        f"http://127.0.0.1:{server.server_address[1]}",
        "single/items",
        options={
            "retry": {"max_attempts": 3, "backoff_seconds": 0.01, "max_backoff_seconds": 5}
        },
    )
    started = time.monotonic()
    pages = list(source.pages())
    elapsed = time.monotonic() - started

    assert len(pages) == 1
    assert elapsed >= 0.2, "Retry-After must be waited out, not ignored"


def test_a_persistent_failure_names_the_endpoint(api) -> None:
    server, state = api
    state.rows = _rows(1)
    state.failures_before_success = 10

    source = ApiSource(
        f"http://127.0.0.1:{server.server_address[1]}",
        "single/items",
        options={
            "retry": {"max_attempts": 2, "backoff_seconds": 0.01, "max_backoff_seconds": 0.05}
        },
    )
    with pytest.raises(ApiSourceError, match="HTTP 500"):
        list(source.pages())


def test_the_page_budget_stops_the_run(api) -> None:
    server, state = api
    state.rows = _rows(10)

    source = ApiSource(
        f"http://127.0.0.1:{server.server_address[1]}",
        "offset/items",
        options={
            "pagination": {"type": "offset", "page_size": 2},
            "budget": {"max_pages": 2},
        },
    )
    with pytest.raises(ApiBudgetExceeded, match="budget.max_pages"):
        list(source.pages())


def test_cancellation_stops_between_pages(api) -> None:
    server, state = api
    state.rows = _rows(10)
    seen: list[int] = []

    source = ApiSource(
        f"http://127.0.0.1:{server.server_address[1]}",
        "offset/items",
        options={"pagination": {"type": "offset", "page_size": 2}},
        should_cancel=lambda: len(seen) >= 1,
    )
    with pytest.raises(ApiCancelled):
        for page in source.pages():
            seen.append(page.index)

    assert seen == [0], "the run stops after the page it was allowed"
    assert len(state.requests) == 1


# ------------------------------------------------------------------------ auth


def test_oauth2_client_credentials_token_is_fetched_and_used(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(3)
    state.require_token = "tok-abc"
    base = f"http://127.0.0.1:{server.server_address[1]}"

    engine.ingest_entity(
        source_type="api",
        connection_params={
            "host": "127.0.0.1",
            "port": server.server_address[1],
            "password": "s3cret",
            "options": {
                "endpoint": "single/items",
                "auth": {
                    "type": "oauth2",
                    "token_url": f"{base}/token",
                    "client_id": "client-1",
                },
            },
        },
        entity="single_items",
        mode="snapshot",
        target_folder=schema,
    )

    assert state.token_calls == 1, "the token endpoint is called once, then cached"
    table = engine._target_sql(schema, "single_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 3


def test_an_api_key_can_live_in_a_query_parameter(api) -> None:
    server, state = api
    state.rows = _rows(1)

    source = ApiSource(
        f"http://127.0.0.1:{server.server_address[1]}",
        "single/items",
        token="key-456",
        options={"auth": {"type": "query", "param": "api_key"}},
    )
    list(source.pages())

    assert state.last_query().get("api_key") == "key-456"


# ---------------------------------------------------------- watermark pushdown


def test_the_watermark_is_pushed_upstream_on_an_incremental_run(lake, api) -> None:
    """The server filters; the platform filters again as the correctness net."""
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(3)
    options = {
        "endpoint": "offset/items",
        "pagination": {"type": "offset", "page_size": 2, "watermark_param": "since"},
    }

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(server, **options),
        entity="offset_items",
        mode="snapshot",
        target_folder=schema,
    )

    state.requests.clear()
    state.rows = _rows(5)
    result = engine.ingest_entity(
        source_type="api",
        connection_params=_params(server, **options),
        entity="offset_items",
        mode="incremental",
        watermark_column="updated_at",
        target_folder=schema,
    )

    assert state.requests[0][1].get("since") == "3", "the delta is asked for, not scraped"
    assert result["new_rows_extracted"] == 2, "only the rows past the stored watermark"
    table = engine._target_sql(schema, "offset_items")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5, (
        "the snapshot's three rows plus the two the incremental run added"
    )


def test_a_graphql_style_body_carries_the_cursor(lake, api) -> None:
    engine, conn, schema = lake
    server, state = api
    state.rows = _rows(5)

    engine.ingest_entity(
        source_type="api",
        connection_params=_params(
            server,
            method="POST",
            body={
                "query": "query($cursor: String) { items(after: $cursor) { id } }",
                "variables": {"cursor": "{cursor}"},
            },
            data_key="data.items",
            pagination={
                "type": "cursor",
                "cursor_param": "cursor",
                "next_key": "data.pageInfo.endCursor",
            },
        ),
        entity="graphql",
        mode="snapshot",
        target_folder=schema,
    )

    cursors = [entry["variables"]["cursor"] for entry in state.bodies]
    assert cursors == ["", "c2", "c4"]
    table = engine._target_sql(schema, "graphql")
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 5


# -------------------------------------------------------------------- the plan


def test_an_unknown_pagination_type_is_refused_not_ignored() -> None:
    """A typo must not quietly load only the first page."""
    with pytest.raises(ApiSourceError, match="Unknown pagination type"):
        build_plan({"pagination": {"type": "cursorr"}})


def test_a_watermark_param_without_a_column_is_dropped() -> None:
    """With no local filter the platform cannot verify what the server returned."""
    plan = build_plan(
        {"pagination": {"type": "offset", "watermark_param": "since"}},
        watermark_column=None,
        watermark_value=10,
    )
    assert plan.watermark_param is None


