"""A paginating REST client for the ingest engine.

The API driver used to make exactly one request per ingest. A "REST source" therefore
meant *the first page* of the endpoint: everything past it was silently absent, the
64 MiB response cap was the dataset size, and an incremental run re-downloaded the whole
payload to filter a handful of new rows in Python.

This module owns the parts that make an API a source rather than a sample: walking
pages, retrying the failures every real API throws, authenticating beyond a static
bearer token, pushing the watermark upstream, and stopping at a budget instead of at a
crash. It deliberately knows nothing about DuckDB or the lakehouse — the ingest engine
turns pages into tables, and that separation is what keeps the paging logic testable
against a plain local HTTP server.

Options live on the connection (``Connection.options``); the schema is documented in
``docs/api-ingestion.md`` and mirrored by the presets in ``api_presets.json``. Anything
absent keeps the historical behaviour: one GET, bearer token, no paging.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

import httpx

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 10_000
DEFAULT_MAX_BYTES = 4 * 1024 * 1024 * 1024  # 4 GiB per run, not per response
DEFAULT_MAX_ROWS = 100_000_000
PAGE_RESPONSE_BYTES = 64 * 1024 * 1024  # one page must still fit in memory
NDJSON_PAGE_BYTES = 512 * 1024 * 1024
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_BACKOFF = 1.0
DEFAULT_RETRY_MAX_BACKOFF = 30.0

PAGINATION_KINDS = ("none", "offset", "page", "cursor", "link")


class ApiSourceError(RuntimeError):
    """A REST source could not be read; the message says which page and why."""


class ApiBudgetExceeded(ApiSourceError):
    """A run hit its page/byte ceiling. Named so callers can tell it from a breakage."""


class ApiCancelled(ApiSourceError):
    """The caller asked to stop between pages."""


@dataclass
class ApiPlan:
    """How one API connection is read, resolved once from ``options``.

    ``kind`` is the paging strategy; ``watermark_param`` is orthogonal — when it is set
    the watermark goes upstream on every request, so an incremental run asks the server
    for the delta instead of downloading the dataset and filtering it locally.
    """

    kind: str = "none"
    page_param: str = "offset"
    size_param: str = "limit"
    page_size: int = DEFAULT_PAGE_SIZE
    start_value: int = 0
    cursor_param: str = "cursor"
    next_key: Optional[str] = None
    next_url_key: Optional[str] = None
    # Orthogonal: push the incremental watermark upstream (e.g. ``since``).
    watermark_param: Optional[str] = None
    watermark_value: Any = None
    # Budgets, counted across the whole run.
    max_pages: int = DEFAULT_MAX_PAGES
    max_bytes: int = DEFAULT_MAX_BYTES
    max_rows: int = DEFAULT_MAX_ROWS
    # Failure policy: "fail" (default) or "skip" (log and continue with the next page).
    on_page_error: str = "fail"
    # Retry policy.
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF
    max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    # Static request decoration.
    method: str = "GET"
    body: Optional[Dict[str, Any]] = None
    params: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class ApiPage:
    """One page as the server sent it, plus what paging needs to know."""

    index: int
    url: str
    body: bytes
    is_ndjson: bool = False

    @property
    def is_top_level_array(self) -> bool:
        return self.body.lstrip().startswith(b"[")

    def is_empty(self) -> bool:
        """Whether this page carried no rows, cheaply enough to decide without parsing.

        A page that is a top-level array or NDJSON can be judged from its bytes; a
        wrapped object needs its ``data_key``, which only the consumer knows, so it is
        never reported empty here and the consumer's record count decides.
        """
        stripped = self.body.strip()
        if not stripped:
            return True
        if stripped in (b"[]", b"{}"):
            return True
        if self.is_ndjson:
            return all(not line.strip() for line in stripped.splitlines())
        return False

    def payload(self) -> Any:
        """Parsed JSON for this page (only when a strategy needs to look inside)."""
        try:
            return json.loads(self.body.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            raise ApiSourceError(f"Page {self.index} from {self.url} is not JSON: {exc}") from exc

    def records(self, data_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """The rows on this page, using the platform's one extraction heuristic."""
        from app.engines.introspector import Introspector

        return Introspector._extract_api_records(self.payload(), data_key)


def _json_path(payload: Any, dotted: Optional[str]) -> Any:
    """Follow ``a.b.c`` into a JSON object. Missing segments answer ``None``."""
    if not dotted:
        return None
    current = payload
    for part in str(dotted).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if 0 <= index < len(current) else None
        else:
            return None
    return current


def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    raw = str(response.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def build_plan(
    options: Optional[Dict[str, Any]],
    *,
    watermark_column: Optional[str] = None,
    watermark_value: Any = None,
) -> ApiPlan:
    """Resolve a connection's ``options`` into an :class:`ApiPlan`.

    Unknown paging kinds raise instead of silently degrading to one request: a typo
    that quietly loads only the first page is the bug this module exists to prevent.
    """
    options = options or {}
    pagination = options.get("pagination") or {}
    retry = options.get("retry") or {}
    budget = options.get("budget") or {}

    kind = str(pagination.get("type") or "none").strip().lower()
    if kind not in PAGINATION_KINDS:
        raise ApiSourceError(
            f"Unknown pagination type {kind!r}; expected one of {list(PAGINATION_KINDS)}."
        )

    headers = {"Accept": "application/json"}
    for key, value in (options.get("headers") or {}).items():
        headers[str(key)] = str(value)

    watermark_param = pagination.get("watermark_param")
    plan = ApiPlan(
        kind=kind,
        page_param=str(pagination.get("page_param") or ("offset" if kind == "offset" else "page")),
        size_param=str(pagination.get("size_param") or "limit"),
        page_size=_positive_int(pagination.get("page_size"), DEFAULT_PAGE_SIZE),
        start_value=int(pagination.get("start") or 0),
        cursor_param=str(pagination.get("cursor_param") or "cursor"),
        next_key=pagination.get("next_key") or None,
        next_url_key=pagination.get("next_url_key") or None,
        watermark_param=str(watermark_param).strip() if watermark_param else None,
        watermark_value=watermark_value,
        max_pages=_positive_int(budget.get("max_pages"), DEFAULT_MAX_PAGES),
        max_bytes=_positive_int(budget.get("max_bytes"), DEFAULT_MAX_BYTES),
        max_rows=_positive_int(budget.get("max_rows"), DEFAULT_MAX_ROWS),
        on_page_error=str(options.get("on_page_error") or "fail").strip().lower(),
        max_attempts=_positive_int(retry.get("max_attempts"), DEFAULT_RETRY_ATTEMPTS),
        backoff_seconds=float(retry.get("backoff_seconds") or DEFAULT_RETRY_BACKOFF),
        max_backoff_seconds=float(
            retry.get("max_backoff_seconds") or DEFAULT_RETRY_MAX_BACKOFF
        ),
        timeout_seconds=float(options.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS),
        method=str(options.get("method") or "GET").strip().upper(),
        body=options.get("body") if isinstance(options.get("body"), dict) else None,
        params={str(k): v for k, v in (options.get("params") or {}).items()},
        headers=headers,
    )
    if plan.on_page_error not in ("fail", "skip"):
        plan.on_page_error = "fail"
    if not watermark_column:
        # Without a local filter there is nothing to compare against, so a param alone
        # would be a value the server decides on and the platform cannot verify.
        plan.watermark_param = None
    return plan


class _TokenCache:
    """OAuth2 access tokens, kept per (token_url, client_id) for the process lifetime."""

    _lock = threading.Lock()
    _tokens: Dict[tuple, tuple] = {}

    @classmethod
    def get(cls, key: tuple) -> Optional[str]:
        with cls._lock:
            entry = cls._tokens.get(key)
        if entry is None:
            return None
        token, expires_at = entry
        # Refresh a minute early so a long page walk does not lose its token midway.
        return token if expires_at - 60 > time.time() else None

    @classmethod
    def put(cls, key: tuple, token: str, expires_in: Any) -> None:
        try:
            ttl = float(expires_in)
        except (TypeError, ValueError):
            ttl = 3600.0
        with cls._lock:
            cls._tokens[key] = (token, time.time() + max(60.0, ttl))


class ApiSource:
    """Pages, retries and authentication for one connection.

    ``pages()`` is a generator: the body of each page is yielded as it arrives, so the
    caller can write it out immediately instead of holding the dataset in memory.
    """

    def __init__(
        self,
        base_url: str,
        endpoint: str = "",
        *,
        token: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        watermark_column: Optional[str] = None,
        watermark_value: Any = None,
        should_cancel: Optional[Callable[[], bool]] = None,
        logger: Any = None,
    ) -> None:
        self.base_url = self._normalise_base(base_url)
        self.endpoint = "" if endpoint in ("", "api_records") else endpoint
        self.token = token
        self.options = options or {}
        self.plan = build_plan(
            self.options,
            watermark_column=watermark_column,
            watermark_value=watermark_value,
        )
        self.should_cancel = should_cancel
        self.logger = logger
        self._oauth_token: Optional[str] = None
        self._client: Optional[httpx.Client] = None

    # ------------------------------------------------------------------ plumbing

    @staticmethod
    def _normalise_base(base_url: str) -> str:
        base = str(base_url or "").strip() or "http://localhost"
        if not base.startswith(("http://", "https://")):
            base = f"http://{base}"
        return base

    def _url(self, endpoint: Optional[str] = None) -> str:
        suffix = (endpoint if endpoint is not None else self.endpoint).lstrip("/")
        return f"{self.base_url.rstrip('/')}/{suffix}" if suffix else self.base_url

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self.logger is not None:
            getattr(self.logger, level, self.logger.info)(message, *args)

    def _check_cancel(self) -> None:
        if self.should_cancel is not None and self.should_cancel():
            raise ApiCancelled("Ingestion cancelled: stopped between API pages.")

    # -------------------------------------------------------------------- auth

    def _oauth_client_params(self) -> Dict[str, Any]:
        auth = self.options.get("auth") or {}
        return {
            "token_url": str(auth.get("token_url") or "").strip(),
            "client_id": str(auth.get("client_id") or "").strip(),
            "client_secret": str(auth.get("client_secret") or self.token or ""),
            "scope": str(auth.get("scope") or "").strip(),
        }

    def _fetch_oauth_token(self, client: httpx.Client) -> str:
        params = self._oauth_client_params()
        if not params["token_url"] or not params["client_id"]:
            raise ApiSourceError(
                "auth.type=oauth2 needs auth.token_url and auth.client_id in the "
                "connection options; the secret may come from the connection password."
            )
        key = (params["token_url"], params["client_id"])
        cached = _TokenCache.get(key)
        if cached:
            return cached
        form = {
            "grant_type": "client_credentials",
            "client_id": params["client_id"],
            "client_secret": params["client_secret"],
        }
        if params["scope"]:
            form["scope"] = params["scope"]
        try:
            response = client.post(params["token_url"], data=form)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 - surfaced with the endpoint named
            raise ApiSourceError(
                f"Could not obtain an OAuth2 token from {params['token_url']}: {exc}"
            ) from exc
        token = str(payload.get("access_token") or "")
        if not token:
            raise ApiSourceError(
                f"The token endpoint {params['token_url']} answered without access_token."
            )
        _TokenCache.put(key, token, payload.get("expires_in"))
        return token

    def _apply_auth(
        self, client: httpx.Client, headers: Dict[str, str], params: Dict[str, Any]
    ) -> Dict[str, str]:
        """Decorates the request with the configured credential scheme."""
        auth = self.options.get("auth") or {}
        kind = str(auth.get("type") or ("bearer" if self.token else "none")).strip().lower()
        if kind in ("", "none"):
            return headers
        if kind == "bearer":
            value = str(auth.get("token") or self.token or "")
            if value:
                headers["Authorization"] = f"Bearer {value}"
            return headers
        if kind == "basic":
            return headers  # httpx takes the pair as auth=(user, secret)
        if kind in ("header", "api_key"):
            name = str(auth.get("header") or "X-API-Key")
            value = str(auth.get("token") or self.token or "")
            if value:
                headers[name] = value
            return headers
        if kind == "query":
            name = str(auth.get("param") or "api_key")
            value = str(auth.get("token") or self.token or "")
            if value:
                params[name] = value
            return headers
        if kind in ("oauth2", "oauth2_cc", "oauth2_refresh"):
            self._oauth_token = self._oauth_token or self._fetch_oauth_token(client)
            headers["Authorization"] = f"Bearer {self._oauth_token}"
            return headers
        raise ApiSourceError(
            f"Unknown auth.type {kind!r}; expected bearer, basic, header, query or oauth2."
        )

    # --------------------------------------------------------------- transport

    def _request(
        self,
        client: httpx.Client,
        url: str,
        params: Dict[str, Any],
        body: Optional[Dict[str, Any]],
    ) -> httpx.Response:
        """One logical request, retried on the failures real APIs produce."""
        plan = self.plan
        attempt = 0
        refreshed = False
        while True:
            attempt += 1
            self._check_cancel()
            request_params = {**plan.params, **{k: v for k, v in params.items() if v is not None}}
            headers = self._apply_auth(client, dict(plan.headers), request_params)
            auth = self.options.get("auth") or {}
            basic = None
            if str(auth.get("type") or "").lower() == "basic":
                basic = (
                    str(auth.get("username") or ""),
                    str(auth.get("password") or self.token or ""),
                )
            method = plan.method
            try:
                response = client.request(
                    method,
                    url,
                    headers=headers,
                    # ``{}`` would *replace* the query string httpx already parsed out
                    # of a next-link URL, so an empty merge must stay ``None``.
                    params=request_params or None,
                    json=body,
                    auth=basic,
                )
            except httpx.HTTPError as exc:
                if attempt >= plan.max_attempts:
                    raise ApiSourceError(
                        f"{method} {url} failed after {attempt} attempts: {exc}"
                    ) from exc
                self._sleep_before_retry(attempt, None, reason=str(exc))
                continue

            status = response.status_code
            if (
                status == 401
                and not refreshed
                and str(auth.get("type") or "").lower()
                in ("oauth2", "oauth2_cc", "oauth2_refresh")
            ):
                # The cached token expired or was revoked: fetch once, retry once.
                refreshed = True
                response.close()
                self._oauth_token = self._fetch_oauth_token(client)
                continue
            if status in (408, 425, 429) or 500 <= status < 600:
                if attempt >= plan.max_attempts:
                    response.close()
                    raise ApiSourceError(
                        f"{method} {url} answered HTTP {status} on all {attempt} attempts."
                    )
                retry_after = _retry_after_seconds(response)
                response.close()
                self._sleep_before_retry(attempt, retry_after, reason=f"HTTP {status}")
                continue
            if status >= 400:
                detail = response.text[:300].replace(chr(10), " ")
                response.close()
                raise ApiSourceError(f"{method} {url} answered HTTP {status}: {detail}")
            return response

    def _sleep_before_retry(
        self, attempt: int, retry_after: Optional[float], *, reason: str
    ) -> None:
        ceiling = max(0.0, float(self.plan.max_backoff_seconds))
        delay = min(ceiling, float(self.plan.backoff_seconds) * (2 ** (attempt - 1)))
        if retry_after is not None:
            delay = min(ceiling, max(delay, retry_after))
        self._log(
            "warning",
            "API request failed (%s); retrying in %.1fs (attempt %d of %d).",
            reason,
            delay,
            attempt,
            self.plan.max_attempts,
        )
        # Sleep in slices so a cancel lands promptly instead of after a long backoff.
        deadline = time.monotonic() + delay
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            self._check_cancel()
            time.sleep(min(0.2, remaining))

    # ------------------------------------------------------------------- pages

    def pages(self) -> Iterator[ApiPage]:
        """Yields pages until the strategy ends, the budget is hit or the caller cancels."""
        plan = self.plan
        is_ndjson = str(self.options.get("format", "")).lower() in ("ndjson", "jsonl")
        total_bytes = 0
        page_count = 0
        position = plan.start_value
        cursor: Any = None
        next_url: Optional[str] = None

        self._client = httpx.Client(timeout=plan.timeout_seconds, follow_redirects=True)
        try:
            while True:
                self._check_cancel()
                if page_count >= plan.max_pages:
                    raise ApiBudgetExceeded(
                        f"Stopped after {page_count} pages (budget.max_pages={plan.max_pages}). "
                        "Raise the budget or narrow the request."
                    )
                params: Dict[str, Any] = {}
                if plan.watermark_param and plan.watermark_value is not None:
                    params[plan.watermark_param] = plan.watermark_value
                if plan.kind in ("offset", "page"):
                    params[plan.page_param] = position
                    params[plan.size_param] = plan.page_size
                elif plan.kind == "cursor" and cursor is not None:
                    params[plan.cursor_param] = cursor

                url = next_url or self._url()
                body = self._render_body(params)
                try:
                    response = self._request(self._client, url, params, body)
                except ApiSourceError:
                    if plan.on_page_error == "skip" and plan.kind in ("offset", "page"):
                        self._log("warning", "Skipping page at %s after a failed request.", url)
                        page_count += 1
                        position += 1 if plan.kind == "page" else plan.page_size
                        continue
                    raise

                link_header = str(response.headers.get("Link") or "")
                try:
                    raw = self._read_body(response, is_ndjson)
                finally:
                    response.close()

                page = ApiPage(index=page_count, url=url, body=raw, is_ndjson=is_ndjson)
                total_bytes += len(raw)
                if total_bytes > plan.max_bytes:
                    raise ApiBudgetExceeded(
                        f"Stopped after {total_bytes // (1024 ** 2)} MiB "
                        f"(budget.max_bytes={plan.max_bytes // (1024 ** 2)} MiB). "
                        "Raise the budget or enable the watermark pushdown."
                    )
                page_count += 1
                yield page

                if page.is_empty():
                    return
                if plan.kind == "offset":
                    position += plan.page_size
                elif plan.kind == "page":
                    # Page numbers count pages, not rows.
                    position += 1
                elif plan.kind == "cursor":
                    cursor = _json_path(page.payload(), plan.next_key or "next_cursor")
                    if cursor in (None, ""):
                        return
                elif plan.kind == "link":
                    link = ""
                    for part in link_header.split(","):
                        if 'rel="next"' in part or "rel=next" in part:
                            link = part.split(";")[0].strip().strip("<>").strip()
                            break
                    if not link and plan.next_url_key:
                        link = str(_json_path(page.payload(), plan.next_url_key) or "")
                    if not link:
                        return
                    from urllib.parse import urljoin

                    next_url = urljoin(url, link)
                else:  # "none": the response was the dataset
                    return
        finally:
            if self._client is not None:
                self._client.close()
                self._client = None

    def _render_body(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """POST/GraphQL bodies with ``{cursor}``/``{watermark}`` placeholders."""
        if self.plan.method == "GET" or self.plan.body is None:
            return None
        cursor_value = str(params.get(self.plan.cursor_param) or "")
        watermark = str(self.plan.watermark_value or "")

        def render(value: Any) -> Any:
            if isinstance(value, str):
                return value.replace("{watermark}", watermark).replace("{cursor}", cursor_value)
            if isinstance(value, dict):
                return {k: render(v) for k, v in value.items()}
            if isinstance(value, list):
                return [render(v) for v in value]
            return value

        rendered = render(self.plan.body)
        return rendered if isinstance(rendered, dict) else None

    def _read_body(self, response: httpx.Response, is_ndjson: bool) -> bytes:
        """Reads a page, bounded by the per-page ceiling (the run budget is separate)."""
        ceiling = NDJSON_PAGE_BYTES if is_ndjson else PAGE_RESPONSE_BYTES
        chunks: List[bytes] = []
        size = 0
        for chunk in response.iter_bytes(chunk_size=65536):
            self._check_cancel()
            size += len(chunk)
            if size > ceiling:
                raise ApiSourceError(
                    f"A single page from {response.request.url} exceeded "
                    f"{ceiling // (1024 ** 2)} MiB. Ask for a smaller page size."
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @property
    def is_single_request(self) -> bool:
        """Whether this plan performs one request, so ordering/limit pushdown applies."""
        return self.plan.kind == "none"


PRESETS_PATH = Path(__file__).with_name("api_presets.json")


def load_presets() -> List[Dict[str, Any]]:
    """The connector recipes shipped with the platform.

    A preset is *data* — an option block with a name — so one can be added without code
    and every one is validated against :func:`build_plan` by the test suite. That keeps
    "we support GitHub/Shopify/HubSpot" an honest claim rather than a marketing one.
    """
    try:
        payload = json.loads(PRESETS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    presets = payload.get("presets")
    return [item for item in presets if isinstance(item, dict)] if isinstance(presets, list) else []
