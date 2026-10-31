# REST / API ingestion

Obelisk reads REST endpoints through the same ingest engine as databases and files:
same write strategies, same lakehouse catalog, same progress and cancellation. What
differs is the connection's `options` block, which is where paging, authentication and
failure handling are configured.

Nothing in this file is required. A connection with no options behaves exactly as it
always has: one `GET`, bearer token taken from the connection password, one page.

## The short version

```jsonc
{
  "endpoint": "/v1/customers",        // request path, when it differs from the entity name
  "auth":     { "type": "bearer" },
  "headers":  { "Accept": "application/json" },
  "data_key": "results",              // where the rows live (dotted paths allowed)
  "pagination": {
    "type": "cursor",
    "cursor_param": "cursor",
    "next_key": "next_cursor",
    "page_size": 100
  },
  "retry":  { "max_attempts": 4, "backoff_seconds": 1.0, "max_backoff_seconds": 30 },
  "budget": { "max_pages": 1000, "max_bytes": 268435456, "max_rows": 10000000 },
  "on_page_error": "fail"
}
```

## Pagination

| `pagination.type` | What it does | Needed keys |
| :--- | :--- | :--- |
| `none` (default) | One request is the dataset. | — |
| `offset` | Sends `offset`/`limit`, adding `page_size` per page until a page comes back empty. | `page_param`, `size_param`, `page_size` |
| `page` | Sends `page`/`per_page`, counting by **1** per page. | `page_param`, `size_param`, `page_size`, `start` (usually `1`) |
| `cursor` | Sends the cursor param, reads the next one from the response, stops when it is null. | `cursor_param`, `next_key` |
| `link` | Follows `Link: <…>; rel="next"` (or a URL in the body via `next_url_key`). | — |

A page is the unit of work: it is parsed, written into the load table, and released
before the next request, so memory is bounded by the page size rather than the dataset.
`page_size` is what the request asks for — when a server caps it lower, the strategies
that detect an empty page still terminate correctly.

## Authentication

`auth.type` may be:

- `bearer` (default when a password/token is set) — `Authorization: Bearer <token>`
- `basic` — `auth.username` + the connection password
- `header` — `auth.header` (default `X-API-Key`), value from `auth.token` or the password
- `query` — `auth.param` (default `api_key`), value from `auth.token` or the password
- `oauth2` — client-credentials against `auth.token_url` with `auth.client_id` and
  `auth.client_secret` (which falls back to the connection password, so the secret
  stays in the encrypted column rather than in `options`). One token is fetched, cached
  for the process, and refreshed once on a `401`.

Prefer putting the secret in the connection's password field: `options` is not encrypted.

## Incremental runs

With `mode: incremental` and a watermark column, the platform reads the highest value
already stored, sends it upstream as `pagination.watermark_param` (for example
`"since"`), and — as the correctness net — still filters the returned rows locally.
The upstream parameter is what makes an incremental run cost the delta; the local
filter is what makes it correct when a server ignores the parameter.

`watermark_param` is dropped if no watermark column is given: a value the platform
cannot verify is worse than no value at all.

## Failure handling

- Retries: `408`, `425`, `429` and every `5xx` are retried with exponential backoff,
  honouring `Retry-After` when present. `retry.max_attempts` defaults to 4.
- Budgets count across the whole run, not per response: `max_pages`, `max_bytes`,
  `max_rows`. Hitting one raises an error naming the limit.
- `on_page_error: "skip"` continues past a failed page for `offset`/`page` sources
  (where the next page is still addressable) and logs it; the default is `fail`.
- One page is still capped in memory (64 MiB, 512 MiB for NDJSON); ask for a smaller
  page size instead of raising it.

## Presets

`GET /api/catalog/api-presets` returns the shipped recipes (GitHub, Shopify, HubSpot,
Notion, Linear/GraphQL, and three generic paging shapes). A preset is only an `options`
block with a name: choosing one fills the form, and the test suite validates every
preset against the same schema ingestion uses.

## Options reference

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `endpoint` | the entity name | Request path. Needed when the path nests (`/v1/customers`), because an entity is a table name and cannot contain `/`. |
| `method` | `GET` | `GET` or `POST` (GraphQL and search endpoints). |
| `body` | — | POST body; `{cursor}` and `{watermark}` are substituted per page. |
| `headers` | `Accept: application/json` | Extra request headers. |
| `params` | — | Static query parameters sent with every request. |
| `data_key` | — | Where the rows live in the response; dotted paths (`data.items`) work. |
| `format` | JSON | `ndjson` streams each page straight into DuckDB without Python objects. |
| `timeout_seconds` | `30` | Per-request timeout. |
