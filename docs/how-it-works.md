# How it works

Obelisk is one FastAPI process on one host, one SQLite control-plane database, and a
lakehouse of Parquet files. This page is the map: what the pieces are, what each owns, and
where the seams sit. It describes the shipped code, not an aspiration.

## Where things live

| Path | Owns |
| :--- | :--- |
| `backend/app/api/` | HTTP and WebSocket surface. Routers do routing and validation; almost no logic. |
| `backend/app/engines/` | The workers: `duckdb_engine`, `ingest_engine`, `pipeline_engine`, `dbt_runner`, `introspector`. |
| `backend/app/core/` | Decisions made once: `catalog`, `storage`, `sources`, `security`, `vault`, `host_resolver`. |
| `backend/app/services/` | Shared operations over the database: catalog reconcile, connection resolution. |
| `backend/app/sdk/` | The `obelisk` module notebooks and scripts import. |
| `backend/app/models/` | SQLAlchemy tables, and nothing else. |
| `backend/alembic/` | Migrations. They run on startup, so an upgrade is "replace the code, restart". |
| `frontend/src/` | React 18 + Vite SPA. Monaco for SQL and dbt, xterm for the terminal, lazy-loaded. |
| `git_repo/` | The code the platform runs: `scripts/`, `notebooks/`, `dbt_project/`. |
| `data/` | `obelisk.db` (the brain) and `lakehouse/<folder>/<table>.parquet` (the warehouse). |

## The seams

Seven places where one module answers a question for everyone else. When behaviour looks
inconsistent between the UI, the SDK and a pipeline, the bug is almost always a caller that
bypassed a seam.

### 1. Storage (`core/storage.py`)

`STORAGE_URI` resolves to one `warehouse_storage()` object that lists, reads, writes and
deletes Parquet. Writes are atomic (one file, one replace), which is what makes "the file is
the table" safe to build on: a reader sees the old file or the new one.

Nothing else in the codebase touches the filesystem for warehouse data.

### 2. The catalog is a projection (`core/catalog.py`)

There is no `layers` table and no registration step. A table's address *is* its path, and
`scan()` walks the storage seam to build the catalog. A table can therefore never be listed
with no file behind it, and deleting a file deletes the table.

`catalog_objects` still exists as a projection of *external* sources (what `reconcile`
discovered on a connection), and is rebuilt rather than hand-maintained.

### 3. Query (`engines/duckdb_engine.py`)

One in-memory DuckDB per query. Before executing, it registers a view per referenced
Parquet file and rewrites the address the user typed into the form DuckDB parses:

| Typed | Resolves to |
| :--- | :--- |
| `lakehouse.raw.orders` | canonical three-slot address |
| `raw.orders` | shorthand, mapped onto the canonical name |
| `orders` | bare alias, registered only while that name is unique |

Rewriting is dot-aware, so a canonical name is never rewritten twice, and it only touches
code regions -- string literals, quoted identifiers and comments are left alone.

### 4. Execution (`engines/pipeline_engine.py`)

The orchestrator decides *what* runs and *how* it is observed; the engines do the work.

| Step type | Runs as |
| :--- | :--- |
| `script`, `notebook` | child process, output streamed into the step row every second |
| `dbt_run`, `dbt_test` | the dbt CLI in a thread, output streamed on the same cadence |
| `raw_sql` | in-process DuckDB; can materialize its result to a layer |
| `webhook` | an HTTP request |
| `pipeline` | another pipeline, with the child run recording its parent |

### 5. Auth (`core/security.py`)

One module answers "who is calling". `require_principal` is registered once on the app, so
every current and future `/api` route inherits it; `authorize_websocket` does the same for
sockets, which cannot send an `Authorization` header and therefore use the session cookie or
`?token=`. Enforcement is off until `OBELISK_AUTH_ENABLED=1`.

### 6. Vault (`core/vault.py`)

Credentials and variables are encrypted with Fernet under `OBELISK_MASTER_KEY` and decrypted
only in memory, at the moment they are needed. The master key is the one secret that must be
set before you store real credentials.

### 7. Source drivers (`core/sources.py`)

A connection is data, not behaviour. `resolve_source` answers how a source is reachable:

| Driver | Meaning |
| :--- | :--- |
| `attach` | DuckDB reads it live (Postgres): zero copy, always fresh, read-only. |
| `sdk` | Reachable only through a client library (MySQL, Oracle, SQL Server, MongoDB): rows must be pulled. |
| `http` | A REST/ERP endpoint: one request per pull. |

Adding a driver is an edit in one place, and every caller inherits it.
## How a run actually happens

The database is the scheduler; the process is only a consumer.

- **Claim.** A due run is taken with `UPDATE ... SET status=/running/ WHERE status=/pending/`
  and a rowcount check, so two engines sharing one file still execute a run exactly once.
- **Promote.** A step becomes runnable when its parents are terminal, and the edge condition
  decides whether it runs at all: `success`, `failure`, `completion` or `skipped`.
- **Retry.** `max_retries` and `retry_delay_seconds` are honoured per step, the attempt
  counter is visible in the run history, and the reason is written into the step log.
- **Publish.** A step declares its outputs (`table`, `rows`, `dbt_summary`, `run_id`,
  `status`); they are stored on the step run and in `pipeline_state`, so a downstream step can
  bind to them as `{{steps.build.table}}` even after the parent run is pruned.
- **Observe.** Logs are flushed to the step row every second and the logs endpoint is an
  incremental tail (`?after=N`), so a run is readable while it is still going.
- **Close.** `keep_runs` prunes old runs per pipeline; `pipeline_state` is never pruned. A run
  that was mid-flight at shutdown is reported as `interrupted` on the next boot.

A `pipeline` step runs another pipeline and waits. The child run records `parent_run_id` and
`parent_step_run_id`, which is the whole lineage story.

## Ingestion

| Mode | Behaviour |
| :--- | :--- |
| `snapshot` | Replace the table. |
| `incremental` | Pull only rows newer than the stored watermark, merged inside DuckDB. |
| `upsert` | Keep the newest row per primary key, ordered by the watermark column. |

Postgres merges run out-of-core inside DuckDB, so a long history never loads into memory, and
the finished table replaces the old file atomically.

## Federation

Attachable sources are read from the vault, attached into the same DuckDB session, and the SQL
text is scanned for aliases so only the sources the query mentions are attached. One resolver
serves three callers -- the editor, the SDK and pipeline `raw_sql` steps -- which is why a
federated join behaves identically in all three.

## Runtime topology

| Port | What |
| :--- | :--- |
| `8000` | FastAPI control plane; also serves the built SPA in production. |
| `5173` | Vite dev server (development only). |
| `2718` | Headless Marimo notebook server. |
| `8088` | Apache Superset container (its own origin; not proxied by the gateway). |
| `80` | Caddy gateway (Docker Compose: `/api`, `/notebooks`, `/gitea`, SPA). |
| `5434` / `27018` | Demo Postgres and MongoDB from `docker-compose.sources.yml`. |

There is no separate scheduler daemon: the orchestrator runs inside the API process, started
and stopped by the application lifespan.

## Security posture

- **No authentication is enforced yet.** The seam is wired (`require_principal`,
  `authorize_websocket`) but `OBELISK_AUTH_ENABLED` defaults to off, so treat the API as
  trusted-network only. Enabling it also refuses to start while the admin password is still
  the shipped default.
- **WebSocket origins are checked even while auth is off**, which is what stops a page on
  another site from driving this backend through the operator's browser.
- **The terminal is a real host shell.** It is origin-gated, capped at four sessions, idle-timed
  at 15 minutes and audited; `OBELISK_TERMINAL=0` removes it entirely.
- **CORS** is a wildcard while auth is off, and an explicit origin set once it is on -- a
  wildcard cannot carry a session cookie.

## Known limits

Stated plainly, because a document that only lists strengths is not useful.

- **SQLite is the brain.** One writer, WAL mode, fine for a single host; not a cluster.
- **Incremental upsert rewrites the whole table file.** Cheap at gigabytes, not at terabytes.
- **Auth is a seam, not a feature.** No users table, no login screen, no roles.
- **Scripts and notebooks run unsandboxed** as the backend's operating-system user.
- **CI is parked** while the platform is in development; the local suite is the gate.