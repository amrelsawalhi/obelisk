# Configuration

Everything is configured through environment variables. [`.env.example`](../.env.example) is
the canonical list; this page explains what each one does and how the installs differ.

## Environment variables

| Variable | Default | What it controls |
| :--- | :--- | :--- |
| `STORAGE_URI` | `file:///data/lakehouse` | Where the Parquet lakehouse lives. `file://` today, object storage when the backend supports it. On a checkout without `/data/lakehouse`, the path is remapped into `./data/lakehouse`. |
| `DATABASE_URL` | `sqlite+aiosqlite:///<checkout>/data/obelisk.db` | The control-plane database: pipelines, runs, catalog, connections, variables. |
| `OBELISK_MASTER_KEY` | a published sample key | Fernet key for the credential vault. **Change it before storing real credentials**; changing it later makes existing secrets unreadable. |
| `OBELISK_ADMIN_USER` / `OBELISK_ADMIN_PASSWORD` | `admin` / `obelisk2026` | Credentials used once authentication is enforced. |
| `OBELISK_AUTH_ENABLED` | `0` (compose: `1`) | `1` gates every `/api` route and every WebSocket behind a session. Startup refuses to continue if the admin password is still the shipped default. With it off, a deployment whose `ENVIRONMENT=production` and `HOST` is not loopback refuses to start until that exposure is made explicit. |
| `OBELISK_ALLOW_UNAUTHENTICATED` | unset | The explicit opt-in for the case above: `1` lets a production, non-loopback instance serve an open API and logs a warning on every start. |
| `OBELISK_SESSION_SECURE_COOKIE` | `0` | `1` marks the session cookie `Secure`. Off for LAN HTTP installs; on for anything served over HTTPS. |
| `OBELISK_SESSION_TTL_SECONDS` | `604800` | Session lifetime. A stolen cookie is valid until it expires. |
| `OBELISK_AUTH_MAX_FAILED_ATTEMPTS` / `OBELISK_AUTH_LOCKOUT_SECONDS` | `10` / `300` | Login brute-force protection: after N failures from one address, that address is locked out for the given seconds. |
| `OBELISK_ALLOW_DEFAULT_CREDENTIALS` | `0` | Allows running with sample master key and default password in test/dev without startup abort. |
| `OBELISK_TRUSTED_PROXIES` | empty | Comma-separated IPs/CIDRs of reverse proxies whose `X-Forwarded-For` may be believed (used by the unauthenticated terminal gate and rate limiting). |
| `OBELISK_ALLOWED_ORIGINS` | empty | Extra browser origins allowed to open sockets and send the session cookie, comma separated. Same-host, loopback and Vite's dev server are always allowed. Set this when a reverse proxy rewrites `Host`. |
| `OBELISK_TERMINAL` | `1` (compose: `0`) | `0` disables the in-app terminal (a real shell on the host) without touching code. While auth is off, the socket only accepts a loopback client. |
| `MARIMO_HOST` | `127.0.0.1` (compose: `0.0.0.0`) | Where the headless notebook server binds. In the container it binds all interfaces so the Caddy gateway can reach it; the gateway authenticates `/notebooks/*` via `forward_auth` first, and the port is never published on the host. Outside the container keep it loopback unless something in front authenticates first. |
| `MARIMO_SESSION_TTL_SECONDS` | `600` | How long a notebook kernel stays alive after its browser tab disconnects (`0` = never expire). |
| `OBELISK_SERVICE_BIND_HOST` | `127.0.0.1` | Network interface to bind managed Docker database and service containers. |
| `OBELISK_NET` | `obelisk_net` | Docker network used for companion services. |
| `DUCKDB_THREADS` | auto | DuckDB worker threads limit. Auto divides logical CPUs by the heavy-slot count. |
| `DUCKDB_TEMP_SIZE_LIMIT` | `50GB` | Maximum disk spill headroom for out-of-core DuckDB operations, divided across heavy slots and bounded by free disk. Prefer binary units in overrides (`50GiB`). |
| `DUCKDB_MAX_QUERY_ROWS` | `10000` | Display limit for interactive JSON query studio responses. Full results stream to disk for exports. |
| `DUCKDB_MAX_CONCURRENT_HEAVY` | `4` | Heavy-work permits per process. The topbar aggregates live cross-process leases, so a notebook or pipeline subprocess adds its reservations to the displayed total; the semaphore itself is not a cross-process lock. |
| `OBELISK_INGEST_MAX_CONCURRENT` | `2` | Maximum concurrent table ingests across every entry point: sync API, batch, pipelines, job manager and SDK. |
| `OBELISK_LAKEHOUSE_COMPACT_MAX_FILES` | `50` | Upper bound on files rewritten by one automatic or API-triggered compaction pass. |
| `OBELISK_SQLITE_JOURNAL_MODE` | `WAL` | Control-plane SQLite journal mode. Keep `WAL` on local ext4/XFS. Use `DELETE` only on a filesystem that rejects WAL's shared-memory mapping. |
| `OBELISK_SQLITE_SYNCHRONOUS` | `NORMAL` | SQLite synchronous mode. `OFF` is an explicit durability tradeoff for virtual filesystems that reject `fsync`. |
| `OBELISK_ALLOW_COMMUNITY_EXTENSIONS` | `true` | Allows unsigned community extensions (such as the Mongo scanner) in DuckDB. |
| `OBELISK_SDK_EAGER_WARN_BYTES` | `2147483648` (2 GiB) | SDK eager reader (`to_polars`, `to_arrow`) memory warning threshold. |
| `OBELISK_SDK_EAGER_REFUSE_BYTES` | `0` (dynamic) | SDK eager reader memory refusal ceiling (`0` derives safe ceiling from host RAM). |
| `OBELISK_SDK_QUERY_ROW_LIMIT` | `1000000` | Maximum rows `obelisk.query()` materializes before raising and pointing to `iter_batches()`. |
| `DOCKER_HOST` | unset | Custom Docker daemon socket path. |
| `SPARK_REMOTE_URL` | unset | Remote Spark Connect cluster URL (e.g., `sc://...`). |

### Engine telemetry and spill

The topbar's engine panel reads live reservations, not just counters in the API process.
Every heavy job publishes a small lease under
`<lakehouse>/.obelisk/resource_leases/`, so pipeline scripts, notebooks, SDK calls and
extra API workers are included. Lease files from dead processes are swept on the next
telemetry poll.

Each dbt run gets a private spill directory beside its scratch database
(`data/duckdb_tmp/dbt_scratch_*_spill/`). dbt sets DuckDB's `temp_directory` once in an
`on-run-start` hook rather than through profile `settings`, because dbt-duckdb reapplies
profile settings on every cursor and DuckDB refuses a second `temp_directory` change.
The panel shows the *active* combined temp budget (for a lakehouse dbt run, the whole
reserved batch pool) and separately shows the per-slot interactive cap.

## Ports

| Port | What |
| :--- | :--- |
| `8000` | FastAPI control plane. In a production install this also serves the built SPA. |
| `5173` | Vite dev server (development only). |
| `2718` | Headless Marimo notebook server. Runs with `--no-token`, so it is never published; Caddy reaches it inside the network and gates `/notebooks/*` behind `forward_auth`. |
| `80` | Caddy gateway, when you deploy with Docker Compose. |
| `5434` / `27018` | Demo sources: Postgres and MongoDB from `docker-compose.sources.yml`. |

## Where state lives

| Path | Contents |
| :--- | :--- |
| `data/obelisk.db` (+ `-wal`, `-shm`) | The control-plane database. Back this up; everything else is rebuildable. |
| `data/lakehouse/` | Parquet warehouse tables and DuckLake SQLite catalog at `.obelisk/catalog.sqlite`. |
| `git_repo/` | Code the platform runs: `scripts/`, `notebooks/`, `dbt_project/`. Version-control it. |

## Installs

### 1-click, from a checkout

```bash
run_obelisk.bat          # Windows
bash run_obelisk.sh      # Linux / macOS / WSL
```

Guards port 8000, starts the API on `:8000` and Vite on `:5173`, then opens the browser.
`run_obelisk.bat --restart` replaces a listener that is still holding port 8000.

### Docker Compose

```bash
docker compose up --build -d
```

Builds `obelisk-core` from `backend/Dockerfile` and a Caddy gateway from
`Dockerfile.gateway`; the gateway fronts `/api`, `/notebooks` and `/gitea` and
serves the SPA on `:80`. Apache Superset is not proxied: it is linked at its own
address (`:8088`), which is the only origin it serves.

### systemd (Linux servers)

The backend serves the built UI, so a Linux install is one process and needs no Node at
runtime:

```bash
cd frontend && npm ci && npm run build && cd ..   # build the SPA once
sudo scripts/install_service.sh install           # code -> /opt/obelisk, state -> /var/lib/obelisk
systemctl status obelisk
journalctl -u obelisk -f
sudo scripts/install_service.sh restart
```

`install_service.sh` takes `{install|uninstall|restart|status}` plus optional
`--source DIR` and `--port PORT`. It writes `/etc/obelisk/obelisk.env` for the settings
above.

On Windows the equivalent is a scheduled task:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install_service.ps1 install
```

### Manual development

```bash
cd backend && python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000

cd ../frontend && npm ci && npm run dev
```

## Demo sources

```bash
docker compose -f docker-compose.sources.yml up -d
```

Starts Postgres on `:5434` (`testuser` / `testpass` / `sourcedb`) and MongoDB on `:27018`,
which is what the end-to-end harness in [testing.md](testing.md) expects.

## Upgrades

Replace the code and restart: the database migrations run on startup. The lakehouse and the
control-plane database are plain files, so backing them up is a copy.