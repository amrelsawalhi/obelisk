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
| `OBELISK_TRUSTED_PROXIES` | empty | Comma-separated IPs/CIDRs of reverse proxies whose `X-Forwarded-For` may be believed (used by the unauthenticated terminal gate). |
| `OBELISK_ALLOWED_ORIGINS` | empty | Extra browser origins allowed to open sockets and send the session cookie, comma separated. Same-host, loopback and Vite's dev server are always allowed. Set this when a reverse proxy rewrites `Host`. |
| `OBELISK_TERMINAL` | `1` (compose: `0`) | `0` disables the in-app terminal (a real shell on the host) without touching code. While auth is off, the socket only accepts a loopback client. |
| `MARIMO_HOST` | `127.0.0.1` (compose: `0.0.0.0`) | Where the headless notebook server binds. In the container it binds all interfaces so the Caddy gateway can reach it; the gateway authenticates `/notebooks/*` via `forward_auth` first, and the port is never published on the host. Outside the container keep it loopback unless something in front authenticates first. |
| `MARIMO_SESSION_TTL_SECONDS` | `600` | How long a notebook kernel stays alive after its browser tab disconnects (`0` = never expire). |
| `OBELISK_NET` | `obelisk_net` | Docker network used for companion services. |

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
| `data/lakehouse/<folder>/<table>.parquet` | The warehouse itself. One file per table, one directory per folder. |
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