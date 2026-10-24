#!/usr/bin/env bash
# =============================================================================
# Obelisk production install / service manager (Linux, systemd)
#
#   sudo scripts/install_service.sh install [--source DIR] [--port 8000]
#   sudo scripts/install_service.sh uninstall
#        scripts/install_service.sh status
#   sudo scripts/install_service.sh restart
#
# Layout (the standard split between code, config and state):
#   /opt/obelisk               application code + backend venv + built frontend
#   /etc/obelisk/obelisk.env   environment file (DATABASE_URL, STORAGE_URI, keys)
#   /var/lib/obelisk           state: obelisk.db, lakehouse/, logs/
#
# The backend serves the built SPA, so nothing else needs to run: one systemd
# unit, Restart=always, enabled at boot. Migrations run on startup.
# =============================================================================
set -euo pipefail

APP_NAME="obelisk"
APP_DIR="/opt/obelisk"
DATA_DIR="/var/lib/obelisk"
ETC_DIR="/etc/obelisk"
ENV_FILE="${ETC_DIR}/obelisk.env"
UNIT_FILE="/etc/systemd/system/${APP_NAME}.service"
SERVICE_USER="${APP_NAME}"
DEFAULT_PORT=8000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PORT="${DEFAULT_PORT}"

log()  { printf '\033[1;34m[obelisk]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

require_root() {
  [ "$(id -u)" -eq 0 ] || die "This command needs root: run it with sudo."
}

require_systemd() {
  command -v systemctl >/dev/null 2>&1 || die "systemd is not available on this host."
}

ensure_user() {
  if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    log "Creating system user ${SERVICE_USER}"
    useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
  fi
}

ensure_dirs() {
  install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}" \
    "${DATA_DIR}/lakehouse" "${DATA_DIR}/logs"
  install -d -m 0750 "${ETC_DIR}"
}

write_env_file() {
  if [ -f "${ENV_FILE}" ]; then
    log "Keeping existing ${ENV_FILE}"
    return
  fi
  log "Writing ${ENV_FILE}"
  local key
  key="$(python3 - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
)" || key="change-me-generate-a-fernet-key"
  cat > "${ENV_FILE}" <<EOF
# Obelisk runtime configuration. Read by systemd (EnvironmentFile).
DATABASE_URL=sqlite+aiosqlite:///${DATA_DIR}/obelisk.db
STORAGE_URI=file://${DATA_DIR}/lakehouse
ENVIRONMENT=production
OBELISK_MASTER_KEY=${key}
EOF
  chmod 0640 "${ENV_FILE}"
  chown root:"${SERVICE_USER}" "${ENV_FILE}"
}

copy_application() {
  local src="$1"
  log "Installing application from ${src} to ${APP_DIR}"
  install -d "${APP_DIR}"
  # Never copy the operator's state or a dev checkout's working files.
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete \
      --exclude '.git' --exclude '.venv' --exclude 'node_modules' \
      --exclude '__pycache__' --exclude '.pytest_cache' \
      --exclude 'backend/data' --exclude 'data' \
      "${src}/" "${APP_DIR}/"
  else
    tar -C "${src}" \
      --exclude='.git' --exclude='.venv' --exclude='node_modules' \
      --exclude='__pycache__' --exclude='.pytest_cache' \
      --exclude='backend/data' --exclude='data' \
      -cf - . | tar -C "${APP_DIR}" -xf -
  fi
  chown -R root:"${SERVICE_USER}" "${APP_DIR}"
  mkdir -p "${APP_DIR}/data"
  chown "${SERVICE_USER}:${SERVICE_USER}" "${APP_DIR}/data"
}

build_venv() {
  local py
  py="$(command -v python3 || true)"
  [ -n "${py}" ] || die "python3 is required but was not found."
  # Debian/Ubuntu ship python3 without ensurepip unless python3-venv is installed,
  # and the failure message appears halfway through the install otherwise.
  if ! "${py}" -c 'import ensurepip' >/dev/null 2>&1; then
    die "python3-venv is missing. Install it first: sudo apt install python3-venv"
  fi
  if [ ! -x "${APP_DIR}/backend/.venv/bin/python" ]; then
    log "Creating the backend virtualenv"
    "${py}" -m venv "${APP_DIR}/backend/.venv"
  fi
  log "Installing Python dependencies (this can take a few minutes)"
  "${APP_DIR}/backend/.venv/bin/python" -m pip install --upgrade pip >/dev/null
  "${APP_DIR}/backend/.venv/bin/python" -m pip install -r "${APP_DIR}/backend/requirements.txt"
}

check_frontend() {
  if [ ! -f "${APP_DIR}/frontend/dist/index.html" ]; then
    cat >&2 <<EOF
[error] frontend/dist is missing.

Obelisk serves the built UI from the backend, so the release must contain a built
frontend. Build it where Node is available and copy it in:

    cd frontend && npm ci && npm run build
    sudo rsync -a frontend/dist/ ${APP_DIR}/frontend/dist/

Linux itself needs no Node at runtime.
EOF
    exit 1
  fi
}

write_unit() {
  log "Writing ${UNIT_FILE}"
  cat > "${UNIT_FILE}" <<EOF
[Unit]
Description=Obelisk Control Plane (API + UI + pipeline orchestrator)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
# The venv interpreter owns the installed packages; calling it directly (instead
# of through a shell) means the service does not depend on PATH at boot.
ExecStart=${APP_DIR}/backend/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --app-dir ${APP_DIR}/backend
Restart=always
RestartSec=5s
TimeoutStopSec=20s
# SIGTERM lets uvicorn run the lifespan hook, which stops the pipeline engine and
# kills running step children instead of orphaning them.
KillSignal=SIGTERM
KillMode=mixed
NoNewPrivileges=yes
PrivateTmp=yes
ReadWritePaths=${DATA_DIR} ${APP_DIR}/data

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
}


install_unit() {
  require_root
  require_systemd
  ensure_user
  ensure_dirs
  write_env_file
  copy_application "${SOURCE_DIR}"
  build_venv
  check_frontend
  write_unit
  log "Enabling ${APP_NAME} at boot and starting it now"
  systemctl enable --now "${APP_NAME}.service"
  sleep 3
  systemctl --no-pager --lines=5 status "${APP_NAME}.service" || true
  cat <<EOF

Obelisk is installed.

  UI + API   http://<host>:${PORT}/          (served by the backend)
  Config     ${ENV_FILE}
  State      ${DATA_DIR}   (obelisk.db, lakehouse/, logs/)
  Logs       journalctl -u ${APP_NAME} -f
  Upgrade    replace ${APP_DIR} contents, then: sudo systemctl restart ${APP_NAME}

Port ${PORT} is the edge by default. If you later put Caddy or nginx in front for
HTTPS, bind uvicorn to 127.0.0.1 and add --proxy-headers in the unit.
EOF
}

uninstall_unit() {
  require_root
  require_systemd
  log "Stopping and removing the service"
  systemctl disable --now "${APP_NAME}.service" 2>/dev/null || true
  rm -f "${UNIT_FILE}"
  systemctl daemon-reload
  warn "Code and state were kept: ${APP_DIR}, ${DATA_DIR}, ${ENV_FILE}"
  warn "Remove them manually if you really want a clean machine."
}

case "${1:-}" in
  install)
    shift || true
    while [ $# -gt 0 ]; do
      case "$1" in
        --source) SOURCE_DIR="$(cd "$2" && pwd)"; shift 2 ;;
        --port)   PORT="$2"; shift 2 ;;
        *) die "Unknown option: $1" ;;
      esac
    done
    install_unit
    ;;
  uninstall) uninstall_unit ;;
  restart)
    require_root
    systemctl restart "${APP_NAME}.service"
    systemctl --no-pager --lines=5 status "${APP_NAME}.service" || true
    ;;
  render-unit)
    # Prints the unit that `install` would write. Useful for review, for CI, and
    # for `systemd-analyze verify` on a host where installing is not appropriate.
    PORT="${2:-${DEFAULT_PORT}}"
    UNIT_PORT="${PORT}" SERVICE_USER_ENV="${SERVICE_USER}" APP_DIR_ENV="${APP_DIR}" \
    DATA_DIR_ENV="${DATA_DIR}" ENV_FILE_ENV="${ENV_FILE}" \
    python3 - <<'PY'
import os
print(f"""[Unit]
Description=Obelisk Control Plane (API + UI + pipeline orchestrator)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={os.environ['SERVICE_USER_ENV']}
Group={os.environ['SERVICE_USER_ENV']}
WorkingDirectory={os.environ['APP_DIR_ENV']}
EnvironmentFile={os.environ['ENV_FILE_ENV']}
ExecStart={os.environ['APP_DIR_ENV']}/backend/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port {os.environ['UNIT_PORT']} --app-dir {os.environ['APP_DIR_ENV']}/backend
Restart=always
RestartSec=5s
TimeoutStopSec=20s
KillSignal=SIGTERM
KillMode=mixed
NoNewPrivileges=yes
PrivateTmp=yes
ReadWritePaths={os.environ['DATA_DIR_ENV']} {os.environ['APP_DIR_ENV']}/data

[Install]
WantedBy=multi-user.target""")
PY
    ;;
  status)
    systemctl --no-pager --lines=20 status "${APP_NAME}.service" || true
    ;;
  *)
    cat <<EOF
Usage: sudo $0 {install|uninstall|restart|status} [--source DIR] [--port PORT]

  install    copy the app to ${APP_DIR}, build the venv, write and enable the unit
  uninstall  stop and remove the unit (code and state are kept)
  restart    restart the running service
  status     show the unit's status and recent log lines
EOF
    exit 2
    ;;
esac

