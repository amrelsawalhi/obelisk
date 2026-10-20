#!/usr/bin/env bash
set -e

# ==============================================================================
# Obelisk 1-Click Startup Script (Linux / macOS / WSL)
# ==============================================================================

# Navigate to project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "=============================================================================="
echo "               Obelisk - Pluggable Data Platform Control Plane"
echo "=============================================================================="
echo ""

# 1. Ensure .env exists
if [ ! -f ".env" ]; then
    echo "[*] Creating .env from .env.example..."
    cp .env.example .env
fi

# 2. Detect Python executable
PYTHON_BIN=""
if [ -f "backend/.venv/bin/python" ]; then
    PYTHON_BIN="backend/.venv/bin/python"
elif [ -f ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON_BIN="python3"
elif command -v python &>/dev/null; then
    PYTHON_BIN="python"
else
    echo "[!] ERROR: Python 3 not found. Please install Python or set up backend/.venv"
    exit 1
fi

# 3. Check for Node & npm
if ! command -v npm &>/dev/null; then
    echo "[!] ERROR: npm is not installed or not in PATH."
    exit 1
fi

# 4. Graceful shutdown handler for Ctrl+C
BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
    echo ""
    echo "[*] Shutting down Obelisk services..."
    if [ -n "$BACKEND_PID" ]; then
        kill "$BACKEND_PID" 2>/dev/null || true
    fi
    if [ -n "$FRONTEND_PID" ]; then
        kill "$FRONTEND_PID" 2>/dev/null || true
    fi
    echo "[+] Done. Goodbye!"
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# 5. Start Backend
echo "[*] Starting FastAPI Backend on http://localhost:8000 ..."
(cd "$PROJECT_ROOT/backend" && "$PROJECT_ROOT/$PYTHON_BIN" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload) &
BACKEND_PID=$!

sleep 2

# 6. Start Frontend
echo "[*] Starting React 18 Frontend on http://localhost:5173 ..."
(cd "$PROJECT_ROOT/frontend" && npm run dev) &
FRONTEND_PID=$!

sleep 2

# 7. Open Browser
if command -v xdg-open &>/dev/null; then
    xdg-open "http://localhost:5173" &>/dev/null &
elif command -v open &>/dev/null; then
    open "http://localhost:5173" &>/dev/null &
fi

echo ""
echo "=============================================================================="
echo " [+] Obelisk is live!"
echo "     - Web UI:     http://localhost:5173"
echo "     - API Docs:   http://localhost:8000/docs"
echo "     - Notebooks:  Auto-starts on http://localhost:2718"
echo ""
echo " Press Ctrl+C at any time to gracefully stop all services."
echo "=============================================================================="
echo ""

# Keep running and wait for background processes
wait
