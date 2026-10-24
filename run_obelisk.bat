@echo off
setlocal enabledelayedexpansion

title Obelisk Launcher
echo ==============================================================================
echo                Obelisk - Pluggable Data Platform Control Plane
echo ==============================================================================
echo.

:: 1. Navigate to project root directory
cd /d "%~dp0"

:: 2. Ensure .env exists
if not exist ".env" (
    echo [*] Creating .env from template .env.example...
    copy ".env.example" ".env" >nul
)

:: 3. Detect Python virtual environment or system Python
set PYTHON_EXE=
if exist "backend\.venv\Scripts\python.exe" (
    set "PYTHON_EXE=backend\.venv\Scripts\python.exe"
) else if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=.venv\Scripts\python.exe"
) else (
    where python >nul 2>nul
    if !errorlevel! equ 0 (
        set "PYTHON_EXE=python"
    ) else (
        echo [!] ERROR: Python not found. Please create a virtual environment in backend\.venv
        pause
        exit /b 1
    )
)

:: 4. Guard the API port. A stale `uvicorn --reload` parent can keep the port
::    listening while its worker is dead, which makes the UI load with no data.
::    `--restart` replaces a backend that is still running (the API can be up
::    while the Vite window that serves the page is gone).
echo [*] Checking port 8000 ...
if /I "%~1"=="--restart" (
    "%~dp0!PYTHON_EXE!" "%~dp0backend\scripts\preflight_port.py" --port 8000 --restart
) else (
    "%~dp0!PYTHON_EXE!" "%~dp0backend\scripts\preflight_port.py" --port 8000
)
if errorlevel 1 (
    echo [!] Aborting: port 8000 is not available. See the message above.
    echo     To replace the running instance: run_obelisk.bat --restart
    pause
    exit /b 1
)

:: 5. Start Backend Server in a new window.
::    --reload is opt-in (set OBELISK_DEV_RELOAD=1); the reloader is what leaves
::    orphaned socket holders behind on Windows.
set "RELOAD_FLAG="
if /I "%OBELISK_DEV_RELOAD%"=="1" set "RELOAD_FLAG=--reload"
echo [*] Starting FastAPI Backend on http://localhost:8000 ...
start "Obelisk Backend (FastAPI)" cmd /k "cd /d "%~dp0backend" && "%~dp0!PYTHON_EXE!" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 !RELOAD_FLAG!"

:: 5. Brief pause to allow backend initialization
timeout /t 2 /nobreak >nul

:: 6. Start Frontend Server in a new window
echo [*] Starting React 18 Frontend on http://localhost:5173 ...
start "Obelisk Frontend (Vite)" cmd /k "cd /d "%~dp0frontend" && npm run dev"

:: 7. Wait 2 seconds and open browser
timeout /t 2 /nobreak >nul
echo [*] Opening Obelisk in your browser...
start http://localhost:5173

echo.
echo ==============================================================================
echo  [+] Obelisk is running!
echo      - Web UI:     http://localhost:5173
echo      - API Docs:   http://localhost:8000/docs
echo      - Notebooks:  Auto-starts on http://localhost:2718
echo.
echo  Keep the opened terminal windows running while testing.
echo  Close those windows when you are done.
echo  If port 8000 is still held next time: run_obelisk.bat --restart
echo ==============================================================================
echo.
