import sys
from pathlib import Path

# Ensure backend/, project root and git_repo/ are on sys.path for uvicorn reloader
# child processes. git_repo/ must be importable so notebooks can `import obelisk`
# (the SDK adapter at git_repo/notebooks/obelisk.py) exactly as they do when run
# from the Pipelines tab.
_current_file = Path(__file__).resolve()
_backend_dir = _current_file.parents[1]
_project_root = _current_file.parents[2]

# git_repo/ lives next to backend/ in a source checkout, but inside the backend
# working directory in the Docker image (./git_repo -> /app/git_repo).
for _p in (
    _project_root,
    _backend_dir,
    _project_root / "git_repo",
    _backend_dir / "git_repo",
):
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from contextlib import asynccontextmanager
import asyncio
import logging
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.config import settings
from app.database import init_db
from app.api.routes_services import router as services_router
from app.api.routes_catalog import router as catalog_router
from app.api.routes_ingest import router as ingest_router
from app.api.routes_query import router as query_router
from app.api.routes_notebook import router as notebook_router
# dbt-facing pipeline endpoints first: their static paths (/api/pipelines/assets,
# /api/pipelines/model/...) must win over the orchestrator's /{pipeline_id}.
from app.api.routes_pipeline import router as dbt_pipeline_router
from app.api.routes_pipelines import router as pipelines_router
from app.api.routes_auth import router as auth_router
from app.api.routes_variables import router as variables_router
from app.api.routes_files import router as files_router
from app.api.routes_terminal import router as terminal_router
from app.api.routes_dbt import router as dbt_router
from app.core.resource_advisor import advisor
from app.core.docker_manager import docker_mgr
from app.core.security import (
    assert_credentials_are_not_shipped_defaults,
    authorize_websocket,
    require_principal,
)

# uvicorn configures its own loggers and leaves this application's without a
# handler, so a refused socket or an opened shell was being written to a logger that
# went nowhere. Attach one to the 'app' namespace only: uvicorn's access and error
# logs keep their own formatting, and records are still free to propagate.
_app_logger = logging.getLogger("app")
if not _app_logger.handlers:
    _app_handler = logging.StreamHandler()
    _app_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    _app_logger.addHandler(_app_handler)
    _app_logger.setLevel(logging.INFO)
    # Alembic's fileConfig also configures the root logger (from alembic.ini). Keeping
    # app records out of that path means one line per event and no dependence on the
    # migration config's level, which is WARN.
    _app_logger.propagate = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Refuse to start in the one configuration that only looks secure.
    assert_credentials_are_not_shipped_defaults()
    # One process owns this data directory. Nothing below enforces it: SQLite tolerates
    # another writer, DuckDB opens the same Parquet files, and DuckLake accepts a second
    # connection against the same catalog (proven in the format spike). Two backends on
    # one lakehouse corrupt it quietly, so the claim is taken before anything is read or
    # migrated, and released only when this process is done.
    from app.core.instance_lock import acquire_instance_lock

    instance_lock = acquire_instance_lock(
        Path(settings.sqlite_db_path).parent,
        database=settings.sqlite_db_path,
        lakehouse=settings.duckdb_storage_path,
    )
    try:
        # Attach the lakehouse catalog before anything can ask for a table. On a fresh
        # install this creates it; on a broken one it fails here, naming the missing
        # ducklake extension, rather than at somebody's first query.
        from app.engines.duckdb_engine import duckdb_engine
        await asyncio.to_thread(lambda: duckdb_engine.get_connection().close())
        # Initialize SQLite database and enforce WAL mode
        await init_db()
        # Close out runs killed by the previous shutdown and reload recent history, so a
        # restart no longer erases every job and dbt run (audit M3/H10).
        from app.services.state import restore_task_history
        await asyncio.to_thread(restore_task_history)
        # The catalog needs no repair on boot: it is a projection, rebuilt from the files
        # the first time anyone reads it.
        # The orchestrator heals interrupted rows itself on its first tick.
        from app.engines.pipeline_engine import pipeline_engine
        pipeline_engine.start()
        yield
        await pipeline_engine.stop()
        # Terminate the headless Marimo subprocess so no orphan holds port 2718.
        from app.engines.marimo_service import marimo_service
        marimo_service.stop()
    finally:
        instance_lock.release()

def _cors_origins() -> list[str]:
    """Origins allowed to call the API, with or without auth.

    A wildcard used to be served while auth was off, which meant any page the operator
    happened to visit could POST /api/pipelines/{id}/run against localhost. Credentialed
    requests cannot ride a wildcard either, so the set is explicit in both modes: the dev
    server and localhost by default, plus whatever ``OBELISK_ALLOWED_ORIGINS`` lists.
    """
    configured = [
        item.strip().rstrip("/") for item in settings.ALLOWED_ORIGINS.split(",") if item.strip()
    ]
    return sorted(
        {
            *configured,
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        }
    )


app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    description="Obelisk: Pluggable On-Premise Data Platform & Control Plane",
    lifespan=lifespan,
    # One declaration instead of ten. `require_principal` is inert while auth is off
    # (see app/core/security.py) and answers 401 for /api/* the moment it is on, so no
    # route has to be revisited when the auth layer lands.
    dependencies=[Depends(require_principal)],
)

# CORS. Explicit origins only, in both modes: a wildcard let any page the operator visited
# drive the API, and it cannot carry credentials anyway.
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(auth_router)
app.include_router(services_router)
app.include_router(catalog_router)
app.include_router(ingest_router)
app.include_router(query_router)
app.include_router(notebook_router)
app.include_router(dbt_pipeline_router)
app.include_router(pipelines_router)
app.include_router(variables_router)
app.include_router(files_router)
app.include_router(terminal_router)
app.include_router(dbt_router)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "app": settings.APP_NAME,
        "environment": settings.ENVIRONMENT,
        "storage_uri": settings.STORAGE_URI,
    }

@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    if await authorize_websocket(websocket, "telemetry") is None:
        return
    """Streams live ResourceAdvisor RAM, CPU, and active container statuses every 2 seconds automatically."""
    await websocket.accept()
    try:
        while True:
            specs = await asyncio.to_thread(advisor.get_host_specs)
            allocations = await asyncio.to_thread(advisor.get_active_container_allocations)
            containers = await asyncio.to_thread(docker_mgr.list_managed_containers)

            data = {
                "cpu_percent": specs.get("cpu_percent", 0.0),
                "cpu_count": specs.get("cpu_count", 1),
                "ram_percent_used": specs.get("ram_percent_used", 0.0),
                "total_ram_gb": specs.get("total_ram_gb", 0.0),
                "available_ram_gb": specs.get("available_ram_gb", 0.0),
                "ram_used_gb": round(specs.get("total_ram_gb", 0.0) - specs.get("available_ram_gb", 0.0), 2),
                "max_safe_allocation_gb": specs.get("max_safe_allocation_gb", 0.0),
                "active_containers_count": len(containers),
                "allocated_ram_gb": allocations.get("total_allocated_gb", 0.0),
                "timestamp": asyncio.get_event_loop().time(),
            }
            await websocket.send_json(data)
            await asyncio.sleep(2.0)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

@app.get("/api/services/telemetry")
@app.get("/api/system/telemetry")
async def get_telemetry():
    """HTTP polling fallback for live telemetry (matches /ws/telemetry schema)."""
    specs = await asyncio.to_thread(advisor.get_host_specs)
    allocations = await asyncio.to_thread(advisor.get_active_container_allocations)
    containers = await asyncio.to_thread(docker_mgr.list_managed_containers)

    return {
        "cpu_percent": specs.get("cpu_percent", 0.0),
        "cpu_count": specs.get("cpu_count", 1),
        "ram_percent_used": specs.get("ram_percent_used", 0.0),
        "total_ram_gb": specs.get("total_ram_gb", 0.0),
        "available_ram_gb": specs.get("available_ram_gb", 0.0),
        "ram_used_gb": round(specs.get("total_ram_gb", 0.0) - specs.get("available_ram_gb", 0.0), 2),
        "max_safe_allocation_gb": specs.get("max_safe_allocation_gb", 0.0),
        "active_containers_count": len(containers),
        "allocated_ram_gb": allocations.get("total_allocated_gb", 0.0),
    }

@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    """Compatibility websocket endpoint for event streaming."""
    if await authorize_websocket(websocket, "events") is None:
        return
    await websocket.accept()
    try:
        while True:
            await asyncio.sleep(5.0)
    except (WebSocketDisconnect, Exception):
        pass



# ---------------------------------------------------------------------------
# Single-process deployment: the backend serves the built SPA.
#
# Vite stays the development server, but a Linux install has no Node at all — the
# systemd unit starts uvicorn and nothing else, and this mount is what makes that
# possible. Registered last so every API route and websocket above wins the match,
# and a deep client route (/pipelines, /dbt) still returns index.html.
# ---------------------------------------------------------------------------
_frontend_dist = (_project_root / "frontend" / "dist").resolve()
if (_frontend_dist / "index.html").is_file():
    if (_frontend_dist / "assets").is_dir():
        app.mount(
            "/assets",
            StaticFiles(directory=str(_frontend_dist / "assets")),
            name="spa-assets",
        )

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        if full_path.startswith(("api/", "ws/")):
            raise HTTPException(status_code=404, detail="Not found")
        candidate = (_frontend_dist / full_path).resolve()
        # Path traversal guard: FileResponse must only ever read inside dist/.
        if candidate.is_file() and str(candidate).startswith(str(_frontend_dist)):
            return FileResponse(candidate)
        return FileResponse(_frontend_dist / "index.html")

