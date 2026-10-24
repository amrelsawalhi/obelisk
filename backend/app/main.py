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
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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
from app.api.routes_layers import router as layers_router
from app.core.resource_advisor import advisor
from app.core.docker_manager import docker_mgr

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize SQLite database and enforce WAL mode
    await init_db()
    # Close out runs killed by the previous shutdown and reload recent history, so a
    # restart no longer erases every job and dbt run (audit M3/H10).
    from app.services.state import restore_task_history
    await asyncio.to_thread(restore_task_history)
    # A layer can be deleted while the catalog still points at it; restore the rows so
    # the tree always describes what exists.
    from app.services.layers import reconcile_layers_from_assets
    await asyncio.to_thread(reconcile_layers_from_assets)
    # The orchestrator heals interrupted rows itself on its first tick.
    from app.engines.pipeline_engine import pipeline_engine
    pipeline_engine.start()
    yield
    await pipeline_engine.stop()
    # Terminate the headless Marimo subprocess so no orphan holds port 2718.
    from app.engines.marimo_service import marimo_service
    marimo_service.stop()

app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    description="Obelisk: Pluggable On-Premise Data Platform & Control Plane",
    lifespan=lifespan,
)

# CORS middleware for LAN clients and Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
app.include_router(layers_router)


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

