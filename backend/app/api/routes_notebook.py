"""
Notebook API Routes: Exposes Marimo server status, lifecycle, and notebook workspace management.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from app.engines.marimo_service import marimo_service

router = APIRouter(prefix="/api/notebooks", tags=["Notebooks"])


class CreateNotebookRequest(BaseModel):
    name: str = Field(..., description="Name of the notebook file to create (without extension)")
    content: Optional[str] = Field(default=None, description="Optional custom python code template for the notebook")
    overwrite: Optional[bool] = Field(default=False, description="Whether to overwrite an existing notebook file")


class StartServerRequest(BaseModel):
    port: Optional[int] = Field(default=None, description="Optional custom port (defaults to 2718)")


@router.get("")
async def list_notebooks():
    """Returns list of all reactive Marimo notebook files in git_repo/notebooks."""
    notebooks = marimo_service.list_notebooks()
    return {"notebooks": notebooks, "total": len(notebooks)}


@router.post("")
async def create_notebook(req: CreateNotebookRequest):
    """Creates a new starter Marimo notebook."""
    if not req.name.strip():
        raise HTTPException(status_code=400, detail="Notebook name cannot be empty.")
    res = marimo_service.create_notebook(
        req.name,
        custom_content=req.content,
        overwrite=bool(req.overwrite),
    )
    return res


@router.delete("/{name}")
async def delete_notebook(name: str):
    """Deletes a reactive Marimo notebook file from git_repo/notebooks."""
    try:
        return marimo_service.delete_notebook(name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete notebook: {str(e)}")


@router.get("/server/status")
async def server_status():
    """Returns current status and health of the headless Marimo server."""
    return marimo_service.get_status()


@router.post("/server/start")
async def start_server(req: Optional[StartServerRequest] = None):
    """Starts the headless Marimo directory server."""
    port = req.port if req else None
    try:
        return marimo_service.start(port=port)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start Marimo server: {str(e)}")


@router.post("/server/stop")
async def stop_server():
    """Stops the headless Marimo server and releases the port."""
    return marimo_service.stop()


@router.post("/server/restart")
async def restart_server(req: Optional[StartServerRequest] = None):
    """Restarts the headless Marimo directory server."""
    port = req.port if req else None
    try:
        marimo_service.stop()
        import asyncio
        await asyncio.sleep(0.5)
        return marimo_service.start(port=port)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to restart Marimo server: {str(e)}")
