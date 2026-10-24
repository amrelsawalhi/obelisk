"""
dbt Studio REST API Endpoints:
Provides full project explorer, Monaco Jinja authoring, real-time SQL compilation,
ad-hoc DataGrid previews, scoped test/run streaming CLI execution, and interactive model lineage.
Strictly Zero Mocks.
"""

import asyncio
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.engines.dbt_runner import dbt_runner

router = APIRouter(prefix="/api/dbt", tags=["dbt Studio"])


class FileSaveRequest(BaseModel):
    path: str = Field(..., description="Relative file path inside git_repo/dbt_project/")
    content: str = Field(..., description="File text content")


class FileDeleteRequest(BaseModel):
    path: str = Field(..., description="Relative file path inside git_repo/dbt_project/")


class CompileModelRequest(BaseModel):
    model_name: str = Field(..., description="Name of the dbt model (e.g. stg_orders)")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )


class PreviewModelRequest(BaseModel):
    model_name: str = Field(..., description="Name of the dbt model (e.g. stg_orders)")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )
    limit: Optional[int] = Field(default=50, description="Max rows to return for DataGrid preview")


class RunDbtRequest(BaseModel):
    selector: str = Field(default="*", description="dbt selector pattern (e.g. 'stg_orders', 'models/staging', '*')")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )


class TestDbtRequest(BaseModel):
    selector: str = Field(default="*", description="dbt selector pattern (e.g. 'stg_orders', 'models/staging', '*')")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )


@router.get("/targets")
async def list_dbt_targets():
    """The targets this project defines in ``profiles.yml``.

    dbt is multi-target by design; the Studio must not carry its own list of adapters.
    """
    try:
        return await asyncio.to_thread(dbt_runner.list_targets)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read dbt targets: {exc}") from exc


@router.get("/tree")
async def get_dbt_project_tree():
    """Returns the hierarchical file tree of git_repo/dbt_project/."""
    try:
        tree = await asyncio.to_thread(dbt_runner.get_dbt_project_tree)
        return tree
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect dbt project tree: {str(e)}")


@router.get("/file")
async def get_dbt_file(path: str = Query(..., description="Relative file path inside git_repo/dbt_project/")):
    """Reads file content from git_repo/dbt_project/."""
    try:
        return await asyncio.to_thread(dbt_runner.read_project_file, path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")


@router.post("/file")
async def save_dbt_file(req: FileSaveRequest):
    """Saves or creates a file in git_repo/dbt_project/."""
    try:
        return await asyncio.to_thread(dbt_runner.write_project_file, req.path, req.content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")


@router.delete("/file")
async def delete_dbt_file(
    path: Optional[str] = Query(default=None),
    body: Optional[FileDeleteRequest] = None,
):
    """Deletes a file safely from git_repo/dbt_project/."""
    target_path = path or (body.path if body else None)
    if not target_path:
        raise HTTPException(status_code=400, detail="Missing required 'path' parameter")

    try:
        return await asyncio.to_thread(dbt_runner.delete_project_file, target_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {target_path}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")


@router.post("/compile")
async def compile_dbt_model(req: CompileModelRequest):
    """Runs dbt compile for a specific model and returns the compiled SQL, duration, and logs."""
    try:
        res = await asyncio.to_thread(dbt_runner.compile_model, req.model_name, req.target or dbt_runner.default_target())
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt compilation failed: {str(e)}")


@router.post("/preview")
async def preview_dbt_model_data(req: PreviewModelRequest):
    """Compiles the model, wraps it with a limit query, and executes against the target database."""
    try:
        res = await asyncio.to_thread(
            dbt_runner.preview_model_data,
            model_name=req.model_name,
            target=req.target or dbt_runner.default_target(),
            limit=req.limit or 50,
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview model data: {str(e)}")


@router.post("/run")
async def run_dbt_models(req: RunDbtRequest):
    """Executes dbt run --select <selector> --target <target>."""
    try:
        res = await asyncio.to_thread(
            dbt_runner.run_scoped_dbt,
            command="run",
            selector=req.selector or "*",
            target=req.target or dbt_runner.default_target(),
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt run failed: {str(e)}")


@router.post("/test")
async def test_dbt_models(req: TestDbtRequest):
    """Executes dbt test --select <selector> --target <target>."""
    try:
        res = await asyncio.to_thread(
            dbt_runner.run_scoped_dbt,
            command="test",
            selector=req.selector or "*",
            target=req.target or dbt_runner.default_target(),
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt test failed: {str(e)}")


@router.get("/lineage")
async def get_dbt_model_lineage(model: str = Query(..., description="dbt model name")):
    """Extracts and returns the dependency sub-graph for the selected model from target/manifest.json."""
    try:
        res = await asyncio.to_thread(dbt_runner.get_model_lineage, model)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract model lineage: {str(e)}")
