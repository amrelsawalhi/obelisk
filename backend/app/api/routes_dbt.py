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

from app.core.catalog import InvalidTablePath
from app.core.warehouse_resolver import WarehouseNotSelected, WarehouseReadOnly
from app.engines.dbt_runner import dbt_runner

router = APIRouter(prefix="/api/dbt", tags=["dbt Studio"])


class FileSaveRequest(BaseModel):
    path: str = Field(..., description="Relative file path inside git_repo/dbt_project/")
    content: str = Field(..., description="File text content")


class FileDeleteRequest(BaseModel):
    path: str = Field(..., description="Relative file path inside git_repo/dbt_project/")


class WarehouseBinding(BaseModel):
    """Which connection a Postgres target publishes into.

    ``connection_id`` is the binding; the target name only picks the adapter. Without a
    binding a Postgres build is refused, because the alternative is guessing.
    """

    connection_id: Optional[str] = Field(
        default=None, description="Connection id to use as a warehouse; null clears it"
    )


class CompileModelRequest(BaseModel):
    model_name: str = Field(..., description="Name of the dbt model (e.g. stg_orders)")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )
    connection_id: Optional[str] = Field(
        default=None, description="Warehouse connection for a Postgres target"
    )


class PreviewModelRequest(BaseModel):
    model_name: str = Field(..., description="Name of the dbt model (e.g. stg_orders)")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )
    limit: Optional[int] = Field(default=50, description="Max rows to return for DataGrid preview")
    connection_id: Optional[str] = Field(
        default=None, description="Warehouse connection for a Postgres target"
    )


class RunDbtRequest(BaseModel):
    selector: str = Field(default="*", description="dbt selector pattern (e.g. 'stg_orders', 'models/staging', '*')")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )
    connection_id: Optional[str] = Field(
        default=None,
        description="Warehouse connection this run publishes into (Postgres targets only)",
    )


class TestDbtRequest(BaseModel):
    selector: str = Field(default="*", description="dbt selector pattern (e.g. 'stg_orders', 'models/staging', '*')")
    target: Optional[str] = Field(
        default=None,
        description="A target name from profiles.yml; defaults to the profile's own default",
    )
    connection_id: Optional[str] = Field(
        default=None, description="Warehouse connection for a Postgres target"
    )


class SampleDataRequest(BaseModel):
    folder: Optional[str] = Field(
        default=None,
        description="Warehouse folder to write the sample tables into; defaults to the project's staging schema",
    )


@router.post("/sample-data")
async def create_sample_data(req: Optional[SampleDataRequest] = None):
    """Writes the shipped sample tables into a folder, because someone asked.

    The explicit way to fill an empty warehouse. A build never calls it: a run with
    no data reports that plainly instead of inventing tables, so what the catalog
    shows is always what the operator put there.
    """
    try:
        return await asyncio.to_thread(
            dbt_runner.seed_sample_data, req.folder if req else None
        )
    except InvalidTablePath as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not write the sample tables: {exc}"
        ) from exc


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


@router.get("/warehouse")
async def get_warehouse():
    """The connection marts publish into, plus the candidates a picker can offer."""
    from app.core.warehouse_resolver import describe_warehouse_postgres, warehouse_candidates

    warehouse, candidates = await asyncio.gather(
        asyncio.to_thread(describe_warehouse_postgres),
        asyncio.to_thread(warehouse_candidates),
    )
    return {
        "warehouse": warehouse,
        "candidates": candidates,
    }


@router.put("/warehouse")
async def set_warehouse(req: WarehouseBinding):
    """Bind the Postgres target to one connection — or clear the choice."""
    from app.core.warehouse_resolver import (
        describe_warehouse_postgres,
        set_warehouse_connection_id,
        warehouse_candidate,
    )

    if req.connection_id:
        candidate = await asyncio.to_thread(warehouse_candidate, req.connection_id.strip())
        if not candidate:
            raise HTTPException(status_code=404, detail="No such Postgres connection.")
        if candidate["read_only"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Connection {candidate['name']!r} is read-only. Mark it writable "
                    "before making it the warehouse."
                ),
            )
    await asyncio.to_thread(set_warehouse_connection_id, req.connection_id)
    warehouse = await asyncio.to_thread(describe_warehouse_postgres)
    return {"warehouse": warehouse}


@router.post("/compile")
async def compile_dbt_model(req: CompileModelRequest):
    """Runs dbt compile for a specific model and returns the compiled SQL, duration, and logs."""
    try:
        res = await asyncio.to_thread(
            dbt_runner.compile_model,
            req.model_name,
            req.target or dbt_runner.default_target(),
            req.connection_id,
        )
        return res
    except WarehouseNotSelected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WarehouseReadOnly as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
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
            connection_id=req.connection_id,
        )
        return res
    except WarehouseNotSelected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WarehouseReadOnly as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
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
            connection_id=req.connection_id,
        )
        return res
    except WarehouseNotSelected as exc:
        # No warehouse chosen: a refusal with the fix, not a build failure.
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WarehouseReadOnly as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
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
            connection_id=req.connection_id,
        )
        return res
    except WarehouseNotSelected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WarehouseReadOnly as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
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
