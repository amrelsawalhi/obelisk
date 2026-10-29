"""
Scripts Studio API:
Enables listing, editing, executing, streaming logs, and terminating (kill switch)
standalone Python scripts inside git_repo/scripts/.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import settings
from app.engines.pipeline_engine import _interpreter_path, _kill_process_tree

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scripts", tags=["Scripts Studio"])

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = (WORKSPACE_ROOT / "git_repo" / "scripts").resolve()
SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)


class ScriptSaveRequest(BaseModel):
    name: str = Field(..., description="File name inside git_repo/scripts/ (e.g. daily_brief.py)")
    content: str = Field(..., description="Python script text content")


class ScriptCreateRequest(BaseModel):
    name: str = Field(..., description="File name to create (without or with .py)")
    content: Optional[str] = Field(default=None, description="Initial Python code template")


class ScriptRunRequest(BaseModel):
    name: str = Field(..., description="File name inside git_repo/scripts/")
    params: Optional[Dict[str, Any]] = Field(default=None, description="Key-value parameters injected as OBELISK_PARAM_*")
    args: Optional[List[str]] = Field(default=None, description="CLI command line arguments")


class ScriptRunState:
    def __init__(self, run_id: str, script_name: str, proc: subprocess.Popen, output_file: Path):
        self.run_id = run_id
        self.script_name = script_name
        self.proc = proc
        self.output_file = output_file
        self.started_at = time.time()
        self.ended_at: Optional[float] = None
        self.status = "running"
        self.exit_code: Optional[int] = None
        self.logs: List[str] = []
        self.outputs: Dict[str, Any] = {}
        self.killed = False

    def to_dict(self) -> Dict[str, Any]:
        dur = (self.ended_at or time.time()) - self.started_at
        return {
            "run_id": self.run_id,
            "name": self.script_name,
            "status": self.status,
            "exit_code": self.exit_code,
            "duration_seconds": round(dur, 2),
            "logs": self.logs,
            "outputs": self.outputs,
        }


_RUNS: Dict[str, ScriptRunState] = {}
_RUNS_LOCK = threading.Lock()


def _safe_script_path(name: str) -> Path:
    clean = name.strip().replace("\\", "/").lstrip("/")
    if ".." in clean or clean.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid script path.")
    if not clean.endswith(".py"):
        clean += ".py"
    target = (SCRIPTS_DIR / clean).resolve()
    if not str(target).startswith(str(SCRIPTS_DIR)):
        raise HTTPException(status_code=400, detail="Access outside scripts directory forbidden.")
    return target


@router.get("")
def list_scripts() -> Dict[str, Any]:
    """Lists all Python scripts in git_repo/scripts/."""
    files = []
    for p in sorted(SCRIPTS_DIR.glob("*.py")):
        if p.name.startswith((".", "_")) and p.name != "__init__.py":
            continue
        try:
            st = p.stat()
            updated = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
            files.append({
                "name": p.name,
                "size_bytes": st.st_size,
                "updated_at": updated,
            })
        except OSError:
            pass
    return {"scripts": files, "total": len(files)}


@router.get("/content")
def get_script_content(name: str = Query(..., description="Script name (e.g. daily_brief.py)")) -> Dict[str, Any]:
    """Reads script content from git_repo/scripts/."""
    path = _safe_script_path(name)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Script '{name}' not found.")
    try:
        content = path.read_text(encoding="utf-8")
        return {"name": path.name, "content": content}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read script: {exc}")


@router.post("/save")
def save_script(req: ScriptSaveRequest) -> Dict[str, Any]:
    """Saves script content to git_repo/scripts/."""
    path = _safe_script_path(req.name)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(req.content, encoding="utf-8")
        st = path.stat()
        return {
            "status": "saved",
            "name": path.name,
            "size_bytes": st.st_size,
            "updated_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not save script: {exc}")


@router.post("/create")
def create_script(req: ScriptCreateRequest) -> Dict[str, Any]:
    """Creates a new Python script in git_repo/scripts/."""
    path = _safe_script_path(req.name)
    if path.exists():
        raise HTTPException(status_code=409, detail=f"Script '{path.name}' already exists.")
    template = req.content
    if template is None:
        clean_stem = path.stem.replace("_", " ").title()
        template = f'''"""
{clean_stem}
Automated script for Obelisk pipelines.
"""
from __future__ import annotations

import os
import sys
import obelisk

def main() -> int:
    print("Running {clean_stem}...")
    # Access parameters via os.environ.get("OBELISK_PARAM_<NAME>")
    # Query lakehouse:
    # df = obelisk.query("SELECT 1 AS ready")
    # print(df)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
'''
    try:
        path.write_text(template, encoding="utf-8")
        return {"status": "created", "name": path.name}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not create script: {exc}")


@router.delete("/{name}")
def delete_script(name: str) -> Dict[str, Any]:
    """Deletes a Python script from git_repo/scripts/."""
    path = _safe_script_path(name)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Script '{name}' not found.")
    try:
        path.unlink()
        return {"status": "deleted", "name": path.name}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not delete script: {exc}")


@router.post("/run")
def run_script(req: ScriptRunRequest) -> Dict[str, Any]:
    """Executes a script in the background and returns a run_id for status and live logs."""
    path = _safe_script_path(req.name)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Script '{req.name}' not found.")

    run_id = uuid.uuid4().hex[:12]
    tmp_dir = WORKSPACE_ROOT / "data" / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_file = tmp_dir / f"script_out_{run_id}.json"

    # Set up child environment
    env = os.environ.copy()
    backend_dir = str(WORKSPACE_ROOT / "backend")
    notebooks_dir = str(WORKSPACE_ROOT / "git_repo" / "notebooks")
    scripts_dir = str(SCRIPTS_DIR)
    ws_root = str(WORKSPACE_ROOT)

    existing_pythonpath = env.get("PYTHONPATH", "")
    parts = [backend_dir, ws_root, notebooks_dir, scripts_dir]
    if existing_pythonpath:
        parts.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    env["OBELISK_OUTPUT_FILE"] = str(out_file)
    env["PYTHONUNBUFFERED"] = "1"

    if req.params:
        for k, v in req.params.items():
            if v is not None:
                val = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                env[f"OBELISK_PARAM_{k.upper()}"] = val

    cmd = [_interpreter_path(), "-u", str(path)]
    if req.args:
        cmd.extend(req.args)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=ws_root,
            env=env,
            text=True,
            bufsize=1,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to spawn script process: {exc}")

    state = ScriptRunState(run_id, path.name, proc, out_file)
    with _RUNS_LOCK:
        _RUNS[run_id] = state

    # Start drain thread
    def _drain():
        try:
            if proc.stdout:
                for line in iter(proc.stdout.readline, ""):
                    clean_line = line.rstrip("\r\n")
                    state.logs.append(clean_line)
            proc.wait()
            state.ended_at = time.time()
            state.exit_code = proc.returncode
            if state.killed:
                state.status = "killed"
            elif proc.returncode == 0:
                state.status = "success"
            else:
                state.status = "failed"

            if out_file.is_file():
                try:
                    state.outputs = json.loads(out_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
                finally:
                    try:
                        out_file.unlink(missing_ok=True)
                    except OSError:
                        pass
        except Exception as drain_exc:
            logger.exception("Error while draining script process: %s", drain_exc)
            state.status = "failed"
            state.ended_at = time.time()

    threading.Thread(target=_drain, daemon=True).start()

    return {"run_id": run_id, "name": path.name, "status": "running"}


@router.get("/runs/{run_id}")
def get_run_status(run_id: str) -> Dict[str, Any]:
    """Returns the current execution status, live logs, duration, and exit code."""
    with _RUNS_LOCK:
        state = _RUNS.get(run_id)
    if not state:
        raise HTTPException(status_code=404, detail="Run not found.")
    return state.to_dict()


@router.post("/runs/{run_id}/kill")
def kill_run(run_id: str) -> Dict[str, Any]:
    """Terminates the script process and any children immediately."""
    with _RUNS_LOCK:
        state = _RUNS.get(run_id)
    if not state:
        raise HTTPException(status_code=404, detail="Run not found.")
    if state.status == "running":
        state.killed = True
        state.status = "killed"
        state.ended_at = time.time()
        _kill_process_tree(state.proc)
        state.logs.append("--- Process terminated by user (Kill switch) ---")
        return {"status": "killed", "run_id": run_id}
    return {"status": state.status, "run_id": run_id}
