"""
Marimo Service: Manages headless reactive Marimo notebook server subprocess.
Executes in git_repo/notebooks with subpath routing (--base-url /notebooks)
and guaranteed clean process lifecycle termination.
"""

import os
import sys
import time
import shutil
import urllib.request
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional
import psutil

class MarimoService:
    def __init__(
        self,
        notebooks_dir: Optional[Path] = None,
        port: int = 2718,
        base_url: str = "/notebooks",
    ):
        if notebooks_dir is None:
            # Resolves workspace root git_repo/notebooks
            root = Path(__file__).resolve().parent.parent.parent.parent
            self.notebooks_dir = (root / "git_repo" / "notebooks").resolve()
        else:
            self.notebooks_dir = notebooks_dir.resolve()

        self.notebooks_dir.mkdir(parents=True, exist_ok=True)
        self.port = port
        self.base_url = base_url if base_url.startswith("/") else f"/{base_url}"
        self._process: Optional[subprocess.Popen] = None
        self._started_at: Optional[float] = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}{self.base_url}"

    def is_running(self) -> bool:
        """Checks if process is alive and responds to HTTP probe."""
        if self._process is not None:
            if self._process.poll() is not None:
                self._process = None
                return False

        try:
            req = urllib.request.Request(self.url, headers={"User-Agent": "ObeliskHealthCheck"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return resp.status in (200, 302)
        except Exception:
            return False

    def start(self, port: Optional[int] = None, wait_timeout: float = 10.0) -> Dict[str, Any]:
        """
        Starts the headless Marimo server in directory mode.
        Safe against duplicate launches and port collisions.
        """
        if port:
            self.port = port

        if self.is_running():
            return {
                "status": "already_running",
                "port": self.port,
                "base_url": self.base_url,
                "url": self.url,
                "pid": self._process.pid if self._process else None,
            }

        cmd = [
            sys.executable,
            "-m",
            "marimo",
            "edit",
            str(self.notebooks_dir),
            "--port",
            str(self.port),
            "--headless",
            "--no-token",
            "--base-url",
            self.base_url,
            "--skip-update-check",
        ]

        # Launch background subprocess
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(self.notebooks_dir),
        )
        self._started_at = time.time()

        # Wait for HTTP readiness
        deadline = time.time() + wait_timeout
        ready = False
        while time.time() < deadline:
            if self._process.poll() is not None:
                out = self._process.stdout.read() if self._process.stdout else ""
                raise RuntimeError(f"Marimo failed to start with code {self._process.poll()}: {out}")
            if self.is_running():
                ready = True
                break
            time.sleep(0.3)

        if not ready:
            self.stop()
            raise TimeoutError(f"Marimo server did not respond at {self.url} within {wait_timeout}s.")

        return {
            "status": "running",
            "port": self.port,
            "base_url": self.base_url,
            "url": self.url,
            "pid": self._process.pid,
            "notebooks_dir": str(self.notebooks_dir),
        }

    def stop(self) -> Dict[str, Any]:
        """
        Cleanly terminates the Marimo process tree on Windows and Linux,
        guaranteeing zero orphaned processes holding the port.
        """
        if self._process is None and not self.is_running():
            return {"status": "not_running"}

        pid = self._process.pid if self._process else None

        if pid is None:
            try:
                for conn in psutil.net_connections(kind="tcp"):
                    if conn.laddr and conn.laddr.port == self.port and conn.status == psutil.CONN_LISTEN:
                        pid = conn.pid
                        break
            except Exception:
                pass

        if pid:
            try:
                parent = psutil.Process(pid)
                children = parent.children(recursive=True)
                for child in children:
                    try:
                        child.terminate()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                parent.terminate()
                _, alive = psutil.wait_procs(children + [parent], timeout=3.0)
                for p in alive:
                    try:
                        p.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception:
                if self._process:
                    self._process.kill()

        self._process = None
        self._started_at = None
        return {"status": "stopped", "previous_pid": pid}

    def get_status(self) -> Dict[str, Any]:
        """Returns live server status and metadata."""
        running = self.is_running()
        uptime_seconds = round(time.time() - self._started_at, 1) if (running and self._started_at) else 0.0
        notebooks = self.list_notebooks()

        return {
            "is_running": running,
            "port": self.port,
            "base_url": self.base_url,
            "url": self.url,
            "pid": self._process.pid if (running and self._process) else None,
            "uptime_seconds": uptime_seconds,
            "notebook_count": len(notebooks),
            "notebooks_dir": str(self.notebooks_dir),
        }

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """Lists all reactive Marimo notebook files in git_repo/notebooks."""
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)
        results = []
        for f in self.notebooks_dir.glob("*.py"):
            if f.name.startswith(("_", ".")) or f.name == "obelisk.py":
                continue
            stat = f.stat()
            results.append({
                "name": f.stem,
                "filename": f.name,
                "path": f.as_posix(),
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
            })
        return sorted(results, key=lambda x: x["name"])

    def _clear_cached_session(self, name: str) -> None:
        """Removes any cached Marimo session JSON so fresh notebook cells load."""
        try:
            clean = name.strip().replace(" ", "_").replace(".py", "")
            session_file = self.notebooks_dir / "__marimo__" / "session" / f"{clean}.py.json"
            if session_file.exists():
                session_file.unlink(missing_ok=True)
        except Exception:
            pass

    def delete_notebook(self, name: str) -> Dict[str, Any]:
        """Deletes a Marimo notebook file from git_repo/notebooks."""
        clean_name = name.strip().replace(" ", "_").replace(".py", "")
        target_file = self.notebooks_dir / f"{clean_name}.py"
        if not target_file.exists():
            raise FileNotFoundError(f"Notebook '{clean_name}' does not exist.")
        target_file.unlink()
        self._clear_cached_session(clean_name)
        return {
            "status": "deleted",
            "name": clean_name,
            "filename": target_file.name,
        }

    def create_notebook(
        self,
        name: str,
        custom_content: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """Creates or updates a starter Marimo notebook in the directory."""
        clean_name = name.strip().replace(" ", "_").replace(".py", "")
        target_file = self.notebooks_dir / f"{clean_name}.py"

        # If file exists, check whether we should update/overwrite
        if target_file.exists() and not overwrite:
            existing_content = target_file.read_text(encoding="utf-8")
            # If the user supplied custom_content and the existing file is missing data loading, overwrite it
            if custom_content and "obelisk.table" not in existing_content:
                target_file.write_text(custom_content, encoding="utf-8")
                self._clear_cached_session(clean_name)
                return {
                    "status": "updated",
                    "name": clean_name,
                    "filename": target_file.name,
                    "path": target_file.as_posix(),
                }
            elif not custom_content and clean_name.startswith("Explore_") and "obelisk.table" not in existing_content:
                table_target = clean_name.replace("Explore_", "")
                template = self._build_starter_template(clean_name, table_target)
                target_file.write_text(template, encoding="utf-8")
                self._clear_cached_session(clean_name)
                return {
                    "status": "updated",
                    "name": clean_name,
                    "filename": target_file.name,
                    "path": target_file.as_posix(),
                }
            elif not custom_content:
                return {
                    "status": "exists",
                    "name": clean_name,
                    "filename": target_file.name,
                    "path": target_file.as_posix(),
                }

        # Build template
        if custom_content:
            template = custom_content
        elif clean_name.startswith("Explore_"):
            table_target = clean_name.replace("Explore_", "")
            template = self._build_starter_template(clean_name, table_target)
        else:
            template = self._build_starter_template(clean_name, None)

        was_existing = target_file.exists()
        target_file.write_text(template, encoding="utf-8")
        self._clear_cached_session(clean_name)
        return {
            "status": "updated" if was_existing else "created",
            "name": clean_name,
            "filename": target_file.name,
            "path": target_file.as_posix(),
        }

    def _build_starter_template(self, notebook_name: str, table_path: Optional[str] = None) -> str:
        """Generates standard reactive Marimo template with obelisk data access."""
        if table_path:
            return f'''import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import obelisk
    return (mo, obelisk)


@app.cell
def _(mo):
    mo.md(
        r"""
        # 📊 Table Analysis: `{table_path}`
        Interactive exploratory workspace powered by **Obelisk Python SDK** and reactive **Marimo**.
        """
    )
    return


@app.cell
def _(obelisk):
    # Load table into Polars DataFrame via Zero-Cost STORAGE_URI
    df = obelisk.table("{table_path}").to_polars()
    df
    return (df,)


@app.cell
def _(df, mo):
    # Live summary
    mo.md(f"Loaded **{{len(df):,}}** rows across **{{len(df.columns)}}** columns from lakehouse.")
    return


if __name__ == "__main__":
    app.run()
'''
        return f'''import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import obelisk
    return (mo, obelisk)


@app.cell
def _(mo):
    mo.md(
        r"""
        # 📓 Notebook: {notebook_name}
        Interactive notebook querying Obelisk Lakehouse.
        """
    )
    return


@app.cell
def _(obelisk):
    # Query or load Lakehouse tables with Zero-Cost STORAGE_URI
    # Example: df = obelisk.table("bronze.orders").to_polars()
    df = obelisk.query("SELECT 'Obelisk Python SDK Ready' AS status, 1 AS ready")
    df
    return (df,)


if __name__ == "__main__":
    app.run()
'''

marimo_service = MarimoService()
