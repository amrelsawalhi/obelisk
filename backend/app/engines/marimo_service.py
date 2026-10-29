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
from typing import Dict, Any, List, Optional, Tuple
import psutil

from app.config import settings

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
        # `get_status` is polled by the home dashboard and the notebooks view, and
        # each call pays a socket probe against a server that is usually off. Short
        # cache keeps those polls cheap without making a start/stop look stale
        # (both invalidate it).
        self._status_cache: Optional[Tuple[float, Dict[str, Any]]] = None
        # Marimo logs continuously. Draining that output into a file instead of an
        # unread PIPE keeps the child from blocking on write() once the ~64 KB pipe
        # buffer fills up, which used to freeze the notebook server.
        self.log_path = Path(__file__).resolve().parents[3] / "data" / "logs" / "marimo.log"
        self._log_handle: Optional[Any] = None
        # Our own child only: stop() must never terminate a process it did not start.
        self.pid_file = Path(__file__).resolve().parents[3] / "data" / "marimo.pid"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}{self.base_url}"

    @property
    def session_ttl_seconds(self) -> int:
        """Seconds a notebook kernel outlives its browser tab (0 = never expires).

        `marimo edit` leaves this unset by default, so a closed tab keeps its
        kernel (~155 MB resident) until the backend restarts. Passing the value
        to `--session-ttl` makes Marimo close the session -- and shut down its
        kernel -- that many seconds after the websocket disconnects.
        """
        try:
            configured = int(settings.MARIMO_SESSION_TTL_SECONDS)
        except (TypeError, ValueError):
            return 0
        return max(0, configured)

    def is_running(self, timeout: float = 1.5) -> bool:
        """Checks if process is alive and responds to HTTP probe."""
        if self._process is not None:
            if self._process.poll() is not None:
                self._process = None
                return False

        try:
            req = urllib.request.Request(self.url, headers={"User-Agent": "ObeliskHealthCheck"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status in (200, 302)
        except Exception:
            return False

    def start(self, port: Optional[int] = None, wait_timeout: float = 10.0) -> Dict[str, Any]:
        """
        Starts the headless Marimo server in directory mode.
        Safe against duplicate launches and port collisions.

        The port is fixed by configuration. ``start(port=8000)`` used to be accepted and
        then remembered, so the next ``stop()`` terminated whatever listened on 8000 —
        including the app itself. A caller that wants another port changes the setting.
        """
        if port and int(port) != self.port:
            raise ValueError(
                f"Marimo is configured to run on port {self.port}; refusing to start it "
                f"on {port}. Change the configured port instead of passing a different one."
            )

        if self.is_running():
            # A server can outlive the backend that started it (restart, crash). Adopt it —
            # but only when its command line is marimo, so ``stop()`` can end it safely and
            # somebody else's listener is never claimed.
            if self._process is None and not self._recorded_child_alive():
                self._adopt_listener()
            return {
                "status": "already_running",
                "port": self.port,
                "base_url": self.base_url,
                "url": self.url,
                "pid": self._process.pid if self._process else None,
                "session_ttl_seconds": self.session_ttl_seconds,
            }

        cmd = [
            sys.executable,
            "-m",
            "marimo",
            "edit",
            str(self.notebooks_dir),
            "--host",
            str(settings.MARIMO_HOST),
            "--port",
            str(self.port),
            "--headless",
            "--no-token",
            "--base-url",
            self.base_url,
            "--skip-update-check",
        ]

        # Reclaim notebook kernels once their tab disconnects. `marimo edit` has no
        # default TTL, so without this each kernel stays resident until the backend
        # restarts; 0 keeps that legacy behaviour.
        ttl_seconds = self.session_ttl_seconds
        if ttl_seconds > 0:
            cmd += ["--session-ttl", str(ttl_seconds)]

        # Stream the child's output to disk (never an unread PIPE) so it can never
        # deadlock on a full pipe buffer.
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = open(self.log_path, "a", encoding="utf-8", errors="replace")

        from app.core.resource_advisor import advisor

        marimo_env = os.environ.copy()
        for k, v in advisor.get_child_env_limits().items():
            marimo_env.setdefault(k, v)
        popen_kwargs: Dict[str, Any] = {"env": marimo_env}
        preexec = advisor.make_posix_rlimit_preexec()
        if preexec is not None:
            popen_kwargs["preexec_fn"] = preexec

        # Launch background subprocess
        self._process = subprocess.Popen(
            cmd,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            cwd=str(self.notebooks_dir),
            **popen_kwargs,
        )
        # Record the child so a restarted backend can still stop *this* process — and
        # nothing else. The PID alone is not enough; ``stop`` re-checks the command line.
        try:
            self.pid_file.parent.mkdir(parents=True, exist_ok=True)
            self.pid_file.write_text(str(self._process.pid), encoding="utf-8")
        except Exception:
            pass
        self._started_at = time.time()
        self._status_cache = None

        # Wait for HTTP readiness
        deadline = time.time() + wait_timeout
        ready = False
        while time.time() < deadline:
            if self._process.poll() is not None:
                tail = self._tail_log()
                self._close_log_handle()
                raise RuntimeError(f"Marimo failed to start with code {self._process.poll()}: {tail}")
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
            "session_ttl_seconds": ttl_seconds,
        }

    def _tail_log(self, max_bytes: int = 4096) -> str:
        """Returns the tail of the Marimo log, for startup-failure diagnostics."""
        try:
            if not self.log_path.exists():
                return ""
            with open(self.log_path, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - max_bytes))
                return fh.read().decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _close_log_handle(self) -> None:
        """Closes the Marimo log file handle if one is open."""
        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except Exception:
                pass
            self._log_handle = None

    def _adopt_listener(self) -> None:
        """Records an already-running marimo left behind by a previous backend.

        The PID comes from the listening socket, and it is only accepted when its command
        line is marimo. Without this, a restart made the server unstoppable (``stop()`` has
        no child) — which is what the old kill-by-port did, at the price of killing
        anything else on the port.
        """
        try:
            for conn in psutil.net_connections(kind="tcp"):
                if not (conn.laddr and conn.laddr.port == self.port):
                    continue
                if conn.status != psutil.CONN_LISTEN or not conn.pid:
                    continue
                try:
                    argv = [part.lower() for part in psutil.Process(conn.pid).cmdline()]
                except Exception:
                    return
                names = {part.replace("\\", "/").rsplit("/", 1)[-1] for part in argv}
                if names & {"marimo", "marimo.exe"} or "marimo" in argv:
                    self.pid_file.parent.mkdir(parents=True, exist_ok=True)
                    self.pid_file.write_text(str(conn.pid), encoding="utf-8")
                return
        except Exception:
            return

    def _read_pid_file(self) -> Optional[int]:
        try:
            return int(self.pid_file.read_text(encoding="utf-8").strip())
        except Exception:
            return None

    def _clear_pid_file(self) -> None:
        try:
            self.pid_file.unlink(missing_ok=True)
        except Exception:
            pass

    def _recorded_child_alive(self) -> bool:
        """True only when the recorded PID is the marimo child we launched.

        ``marimo`` has to be an argument of its own (``-m marimo`` or the launcher), not a
        substring of a path: a directory that merely contains the word — this repository's
        own tests do — must not make a stranger look like ours. A recycled PID is the
        failure this replaces, so the check errs towards "not ours".
        """
        pid = self._read_pid_file()
        if not pid:
            return False
        try:
            argv = [part.lower() for part in psutil.Process(pid).cmdline()]
        except Exception:
            return False
        for part in argv:
            name = part.replace("\\", "/").rsplit("/", 1)[-1]
            if name in ("marimo", "marimo.exe") or part == "marimo":
                return True
        return False

    def stop(self) -> Dict[str, Any]:
        """
        Terminates the Marimo process tree this service started.

        Only our own child is touched. The previous implementation looked the PID up *by
        port* and killed whatever it found, so ``start(port=8000)`` followed by ``stop()``
        took down the app — or any other program listening there. A PID recorded at launch
        is re-verified against the child's command line, so a stale file after a restart
        cannot target the wrong process either.
        """
        if self._process is None and not self._recorded_child_alive():
            self._clear_pid_file()
            self._close_log_handle()
            return {"status": "not_running"}

        if self._process is not None:
            pid = self._process.pid
        else:
            pid = self._read_pid_file()

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
        self._status_cache = None
        self._clear_pid_file()
        self._close_log_handle()
        return {"status": "stopped", "previous_pid": pid}

    def get_status(self) -> Dict[str, Any]:
        """Returns live server status and metadata, cached for thirty seconds.

        Every call otherwise pays a socket probe, which is ~1.8 s when the server
        is down - and the home dashboard polls this endpoint every 15 s. The probe
        itself uses a short timeout because this is a status light, not a
        lifecycle decision (`start` keeps the longer timeout, and both `start` and
        `stop` drop this cache).
        """
        cached = self._status_cache
        if cached is not None and (time.monotonic() - cached[0]) < 30.0:
            return dict(cached[1])

        running = self.is_running(timeout=0.4)
        uptime_seconds = round(time.time() - self._started_at, 1) if (running and self._started_at) else 0.0
        notebooks = self.list_notebooks()

        status = {
            "is_running": running,
            "port": self.port,
            "base_url": self.base_url,
            "url": self.url,
            "pid": self._process.pid if (running and self._process) else None,
            "uptime_seconds": uptime_seconds,
            "notebook_count": len(notebooks),
            "notebooks_dir": str(self.notebooks_dir),
            "session_ttl_seconds": self.session_ttl_seconds,
        }
        self._status_cache = (time.monotonic(), status)
        return dict(status)

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
