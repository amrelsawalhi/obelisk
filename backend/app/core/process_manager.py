"""
Process Service Manager:
Manages catalog services with run_mode="process" as host processes.

Why process mode: a service can be an execution engine that runs Obelisk's
own code. Running it from the backend venv needs zero image download, has no
dependency drift, and its logs are directly readable. Container mode remains
available per blueprint (or run_mode override) for production/air-gapped parity.
No shipped blueprint uses process mode today; the machinery stays because it is
the cheapest way to run a future host process without inventing a second deployer.

Layout:
  data/run/<service>_<n>.pid        process ids
  data/logs/<service>_<n>.log       combined stdout/stderr
  data/<service>_home/              service home dir, exposed as {service_home}
"""
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DATA_DIR = _PROJECT_ROOT / "data"
_RUN_DIR = _DATA_DIR / "run"
_LOG_DIR = _DATA_DIR / "logs"
_LOG_HANDLES: Dict[str, List[Any]] = {}


class ProcessServiceManager:
    """Start/stop/status for host-process services declared in service_blueprints."""

    def _scripts_dir(self) -> Path:
        return Path(sys.executable).resolve().parent

    def _resolve_cmd(self, argv: List[str]) -> List[str]:
        """Resolve console scripts from the backend venv rather than PATH."""
        base = self._scripts_dir()
        name = argv[0]
        for cand in (base / name, base / f"{name}.exe", base / f"{name}.cmd"):
            if cand.exists():
                return [str(cand)] + argv[1:]
        return argv

    def _build_env(self, bp: Any, service_home: Optional[Path]) -> Dict[str, str]:
        from app.config import settings

        env = os.environ.copy()
        env["PYTHONPATH"] = str((_PROJECT_ROOT / "backend").resolve()) + os.pathsep + env.get("PYTHONPATH", "")
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        if service_home is not None:
            env["SERVICE_HOME"] = str(service_home)
        for key in bp.pass_env or []:
            val = getattr(settings, key, None) or os.environ.get(key, "")
            if val:
                env[key] = str(val)
        return env

    def _pid_files(self, service_name: str) -> List[Path]:
        if not _RUN_DIR.exists():
            return []
        return sorted(_RUN_DIR.glob(f"{service_name}_*.pid"))

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            if os.name == "nt":
                out = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True, text=True, timeout=8,
                )
                return str(pid) in out.stdout
            os.kill(pid, 0)
            return True
        except Exception:
            return False

    def start(self, service_name: str, bp: Any, on_log: Any = None) -> Dict[str, Any]:
        """Starts all blueprint process commands (idempotent: stops existing first)."""
        _RUN_DIR.mkdir(parents=True, exist_ok=True)
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.stop(service_name)

        home = _DATA_DIR / f"{service_name}_home"
        home.mkdir(parents=True, exist_ok=True)
        env = self._build_env(bp, home)
        cwd = str((_PROJECT_ROOT / bp.process_cwd).resolve()) if bp.process_cwd else str(_PROJECT_ROOT)

        pids: List[int] = []
        handles: List[Any] = []

        for idx, argv in enumerate(bp.process_commands):
            expanded = [
                a.replace("{service_home}", str(home).replace("\\", "/"))
                 .replace("{project_root}", str(_PROJECT_ROOT).replace("\\", "/"))
                for a in argv
            ]
            cmd = self._resolve_cmd(expanded)
            log_path = _LOG_DIR / f"{service_name}_{idx}.log"
            log_file = open(log_path, "a", encoding="utf-8", errors="replace")
            log_file.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} starting: {' '.join(cmd)} ===\n")
            log_file.flush()
            handles.append(log_file)

            kwargs: Dict[str, Any] = {"cwd": cwd, "env": env, "stdout": log_file, "stderr": subprocess.STDOUT}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            proc = subprocess.Popen(cmd, **kwargs)
            (_RUN_DIR / f"{service_name}_{idx}.pid").write_text(str(proc.pid), encoding="utf-8")
            pids.append(proc.pid)
            if on_log:
                on_log(f"started {argv[0]} (pid {proc.pid})")

        _LOG_HANDLES[service_name] = handles
        return {"pids": pids, "log_dir": str(_LOG_DIR), "home": str(home)}

    def _kill_tree(self, pid: int) -> None:
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, timeout=15)
            else:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
        except Exception:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass

    def stop(self, service_name: str) -> None:
        """Terminates the whole process tree (webserver spawns a gRPC child)."""
        for pid_file in self._pid_files(service_name):
            try:
                self._kill_tree(int(pid_file.read_text(encoding="utf-8").strip()))
            except Exception:
                pass
            pid_file.unlink(missing_ok=True)
        for handle in _LOG_HANDLES.pop(service_name, []):
            try:
                handle.close()
            except Exception:
                pass

    def destroy(self, service_name: str) -> None:
        """Stop + delete the service home directory (mirrors container volume destroy)."""
        self.stop(service_name)
        shutil.rmtree(_DATA_DIR / f"{service_name}_home", ignore_errors=True)

    def status(self, service_name: str, reachable: Optional[bool] = None) -> Dict[str, Any]:
        pids = [int(f.read_text(encoding="utf-8").strip()) for f in self._pid_files(service_name)]
        alive = [p for p in pids if self._pid_alive(p)]
        state = "stopped"
        if alive:
            state = "running" if reachable in (None, True) else "starting"
        return {"service_name": service_name, "pids": pids, "alive_pids": alive,
                "status": state, "reachable": reachable}

    def logs(self, service_name: str, tail: int = 200) -> str:
        """Tail of the combined per-process logs (readable — a process-mode benefit)."""
        chunks: List[str] = []
        for log_path in sorted(_LOG_DIR.glob(f"{service_name}_*.log")):
            try:
                lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
                chunks.append(f"--- {log_path.name} ---\n" + "\n".join(lines[-tail:]))
            except Exception:
                continue
        return "\n".join(chunks)


process_manager = ProcessServiceManager()
