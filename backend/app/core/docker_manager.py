import os
import subprocess
from typing import Dict, Any, List, Optional
import docker
from docker.models.containers import Container
from docker.errors import NotFound, DockerException
from app.config import settings
from app.core.resource_advisor import advisor
from app.core.service_blueprints import get_blueprint

MANAGED_LABEL = "managed_by"
OBELISK_TAG = "obelisk"

def get_docker_client() -> docker.DockerClient:
    """
    Returns a configured DockerClient.
    Connects to local Docker engine (/var/run/docker.sock or DOCKER_HOST).
    On Windows, if DOCKER_HOST is not set, attempts to resolve WSL2 host IP.
    """
    docker_host = settings.DOCKER_HOST or os.environ.get("DOCKER_HOST")
    if docker_host:
        return docker.DockerClient(base_url=docker_host)

    try:
        client = docker.from_env(timeout=2)
        client.ping()
        return client
    except Exception:
        if os.name == "nt":
            # Probe WSL2 IP if running in Windows environment
            try:
                out = subprocess.check_output(["wsl", "hostname", "-I"], text=True, timeout=2).strip()
                wsl_ip = out.split()[0]
                client = docker.DockerClient(base_url=f"tcp://{wsl_ip}:2375", timeout=2)
                client.ping()
                return client
            except Exception:
                pass
        raise

class DockerManager:
    """
    Localhost Docker Manager:
    Provisions and manages containers strictly on localhost with dynamic hardware sizing,
    attached to obelisk_net and tagged with managed_by=obelisk.
    """
    def __init__(self, client: Optional[docker.DockerClient] = None):
        self._client = client

    @property
    def client(self) -> Optional[docker.DockerClient]:
        if self._client is None:
            try:
                self._client = get_docker_client()
            except Exception:
                return None
        return self._client

    def ensure_network(self) -> Any:
        """Ensures the obelisk_net bridge network exists."""
        if not self.client:
            raise RuntimeError("Docker engine is not running on localhost.")
        net_name = settings.OBELISK_NET
        try:
            return self.client.networks.get(net_name)
        except NotFound:
            return self.client.networks.create(net_name, driver="bridge")

    def _get_container_name(self, service_name: str) -> str:
        return f"obelisk_{service_name.lower()}"

    def get_service_container(self, service_name: str) -> Optional[Container]:
        """
        Retrieves a container by service name.
        Enforces safety rule: never inspect or return containers that lack managed_by=obelisk.
        """
        try:
            if not self.client:
                return None
            name = self._get_container_name(service_name)
            container = self.client.containers.get(name)
            labels = container.attrs.get("Config", {}).get("Labels", {}) or {}
            if labels.get(MANAGED_LABEL) != OBELISK_TAG:
                raise ValueError(
                    f"Container {name} exists but is not managed by Obelisk (missing {MANAGED_LABEL}={OBELISK_TAG}). Refusing access."
                )
            return container
        except ValueError:
            # Safety-rule violations MUST propagate: silently returning None here would
            # let callers treat a foreign container as absent instead of refused.
            # (test_docker_manager_safety_rule proves this invariant.)
            raise
        except Exception:
            return None

    def list_managed_containers(self) -> List[Container]:
        """Lists all containers bearing the managed_by=obelisk label."""
        try:
            if not self.client:
                return []
            return self.client.containers.list(
                all=True,
                filters={"label": f"{MANAGED_LABEL}={OBELISK_TAG}"}
            )
        except Exception:
            return []

    def deploy_service(
        self,
        service_name: str,
        image: Optional[str] = None,
        command: Optional[Any] = None,
        environment: Optional[Dict[str, str]] = None,
        ports: Optional[Dict[str, Any]] = None,
        memory_mb: Optional[int] = None,
        volumes: Optional[Dict[str, Any]] = None,
        pull_progress_cb: Optional[Any] = None,
    ) -> Container:
        """
        Deploys or recreates a managed service container on localhost.
        Applies dynamic resource limits from ResourceAdvisor, attaches to obelisk_net,
        and enforces managed_by=obelisk label.
        """
        if not self.client:
            raise RuntimeError(
                "Docker engine is not reachable. Start Docker Desktop (or set DOCKER_HOST) and try again."
            )

        self.ensure_network()
        name = self._get_container_name(service_name)

        # Check existing container (redeploy path) — stop it first, keeping data volumes
        existing = self.get_service_container(service_name)

        if existing:
            self.stop_service(service_name)

        # Compute dynamic memory allocation
        if memory_mb is None:
            rec = advisor.recommend_allocation(service_name)
            alloc_mb = rec["recommended_ram_mb"]
        else:
            alloc_mb = memory_mb
        mem_bytes = alloc_mb * 1024 * 1024

        # Blueprint-driven defaults; explicit arguments always win.
        bp = get_blueprint(service_name)
        if bp is not None:
            img = image or bp.image
            cmd = command or bp.command
            env = dict(bp.env)
            if environment:
                env.update(environment)
            port_bindings = {
                c_port: ("0.0.0.0", h_port) for c_port, h_port in bp.ports.items()
            }
            if ports:
                port_bindings.update(ports)
            vol_map = {}
            if bp.volume_name and bp.volume_path:
                # Named volume = persistence: survives stop & redeploy; removed only
                # by destroy_service(). docker auto-creates named volumes on first run.
                vol_map[bp.volume_name] = {"bind": bp.volume_path, "mode": "rw"}
            if volumes:
                vol_map.update(volumes)
        else:
            # Generic fallback (e.g. test services): bare container, no data volume
            img = image or "alpine:latest"
            cmd = command or ["tail", "-f", "/dev/null"]
            env = environment or {}
            port_bindings = ports or {}
            vol_map = volumes or {}

        labels = {
            MANAGED_LABEL: OBELISK_TAG,
            "service": service_name.lower(),
        }

        # Pull explicitly: failures (auth, network, unknown tag) raise here with a
        # clear error instead of a silent multi-minute hang inside containers.run.
        # With a progress callback, layer-by-layer pull progress is streamed to it.
        try:
            if pull_progress_cb is not None:
                self.pull_image_with_progress(img, pull_progress_cb)
            else:
                self.client.images.pull(img)
        except Exception:
            # Image may already exist locally (air-gapped hosts) — let containers.run try.
            pass

        # Create and run container
        container = self.client.containers.run(
            image=img,
            name=name,
            command=cmd,
            environment=env,
            ports=port_bindings,
            network=settings.OBELISK_NET,
            mem_limit=mem_bytes,
            labels=labels,
            volumes=vol_map,
            detach=True,
            restart_policy={"Name": "unless-stopped"},
        )

        return container

    def stop_service(self, service_name: str) -> None:
        """
        Stops and removes a managed container, PRESERVING its named data volume.
        Enforces safety: never stops containers that lack managed_by=obelisk.
        Use destroy_service() to permanently delete persisted data.
        """
        container = self.get_service_container(service_name)
        if container:
            # Enforce label check
            labels = container.attrs.get("Config", {}).get("Labels", {}) or {}
            if labels.get(MANAGED_LABEL) != OBELISK_TAG:
                raise ValueError(
                    f"Refusing to stop container {container.name}: missing {MANAGED_LABEL}={OBELISK_TAG}"
                )
            container.stop(timeout=5)
            # v=True removes only ANONYMOUS volumes; blueprint data lives in NAMED
            # volumes which are preserved here and removed only by destroy_service().
            container.remove(v=True, force=True)

    def destroy_service(self, service_name: str) -> None:
        """
        Stops the managed container AND permanently deletes its named data volume.
        This is the only path that removes persisted service data.
        """
        self.stop_service(service_name)
        bp = get_blueprint(service_name)
        if bp and bp.volume_name and self.client:
            try:
                vol = self.client.volumes.get(bp.volume_name)
                vol.remove(force=True)
            except NotFound:
                pass

    def get_container_logs(self, service_name: str, tail: int = 200) -> str:
        """Returns the last `tail` log lines of a managed container."""
        container = self.get_service_container(service_name)
        if not container:
            return ""
        try:
            return container.logs(tail=tail, timestamps=True).decode("utf-8", errors="replace")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch logs for '{service_name}': {e}")

    def pull_image_with_progress(self, image: str, on_event: Any) -> None:
        """
        Pulls an image, streaming layer progress to on_event(layer_id, status, current, total).
        Raises on pull failure so callers can surface registry/auth/network errors clearly.
        """
        last_err: Optional[Exception] = None
        try:
            # Low-level api.pull properly supports stream+decode (the models-level
            # images.pull ignores the stream parameter with a UserWarning).
            if ":" in image.rsplit("/", 1)[-1]:
                repo, tag = image.rsplit(":", 1)
            else:
                repo, tag = image, "latest"
            for chunk in self.client.api.pull(repo, tag, stream=True, decode=True):
                if not isinstance(chunk, dict):
                    continue
                layer = str(chunk.get("id") or "")
                status = str(chunk.get("status") or "")
                detail = chunk.get("progressDetail") or {}
                if on_event and (layer or status):
                    on_event(layer, status, int(detail.get("current", 0)), int(detail.get("total", 0)))
        except Exception as e:
            last_err = e
        if last_err is not None:
            raise RuntimeError(f"Image pull failed for '{image}': {last_err}")

    def run_post_start_commands(self, service_name: str, commands: List[str], on_log: Any = None) -> None:
        """
        Runs blueprint post-start init commands inside the managed container
        (e.g. superset db upgrade / admin creation). Non-zero exits are tolerated
        and reported via on_log — idempotent redeploys expect 'already exists' errors.
        """
        container = self.get_service_container(service_name)
        if not container or container.status != "running":
            raise RuntimeError(f"Container for '{service_name}' is not running — cannot run init commands.")
        for cmd in commands:
            exit_code, output = container.exec_run(["/bin/sh", "-c", cmd])
            text = output.decode("utf-8", errors="replace") if isinstance(output, (bytes, bytearray)) else str(output)
            if on_log:
                on_log(cmd, text)
            if exit_code not in (0, None) and on_log:
                on_log(cmd, f"(exit code {exit_code} — tolerated for redeploy idempotency)")

    def probe_readiness(self, service_name: str, host_port: Optional[int] = None) -> Dict[str, Any]:
        """
        Runs the blueprint readiness probe and returns {"ready": bool, "detail": str}.
        A running container is NOT proof the service inside accepts work.
        """
        bp = get_blueprint(service_name)
        if bp is None or bp.readiness is None:
            container = self.get_service_container(service_name)
            running = bool(container) and container.status == "running"
            return {"ready": running, "detail": "container running" if running else "container not running"}

        probe = bp.readiness
        port = host_port or probe.host_port
        try:
            if probe.kind == "pg_isready":
                container = self.get_service_container(service_name)
                if not container or container.status != "running":
                    return {"ready": False, "detail": "container not running"}
                args = ["pg_isready", "-U", probe.user or "postgres", "-d", probe.db or "postgres"]
                exit_code, output = container.exec_run(args)
                detail = output.decode("utf-8", errors="replace").strip()
                return {"ready": exit_code == 0, "detail": detail}

            if probe.kind == "http":
                import httpx
                resp = httpx.get(f"http://127.0.0.1:{port}{probe.path}", timeout=3.0)
                return {"ready": resp.status_code == probe.expected_status, "detail": f"HTTP {resp.status_code}"}

            if probe.kind == "tcp":
                import socket
                with socket.create_connection(("127.0.0.1", port), timeout=3.0):
                    return {"ready": True, "detail": f"port {port} accepting connections"}
        except Exception as e:
            return {"ready": False, "detail": str(e)}

        return {"ready": False, "detail": f"unknown probe kind '{probe.kind}'"}

    def engine_status(self) -> Dict[str, Any]:
        """Checks Docker engine reachability (fresh probe; failures are never cached)."""
        try:
            get_docker_client()
            return {"available": True, "error": None}
        except Exception as e:
            return {"available": False, "error": str(e)}

    def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """Returns runtime status and resource metrics for a managed service."""
        container = self.get_service_container(service_name)
        if not container:
            return {
                "service_name": service_name,
                "status": "stopped",
                "container_id": None,
                "allocated_memory_bytes": 0,
                "allocated_memory_mb": 0,
                "ports": {},
            }

        # Reload live attributes
        container.reload()
        host_config = container.attrs.get("HostConfig", {}) or {}
        mem_bytes = host_config.get("Memory", 0)
        net_settings = container.attrs.get("NetworkSettings", {}) or {}
        ports = net_settings.get("Ports", {}) or {}

        return {
            "service_name": service_name,
            "status": container.status,
            "container_id": container.id,
            "allocated_memory_bytes": mem_bytes,
            "allocated_memory_mb": mem_bytes // (1024 * 1024),
            "ports": ports,
        }

docker_mgr = DockerManager()
