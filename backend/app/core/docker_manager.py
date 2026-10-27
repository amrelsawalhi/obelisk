import os
import re
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional
import docker
from docker.models.containers import Container
from docker.errors import NotFound, DockerException
from app.config import settings
from app.core.resource_advisor import advisor
from app.core.service_blueprints import get_blueprint

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

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
        self._daemon_is_wsl: Optional[bool] = None

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

        # Check existing container (redeploy path) — remove it first, keeping data volumes
        existing = self.get_service_container(service_name)

        if existing:
            # A deploy always produces a *new* container, and Docker refuses a duplicate
            # name. remove(v=True) drops only anonymous volumes: the blueprint's NAMED data
            # volume survives, so the data (and the credentials it was initialized with)
            # carry over. The API refuses to deploy while a container exists — this branch
            # keeps the manager itself consistent for direct callers.
            existing.remove(v=True, force=True)

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
            for host_rel, mount in (bp.bind_mounts or {}).items():
                host_path = str(_PROJECT_ROOT / host_rel)
                vol_map[self._to_daemon_path(host_path)] = dict(mount)
            if volumes:
                vol_map.update(volumes)

            if bp.build_dockerfile:
                # Local image: build from the project Dockerfile instead of pulling.
                # Skipped when the image already exists locally — code is bind-mounted,
                # so only Dockerfile/dependency changes need a rebuild (a rebuild can
                # take 10-30 min on slow networks; force one by deleting the image).
                try:
                    self.client.images.get(bp.image)
                    image_exists = True
                except Exception:
                    image_exists = False
                if not image_exists:
                    self.build_image_with_progress(bp.build_dockerfile, bp.image, pull_progress_cb)
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
        # Locally-built images (bp.build_dockerfile) skip the registry pull.
        already_built = bp is not None and bool(bp.build_dockerfile)
        if not already_built:
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
        Stops a managed container **in place**, keeping the container itself.

        Everything it was created with survives untouched — image, environment (so the
        credentials the data volume was initialized with), labels, published ports and the
        named data volume. ``start_service`` brings it back exactly as it was; nothing is
        rebuilt and no credentials can drift. Enforces safety: never touches containers
        that lack managed_by=obelisk. Use destroy_service() to remove the container and
        its data.
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

    def start_service(self, service_name: str) -> Optional[Container]:
        """
        Starts a previously stopped managed container, unchanged.

        This is ``docker start``: the same container id, the same environment (including
        the credentials it was deployed with) and the same named volume. If nothing was
        ever deployed there is nothing to start — the caller has to deploy instead.
        """
        container = self.get_service_container(service_name)
        if not container:
            raise LookupError(
                f"No managed container for '{service_name}' to start. Deploy it first."
            )
        if container.status != "running":
            container.start()
        return container

    def destroy_service(self, service_name: str) -> None:
        """
        Removes the managed container AND permanently deletes its named data volume.
        This is the only path that removes persisted service data — after it, the service
        is gone as if never deployed, and the next deploy starts from an empty volume.
        """
        container = self.get_service_container(service_name)
        if container:
            # Label check lives in get_service_container (it refuses foreign containers).
            # force=True removes a running container too, so a running service can be
            # destroyed in one step. v=True drops only ANONYMOUS volumes; blueprint data
            # lives in the NAMED volume removed just below.
            container.remove(v=True, force=True)
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

    def _to_daemon_path(self, host_path: str) -> str:
        """
        Translates Windows host paths to WSL paths when the Docker daemon runs inside
        WSL (tcp:// endpoint): a raw WSL dockerd expects /mnt/<drive>/... mount specs,
        unlike Docker Desktop (named pipe) which accepts Windows paths natively.
        """
        if self._daemon_is_wsl is None:
            try:
                base_url = getattr(getattr(self.client, "api", None), "base_url", "") or ""
                self._daemon_is_wsl = os.name == "nt" and base_url.startswith(("tcp://", "http://"))
            except Exception:
                self._daemon_is_wsl = False
        if self._daemon_is_wsl and re.match(r"^[A-Za-z]:[\\/]", host_path):
            drive = host_path[0].lower()
            rest = host_path[3:].replace("\\", "/")
            return f"/mnt/{drive}/{rest}"
        return host_path

    def build_image_with_progress(self, dockerfile: str, tag: str, on_event: Any = None) -> None:
        """
        Builds a local image from the project root (e.g. a service Dockerfile),
        streaming build log lines to on_event(line). Raises on build failure.
        """
        last_err: Optional[Exception] = None
        try:
            for chunk in self.client.api.build(
                path=str(_PROJECT_ROOT), dockerfile=dockerfile, tag=tag, decode=True, rm=True,
            ):
                line = str(chunk.get("stream") or chunk.get("status") or chunk.get("error") or "").strip()
                if line and on_event:
                    on_event(line)
                if chunk.get("error"):
                    last_err = RuntimeError(chunk["error"])
        except Exception as e:
            last_err = e
        if last_err is not None:
            raise RuntimeError(f"Image build failed for {dockerfile}: {last_err}")

    def _sibling_probe_target(
        self, service_name: str, container_port: Optional[int]
    ) -> Optional[str]:
        """Container hostname on ``obelisk_net``, when probing it can work.

        Only useful from inside a container: ``127.0.0.1`` there is the backend
        itself, and a sibling service answers on its container port under its
        container name. From a host install the name does not resolve, so returning
        None keeps the host probe path exactly as it was.
        """
        if container_port is None:
            return None
        if not (os.path.exists("/.dockerenv") or os.environ.get("OBELISK_IN_CONTAINER") == "1"):
            return None
        try:
            container = self.get_service_container(service_name)
            if container is None or container.status != "running":
                return None
            networks = (container.attrs.get("NetworkSettings", {}) or {}).get("Networks", {}) or {}
            if settings.OBELISK_NET not in networks:
                return None
        except Exception:
            return None
        return f"{self._get_container_name(service_name)}:{container_port}"

    def published_port_owner(self, host_port: int) -> Optional[Dict[str, Any]]:
        """Which running container publishes ``host_port`` on the host, and on which port.

        The platform stores ``127.0.0.1:5434`` because that is how *it* reaches a source
        from the host. A sibling container cannot use that address — inside a container
        ``127.0.0.1`` is the container itself — so a consumer living on ``obelisk_net``
        has to be pointed at the publishing container's name and container port instead.
        Returns ``{"container": str, "container_port": int}`` or ``None`` when nothing
        publishes the port (an external host, or a service Obelisk does not manage).
        """
        if not self.client:
            return None
        try:
            containers = self.client.containers.list()
        except Exception:
            return None
        wanted = str(int(host_port))
        for container in containers:
            try:
                bindings_map = (container.attrs.get("NetworkSettings", {}) or {}).get("Ports", {}) or {}
            except Exception:
                continue
            for container_port, bindings in (bindings_map or {}).items():
                for binding in bindings or []:
                    if str((binding or {}).get("HostPort") or "") != wanted:
                        continue
                    try:
                        port_number = int(str(container_port).split("/")[0])
                    except (TypeError, ValueError):
                        continue
                    return {"container": container.name, "container_port": port_number}
        return None

    def is_on_network(self, container_name: str, network: Optional[str] = None) -> bool:
        """Whether ``container_name`` is *already* attached to ``network``. Reads only.

        A preview must be safe to call from a GET: it may report that a sync would attach
        a container, but it must not attach one to find out.
        """
        if not self.client or not container_name:
            return False
        net_name = network or settings.OBELISK_NET
        try:
            container = self.client.containers.get(container_name)
            networks = (container.attrs.get("NetworkSettings", {}) or {}).get("Networks", {}) or {}
            return net_name in networks
        except Exception:
            return False

    def ensure_on_network(self, container_name: str, network: Optional[str] = None) -> bool:
        """Attaches ``container_name`` to ``network`` (default ``obelisk_net``) if absent.

        Containers deployed by compose can land on their project's default network, where
        a sibling on ``obelisk_net`` cannot resolve them by name. Connecting is additive
        and needs no restart, so this never disrupts a running service.
        """
        if not self.client or not container_name:
            return False
        net_name = network or settings.OBELISK_NET
        try:
            container = self.client.containers.get(container_name)
            networks = (container.attrs.get("NetworkSettings", {}) or {}).get("Networks", {}) or {}
            if net_name in networks:
                return True
            self.ensure_network().connect(container)
            return True
        except Exception:
            return False

    @staticmethod
    def _container_self_address(container) -> Optional[str]:
        """The container's own bridge address, or ``None`` when Docker did not say.

        The official postgres image ships ``trust`` for ``local``/``127.0.0.1``/``::1``
        and ``scram-sha-256`` for everything else, so a login only gets its password
        checked when it arrives on an interface rather than on loopback.
        """
        networks = ((container.attrs or {}).get("NetworkSettings") or {}).get("Networks") or {}
        for net in networks.values():
            address = (net or {}).get("IPAddress")
            if address:
                return address
        return None

    def _authenticate(self, container, user: str, db: str, password: str) -> Optional[str]:
        """Logs in with the credentials the deploy intends to register.

        Returns ``None`` when they work (or when the question cannot be answered — an
        image without a ``psql`` client, or a container Docker reported no address for;
        liveness was already proven, so neither is treated as a failure). Otherwise it
        returns the reason they do not.
        """
        address = self._container_self_address(container)
        if not address:
            return None
        args = ["psql", "-h", address, "-U", user, "-d", db, "-tAc", "select 1"]
        try:
            exit_code, output = container.exec_run(args, environment={"PGPASSWORD": password})
        except Exception:
            # Liveness is already proven; a failed exec is inconclusive, not bad credentials.
            return None
        text = (output or b"").decode("utf-8", errors="replace").strip()
        if exit_code == 0:
            return None
        if exit_code == 127 or "command not found" in text:
            return None
        return text or f"psql exited {exit_code}"

    def probe_readiness(
        self,
        service_name: str,
        host_port: Optional[int] = None,
        user: Optional[str] = None,
        db: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Runs the blueprint readiness probe and returns {"ready": bool, "detail": str}.
        A running container is NOT proof the service inside accepts work.

        ``http`` and ``tcp`` probes try ``127.0.0.1`` first (correct when the backend
        runs on the host, where the container publishes its port) and fall back to the
        container name on ``obelisk_net`` (correct when the backend is itself a
        container, where its own localhost has nothing on that port).
        """
        bp = get_blueprint(service_name)
        if bp is None or bp.readiness is None:
            container = self.get_service_container(service_name)
            running = bool(container) and container.status == "running"
            return {"ready": running, "detail": "container running" if running else "container not running"}

        probe = bp.readiness
        port = host_port or probe.host_port
        container_port: Optional[int] = None
        try:
            container_port = int(str(bp.primary_container_port).split("/")[0])
        except (TypeError, ValueError):
            container_port = port
        try:
            if probe.kind == "pg_isready":
                container = self.get_service_container(service_name)
                if not container or container.status != "running":
                    return {"ready": False, "detail": "container not running"}
                user_name = user or probe.user or "postgres"
                db_name = db or probe.db or "postgres"
                args = ["pg_isready", "-U", user_name, "-d", db_name]
                exit_code, output = container.exec_run(args)
                detail = output.decode("utf-8", errors="replace").strip()
                if exit_code != 0:
                    return {"ready": False, "detail": detail}
                if password:
                    # pg_isready only proves the socket answers — it never authenticates.
                    # The image honors POSTGRES_USER/PASSWORD/DB on its first boot alone,
                    # so a deploy over an initialized volume can advertise credentials the
                    # cluster does not have. Without this check the deploy would call that
                    # "ready" and register a Connection nobody can log in with.
                    auth_error = self._authenticate(container, user_name, db_name, password)
                    if auth_error:
                        return {"ready": False, "detail": f"{detail}; auth failed: {auth_error}"}
                return {"ready": True, "detail": detail}

            if probe.kind == "http":
                import httpx
                last_error: Optional[Exception] = None
                for target in (f"127.0.0.1:{port}", self._sibling_probe_target(service_name, container_port)):
                    if not target:
                        continue
                    try:
                        resp = httpx.get(f"http://{target}{probe.path}", timeout=3.0)
                    except Exception as e:
                        last_error = e
                        continue
                    detail = f"HTTP {resp.status_code}"
                    if not target.startswith("127.0.0.1"):
                        detail += f" via {target.split(':')[0]}"
                    return {"ready": resp.status_code == probe.expected_status, "detail": detail}
                raise last_error or RuntimeError("no reachable probe target")

            if probe.kind == "tcp":
                import socket
                last_error = None
                for target in (f"127.0.0.1:{port}", self._sibling_probe_target(service_name, container_port)):
                    if not target:
                        continue
                    target_host, _, target_port = target.partition(":")
                    try:
                        with socket.create_connection((target_host, int(target_port)), timeout=3.0):
                            return {"ready": True, "detail": f"port {target_port} accepting connections"}
                    except Exception as e:
                        last_error = e
                raise last_error or RuntimeError("no reachable probe target")
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
