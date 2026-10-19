import os
import subprocess
from typing import Dict, Any, List, Optional
import docker
from docker.models.containers import Container
from docker.errors import NotFound, DockerException
from app.config import settings
from app.core.resource_advisor import advisor

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
    ) -> Container:
        """
        Deploys or recreates a managed service container on localhost.
        Applies dynamic resource limits from ResourceAdvisor, attaches to obelisk_net,
        and enforces managed_by=obelisk label.
        """
        self.ensure_network()
        name = self._get_container_name(service_name)

        # Check existing container
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

        # Service configuration blueprints
        env = environment or {}
        port_bindings = ports or {}
        img = image
        cmd = command

        svc_lower = service_name.lower()
        if svc_lower == "postgres":
            img = img or "postgres:16-alpine"
            env.setdefault("POSTGRES_USER", "obelisk")
            env.setdefault("POSTGRES_PASSWORD", "obelisk")
            env.setdefault("POSTGRES_DB", "obelisk")
            port_bindings.setdefault("5432/tcp", ("0.0.0.0", 5432))
        elif svc_lower == "dagster":
            img = img or "dagster/dagster-k8s:latest"
            # Explicit subpath prefix flag for reverse proxy routing
            cmd = cmd or "dagster-webserver -h 0.0.0.0 -p 3000 --path-prefix /dagster"
            port_bindings.setdefault("3000/tcp", ("0.0.0.0", 3000))
        elif svc_lower == "superset":
            img = img or "apache/superset:latest"
            # Explicit subpath configuration for reverse proxy routing
            env.setdefault("SUPERSET_WEBSERVER_BASEURL", "/superset/")
            env.setdefault("APPLICATION_ROOT", "/superset")
            port_bindings.setdefault("8088/tcp", ("0.0.0.0", 8088))
        elif svc_lower == "clickhouse":
            img = img or "clickhouse/clickhouse-server:latest"
            port_bindings.setdefault("8123/tcp", ("0.0.0.0", 8123))
            port_bindings.setdefault("9000/tcp", ("0.0.0.0", 9000))
        elif svc_lower == "redpanda":
            img = img or "docker.redpanda.com/redpandadata/redpanda:latest"
            cmd = cmd or "redpanda start --mode dev-container --smp 1 --memory 1G"
            port_bindings.setdefault("9092/tcp", ("0.0.0.0", 9092))
            port_bindings.setdefault("9644/tcp", ("0.0.0.0", 9644))
        elif svc_lower == "gitea":
            img = img or "gitea/gitea:latest"
            # Gitea internal container port 3000/tcp MUST map to host port 3001
            port_bindings.setdefault("3000/tcp", ("0.0.0.0", 3001))
            port_bindings.setdefault("22/tcp", ("0.0.0.0", 2222))
        else:
            # Generic fallback or test service (e.g. alpine)
            img = img or "alpine:latest"
            cmd = cmd or ["tail", "-f", "/dev/null"]

        labels = {
            MANAGED_LABEL: OBELISK_TAG,
            "service": svc_lower,
        }

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
            volumes=volumes or {},
            detach=True,
            restart_policy={"Name": "unless-stopped"},
        )

        return container

    def stop_service(self, service_name: str) -> None:
        """
        Stops and removes a managed container.
        Enforces safety: never stops containers that lack managed_by=obelisk.
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
            container.remove(v=True, force=True)

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
