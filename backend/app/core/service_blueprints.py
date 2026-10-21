"""
Service Blueprints — declarative deployment specifications for Obelisk-managed services.

Adding a new service = adding one ServiceBlueprint entry below; no deploy-logic changes.

Hard rules enforced by this module's consumers:
- All deployable images are pinned (no :latest) and overridable per-deploy.
- Every deployable data service gets a named Docker volume. Stop keeps the volume;
  only the explicit destroy endpoint (DockerManager.destroy_service) may delete it.
- A service is "ready" only when its readiness probe passes — a running container
  is not proof the service inside accepts work.
- docker_manager never handles secrets: the routes layer generates credentials,
  injects them via env, and vaults them (see routes_services.deploy_local_service).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ReadinessProbe:
    kind: str                     # "pg_isready" | "http" | "tcp"
    host_port: int                # host-side port probed (after port binding)
    path: str = "/"              # http only
    expected_status: int = 200   # http only
    user: Optional[str] = None    # pg_isready only
    db: Optional[str] = None     # pg_isready only


@dataclass
class ServiceBlueprint:
    name: str
    display_name: str
    description: str
    image: str                                   # pinned image:tag (overridable per-deploy)
    ports: Dict[str, int]                        # container port ("5432/tcp") -> default host port
    primary_container_port: str                  # container port whose host binding is canonical
    primary_host_port: int
    deployable: bool = True
    reason_not_deployable: Optional[str] = None
    volume_name: Optional[str] = None            # named volume — persistence across stop/redeploy
    volume_path: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)   # "__GENERATED__" = per-deploy vaulted secret
    command: Optional[Any] = None
    readiness: Optional[ReadinessProbe] = None
    probe_timeout_seconds: int = 90
    image_size_gb: float = 1.0                   # rough pull footprint for disk pre-flight
    register_connection: bool = False            # auto-register as catalog Connection when ready
    connection_type: Optional[str] = None        # Connection.source_type for registration
    web_ui: bool = False                         # primary host port serves a browsable web UI
    post_start_commands: List[str] = field(default_factory=list)  # exec'd after container start (init flows)
    credential_username: Optional[str] = None     # username shown/registered for generated credentials
    credential_database: Optional[str] = None     # database shown/registered for generated credentials


_BLUEPRINTS: List[ServiceBlueprint] = [
    ServiceBlueprint(
        name="postgres",
        display_name="PostgreSQL 16",
        description="Relational warehouse for dbt marts and pipeline staging. Password is generated per-deploy, vaulted, and auto-registered as a catalog Connection.",
        image="postgres:16-alpine",
        ports={"5432/tcp": 5432},
        primary_container_port="5432/tcp",
        primary_host_port=5432,
        volume_name="obelisk_postgres_data",
        volume_path="/var/lib/postgresql/data",
        env={"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "__GENERATED__", "POSTGRES_DB": "obelisk"},
        readiness=ReadinessProbe(kind="pg_isready", host_port=5432, user="postgres", db="obelisk"),
        probe_timeout_seconds=60,
        image_size_gb=1.0,
        credential_username="postgres",
        credential_database="obelisk",
        register_connection=True,
        connection_type="postgres",
    ),
    ServiceBlueprint(
        name="clickhouse",
        display_name="ClickHouse OLAP",
        description="Columnar OLAP engine for high-speed aggregation analytics. Data persists in a named volume.",
        image="clickhouse/clickhouse-server:24.8",
        ports={"8123/tcp": 8123, "9000/tcp": 9000},
        primary_container_port="8123/tcp",
        primary_host_port=8123,
        volume_name="obelisk_clickhouse_data",
        volume_path="/var/lib/clickhouse",
        readiness=ReadinessProbe(kind="http", host_port=8123, path="/ping", expected_status=200),
        probe_timeout_seconds=90,
        image_size_gb=2.5,
    ),
    ServiceBlueprint(
        name="redpanda",
        display_name="Redpanda Streaming",
        description="Kafka-compatible event streaming engine for CDC and event pipelines.",
        image="docker.redpanda.com/redpandadata/redpanda:v24.1.1",
        ports={"9092/tcp": 9092, "9644/tcp": 9644},
        primary_container_port="9092/tcp",
        primary_host_port=9092,
        volume_name="obelisk_redpanda_data",
        volume_path="/var/lib/redpanda/data",
        command="redpanda start --mode dev-container --smp 1 --memory 1G",
        readiness=ReadinessProbe(kind="tcp", host_port=9092),
        probe_timeout_seconds=60,
        image_size_gb=1.0,
    ),
    ServiceBlueprint(
        name="gitea",
        display_name="Gitea Git Server",
        description="Self-hosted Git service for versioning notebooks, models, and pipelines.",
        image="gitea/gitea:1.22",
        ports={"3000/tcp": 3001, "22/tcp": 2222},
        primary_container_port="3000/tcp",
        primary_host_port=3001,
        volume_name="obelisk_gitea_data",
        volume_path="/data",
        readiness=ReadinessProbe(kind="http", host_port=3001, path="/api/healthz", expected_status=200),
        probe_timeout_seconds=90,
        image_size_gb=1.0,
        web_ui=True,
    ),
    ServiceBlueprint(
        name="dagster",
        display_name="Dagster Engine",
        description="Asset orchestrator. Local deployment is pending Workstream 5 (workspace mount wiring); connect an existing remote Dagster instance meanwhile.",
        image="dagster/dagster-k8s:latest",
        ports={"3000/tcp": 3000},
        primary_container_port="3000/tcp",
        primary_host_port=3000,
        deployable=False,
        reason_not_deployable="Local deployment pending Workstream 5: the container needs a workspace/DAG mount to serve anything useful.",
        command="dagster-webserver -h 0.0.0.0 -p 3000 --path-prefix /dagster",
        readiness=ReadinessProbe(kind="http", host_port=3000, path="/", expected_status=200),
        web_ui=True,
    ),
    ServiceBlueprint(
        name="superset",
        display_name="Apache Superset",
        description="BI dashboards with SQL Lab. Deploy runs the full init flow (secret key, DB upgrade, admin creation); generated admin credentials are vaulted and shown once.",
        image="apache/superset:4.1.1",
        ports={"8088/tcp": 8088},
        primary_container_port="8088/tcp",
        primary_host_port=8088,
        volume_name="obelisk_superset_data",
        volume_path="/app/superset_home",
        env={"SUPERSET_SECRET_KEY": "__GENERATED__", "SUPERSET_WEBSERVER_BASEURL": "/superset/", "APPLICATION_ROOT": "/superset"},
        readiness=ReadinessProbe(kind="http", host_port=8088, path="/health", expected_status=200),
        probe_timeout_seconds=300,
        image_size_gb=3.0,
        credential_username="admin",
        web_ui=True,
        post_start_commands=[
            "superset db upgrade",
            'superset fab create-admin --username admin --firstname Admin --lastname User --email admin@obelisk.local --password "$SUPERSET_SECRET_KEY" || true',
            "superset init",
        ],
    ),
]

SERVICE_BLUEPRINTS: Dict[str, ServiceBlueprint] = {bp.name: bp for bp in _BLUEPRINTS}


def get_blueprint(service_name: str) -> Optional[ServiceBlueprint]:
    return SERVICE_BLUEPRINTS.get((service_name or "").lower())


def list_blueprints() -> List[ServiceBlueprint]:
    return list(SERVICE_BLUEPRINTS.values())


def blueprint_catalog_entries() -> List[Dict[str, Any]]:
    """Public catalog metadata consumed by the frontend pre-deploy configuration form."""
    return [
        {
            "name": bp.name,
            "display_name": bp.display_name,
            "description": bp.description,
            "image": bp.image,
            "deployable": bp.deployable,
            "reason_not_deployable": bp.reason_not_deployable,
            "default_host_port": bp.primary_host_port,
            "port_map": bp.ports,
            "volume_name": bp.volume_name,
            "probe_timeout_seconds": bp.probe_timeout_seconds,
            "recommended_min_disk_gb": bp.image_size_gb,
            "needs_credentials": "__GENERATED__" in bp.env.values(),
            "register_connection": bp.register_connection,
            "web_ui": bp.web_ui,
            "post_start": bool(bp.post_start_commands),
        }
        for bp in SERVICE_BLUEPRINTS.values()
    ]