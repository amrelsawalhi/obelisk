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

from app.core.dsn import bi_container_modules, bi_container_packages


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
    category: str = ""                            # functional role (Warehouse, Orchestration, BI & Analytics, ...)
    deployable: bool = True
    reason_not_deployable: Optional[str] = None
    volume_name: Optional[str] = None            # named volume — persistence across stop/redeploy
    volume_path: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)   # "__GENERATED__" = per-deploy vaulted secret
    command: Optional[Any] = None
    readiness: Optional[ReadinessProbe] = None
    probe_timeout_seconds: int = 90
    image_size_gb: float = 1.0                   # rough pull footprint for disk pre-flight
    # What this service *is*, in platform terms: the Connection.source_type it provides
    # once it is ready. Declared once here, and the deploy path registers the Connection
    # from it — no per-service boolean to remember and no second way to describe a source.
    provides: Optional[str] = None
    default_connection_name: Optional[str] = None # SQL identifier used when registering Connection (e.g. obelisk_postgres)
    web_ui: bool = False                         # primary host port serves a browsable web UI
    web_ui_path: Optional[str] = None             # relative gateway subpath (e.g. "/gitea") served by Caddy / Vite proxy
    post_start_commands: List[str] = field(default_factory=list)  # exec'd after container start (init flows)
    restart_after_init: bool = False             # triggers container restart after first boot & init to load drivers cleanly
    credential_username: Optional[str] = None     # username shown/registered for generated credentials
    credential_database: Optional[str] = None     # database shown/registered for generated credentials
    build_dockerfile: Optional[str] = None       # local image build instead of registry pull
    bind_mounts: Dict[str, Dict[str, str]] = field(default_factory=dict)  # host rel path -> {"bind": container_path, "mode": ...}
    pass_env: List[str] = field(default_factory=list)  # settings attributes injected as env at deploy
    run_mode: str = "container"                  # "container" | "process" (host-managed process)
    process_commands: List[List[str]] = field(default_factory=list)  # argv lists started by core/process_manager
    process_cwd: Optional[str] = None            # working dir for process commands (defaults to project root)


_BLUEPRINTS: List[ServiceBlueprint] = [
    ServiceBlueprint(
        name="postgres",
        display_name="PostgreSQL",
        description="Relational warehouse for dbt marts and staging.",
        category="Warehouse",
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
        provides="postgres",
        default_connection_name="obelisk_postgres",
    ),
    ServiceBlueprint(
        name="clickhouse",
        display_name="ClickHouse OLAP",
        description="Columnar OLAP engine for real-time analytics.",
        category="Analytics",
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
        description="Kafka-compatible event streaming engine.",
        category="Streaming",
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
        description="Self-hosted Git service.",
        category="Version Control",
        image="gitea/gitea:1.22",
        ports={"3000/tcp": 3001, "22/tcp": 2222},
        primary_container_port="3000/tcp",
        primary_host_port=3001,
        volume_name="obelisk_gitea_data",
        volume_path="/data",
        env={
            "GITEA__server__HTTP_PORT": "3000",
            # Gitea has no relative ROOT_URL mode; the deploy route expands
            # {public_url} from the browser origin so subpath links work in dev
            # (:5173) and behind Caddy (:80) alike.
            # NOTE: Gitea mounts its routes at "/" regardless of ROOT_URL, so the
            # gateway (Caddy handle_path / Vite rewrite) must strip /gitea. This
            # value only controls the links Gitea generates.
            "GITEA__server__ROOT_URL": "{public_url}/gitea/",
            "GITEA__server__SSH_PORT": "2222",
            "GITEA__server__SSH_LISTEN_PORT": "22",
            "GITEA__server__START_SSH_SERVER": "true",
            "GITEA__server__LFS_START_SERVER": "false",
        },
        # Probed directly against the published port, so the path must be the one
        # Gitea actually serves: "/api/healthz" (not the /gitea-prefixed variant).
        readiness=ReadinessProbe(kind="http", host_port=3001, path="/api/healthz", expected_status=200),
        probe_timeout_seconds=90,
        image_size_gb=1.0,
        web_ui=True,
        web_ui_path="/gitea",
    ),
    ServiceBlueprint(
        name="superset",
        display_name="Apache Superset",
        description="BI dashboards and SQL analytics.",
        category="BI & Analytics",
        image="apache/superset:4.1.1",
        ports={"8088/tcp": 8088},
        primary_container_port="8088/tcp",
        primary_host_port=8088,
        volume_name="obelisk_superset_data",
        volume_path="/app/superset_home",
        # ``pylibs`` lives on the data volume and is on PYTHONPATH so the drivers installed
        # below are importable by gunicorn: a directory only becomes an import location if
        # it exists when the interpreter starts, and a fresh volume has no ``pylibs`` yet.
        env={
            "SUPERSET_SECRET_KEY": "__GENERATED__",
            "PYTHONPATH": "/app/pythonpath:/app/superset_home/pylibs",
        },
        # Superset's image ships no database drivers, and it resolves (and caches) an engine
        # spec the first time it is asked for one — at boot. Furthermore, starting Gunicorn
        # before database migrations and role initialization run causes workers to boot against
        # an empty schema and answer every datasource create with 422 until restarted.
        # Everything — driver installation, schema migrations, admin creation, and permission
        # init — runs sequentially ahead of run-server.sh. The import guard keeps an
        # already-provisioned volume from paying for pip again on restarts.
        command=[
            "/bin/sh",
            "-c",
            'python -c "import ' + ", ".join(bi_container_modules()) + '" 2>/dev/null || '
            "pip install --no-cache-dir --no-deps --target /app/superset_home/pylibs "
            + " ".join(bi_container_packages())
            + "; "
            "superset db upgrade && "
            '(superset fab create-admin --username "$SUPERSET_ADMIN_USER" --firstname Admin --lastname User --email admin@obelisk.local --password "$SUPERSET_ADMIN_PASSWORD" || true) && '
            "superset init && "
            "exec /usr/bin/run-server.sh",
        ],
        readiness=ReadinessProbe(kind="http", host_port=8088, path="/health", expected_status=200),
        probe_timeout_seconds=300,
        image_size_gb=3.0,
        credential_username="admin",
        web_ui=True,
        # No ``web_ui_path``: Superset is served at the root of its own port, and the
        # Dashboards page links to it there. The gateway used to carve out a ``/superset``
        # subpath, but Superset was never told to serve one (it needs
        # ``SUPERSET_WEBSERVER_BASEURL`` *and* ``APPLICATION_ROOT``, and its own asset
        # paths assume the root), so ``/superset`` answered a Flask 404 while every
        # dashboard link and cookie stayed wrong. Pointing the browser at the real address
        # is the whole fix: one origin, no rewrite, no base path to keep in sync.
        web_ui_path=None,
        post_start_commands=[],
        restart_after_init=True,
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
            "category": bp.category,
            "image": bp.image,
            "deployable": bp.deployable,
            "reason_not_deployable": bp.reason_not_deployable,
            "default_host_port": bp.primary_host_port,
            "port_map": bp.ports,
            "volume_name": bp.volume_name,
            "probe_timeout_seconds": bp.probe_timeout_seconds,
            "recommended_min_disk_gb": bp.image_size_gb,
            "needs_credentials": "__GENERATED__" in bp.env.values(),
            "default_username": bp.credential_username,
            "default_database": bp.credential_database,
            "provides": bp.provides,
            "web_ui": bp.web_ui,
            "web_ui_path": bp.web_ui_path,
            "post_start": bool(bp.post_start_commands),
            "run_mode": bp.run_mode,
        }
        for bp in SERVICE_BLUEPRINTS.values()
    ]