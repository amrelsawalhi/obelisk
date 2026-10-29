import logging
import os
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices

# Schemes that are recognised but cannot be served until the StorageProvider
# seam lands (docs/architecture.md §7.1). They are rejected at startup instead
# of being silently rewritten onto local disk.
REMOTE_STORAGE_SCHEMES = frozenset(
    {"s3", "gs", "gcs", "abfs", "abfss", "az", "azure", "http", "https"}
)
LOCAL_STORAGE_SCHEMES = frozenset({"", "file"})

# Default path baked into the Docker image. It is remapped into the checkout
# only when the host genuinely has no /data/lakehouse directory, so a real
# bind mount at that path still wins.
CONTAINER_LAKEHOUSE_PATH = "/data/lakehouse"

# Where the lakehouse belongs in the image: the target of the ``./data:/app/data``
# bind mount docker-compose.yml declares. Remapping the container default onto
# "/data/lakehouse" *inside* the image looked correct and was not — the image root is
# "/", so the repo candidate resolved to the very path being avoided, ``mkdir`` happily
# created it in the container's writable layer, and every table written there was lost
# the next time the container was recreated.
APP_DATA_LAKEHOUSE_PATH = "/app/data/lakehouse"

logger = logging.getLogger(__name__)

# A silent remap is how the bug above survived: the process wrote somewhere plausible
# and nothing said so. This warns once per process, on the first path resolution.
_lakehouse_remap_reported = False


def resolve_lakehouse_directory(
    raw: str, project_root: Path, app_data_available: bool | None = None
) -> str:
    """Maps the un-URI'd container default onto a directory that outlives the container.

    Two hosts share this code and they cannot use the same answer:

    * a checkout keeps its lakehouse beside the code, ``<repo>/data/lakehouse``;
    * the image's project root is ``/``, so that same candidate *is*
      ``/data/lakehouse`` — the container path being escaped. There the only
      persistent directory is the ``./data:/app/data`` bind mount.

    ``app_data_available`` exists so the image case is a testable fact rather than a
    filesystem probe; ``None`` means "look". Returns ``raw`` unchanged when it is not
    the container default, or when something really is mounted at that path.
    """
    normalised = raw.replace("\\", "/").rstrip("/")
    is_container_default = normalised == CONTAINER_LAKEHOUSE_PATH or normalised.startswith(
        CONTAINER_LAKEHOUSE_PATH + "/"
    )
    if not is_container_default or Path(CONTAINER_LAKEHOUSE_PATH).exists():
        return raw

    suffix = normalised[len(CONTAINER_LAKEHOUSE_PATH):].strip("/")
    checkout = Path(os.path.normpath(str(project_root / "data" / "lakehouse")))
    if app_data_available is None:
        app_data_available = Path(APP_DATA_LAKEHOUSE_PATH).parent.is_dir()

    if str(checkout).replace("\\", "/") != CONTAINER_LAKEHOUSE_PATH:
        chosen = checkout
    elif app_data_available:
        chosen = Path(APP_DATA_LAKEHOUSE_PATH)
    else:
        chosen = checkout
    # POSIX form: this answer is about a container path, and returning a Windows
    # `\app\data\lakehouse` for it would be a lie that only tests could notice.
    chosen_path = chosen / suffix if suffix else chosen
    return chosen_path.as_posix()

# Shipped defaults, named once so "is this still the default?" is a comparison rather than
# a string literal repeated in the guard, the login route and the docs.
DEFAULT_MASTER_KEY = "dGhpcy1pcy1hLXNhbXBsZS0zMi1ieXRlLWtleS0xMjM0NTY="
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASSWORD = "obelisk2026"

_ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(str(_ROOT_ENV), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    APP_NAME: str = "Obelisk Control Plane"
    ENVIRONMENT: str = "development"
    DEBUG: bool = False
    
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # Master encryption key for credentials vault (Fernet key)
    OBELISK_MASTER_KEY: str = Field(
        default_factory=lambda: os.getenv("OBELISK_MASTER_KEY", DEFAULT_MASTER_KEY)
    )

    # Admin credentials for the session login. The field names match the environment
    # variable names on purpose: `SettingsConfigDict(env_file=".env")` then populates
    # them, so a value set only in `.env` is actually used. Reading `os.environ`
    # directly meant `.env` was ignored and the shipped default silently won.
    OBELISK_ADMIN_USER: str = DEFAULT_ADMIN_USER
    OBELISK_ADMIN_PASSWORD: str = DEFAULT_ADMIN_PASSWORD

    # ---------------------------------------------------------------- auth
    # Enforcement is off until a real user store exists. The plumbing already asks
    # "who is calling" (app/core/security.py), so turning this on gates every /api
    # route and every socket without editing a single handler.
    AUTH_ENABLED: bool = Field(
        default_factory=lambda: os.getenv("OBELISK_AUTH_ENABLED", "0").strip().lower()
        in ("1", "true", "yes", "on"),
        validation_alias=AliasChoices("AUTH_ENABLED", "OBELISK_AUTH_ENABLED"),
    )
    # Session cookie behaviour. ``Secure`` stays off by default because a LAN HTTP
    # deployment is a supported install; anything served over HTTPS should set it to 1
    # so the cookie never travels in clear text.
    SESSION_SECURE_COOKIE: bool = Field(
        default_factory=lambda: os.getenv("OBELISK_SESSION_SECURE_COOKIE", "0")
        .strip()
        .lower()
        in ("1", "true", "yes", "on"),
        validation_alias=AliasChoices("SESSION_SECURE_COOKIE", "OBELISK_SESSION_SECURE_COOKIE"),
    )
    # Session lifetime. A stolen cookie is valid until it expires, so keep this as
    # short as the workflow tolerates.
    SESSION_TTL_SECONDS: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_SESSION_TTL_SECONDS", str(7 * 86400))),
        validation_alias=AliasChoices("SESSION_TTL_SECONDS", "OBELISK_SESSION_TTL_SECONDS"),
    )
    # Login brute-force protection: after this many failures from one address, that
    # address is locked out for AUTH_LOCKOUT_SECONDS.
    AUTH_MAX_FAILED_ATTEMPTS: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_AUTH_MAX_FAILED_ATTEMPTS", "10")),
        validation_alias=AliasChoices("AUTH_MAX_FAILED_ATTEMPTS", "OBELISK_AUTH_MAX_FAILED_ATTEMPTS"),
    )
    AUTH_LOCKOUT_SECONDS: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_AUTH_LOCKOUT_SECONDS", "300")),
        validation_alias=AliasChoices("AUTH_LOCKOUT_SECONDS", "OBELISK_AUTH_LOCKOUT_SECONDS"),
    )
    # Allows testing auth locally with sample master key and default password without startup abort.
    ALLOW_DEFAULT_CREDENTIALS: bool = Field(
        default_factory=lambda: os.getenv("OBELISK_ALLOW_DEFAULT_CREDENTIALS", "0").strip().lower()
        in ("1", "true", "yes", "on"),
        validation_alias=AliasChoices("ALLOW_DEFAULT_CREDENTIALS", "OBELISK_ALLOW_DEFAULT_CREDENTIALS"),
    )
    # Extra browser origins allowed to open sockets, comma separated. Same-host,
    # loopback and Vite's dev server are always allowed; this is for a reverse proxy
    # that rewrites the Host header to something else.
    ALLOWED_ORIGINS: str = Field(
        default_factory=lambda: os.getenv("OBELISK_ALLOWED_ORIGINS", "")
    )
    # Comma-separated IPs/CIDRs of reverse proxies whose X-Forwarded-For may be
    # believed. Empty means no proxy is trusted: a forwarded header is refused. Set
    # this when the API sits behind Caddy/nginx on another host and the terminal is
    # used without authentication (with auth on, sessions do the gating instead).
    TRUSTED_PROXIES: str = Field(
        default_factory=lambda: os.getenv("OBELISK_TRUSTED_PROXIES", "")
    )

    # ------------------------------------------------------------ terminal
    # The in-app terminal spawns a real shell as this process's user. It stays on for
    # local use; a deployment that would rather not hand that out turns it off with
    # OBELISK_TERMINAL=0 instead of editing code.
    TERMINAL_ENABLED: bool = Field(
        default_factory=lambda: os.getenv("OBELISK_TERMINAL", "1").strip().lower()
        not in ("0", "false", "no", "off")
    )
    TERMINAL_MAX_SESSIONS: int = 4
    TERMINAL_IDLE_TIMEOUT_SECONDS: int = 900

    @staticmethod
    def _default_database_url() -> str:
        env_url = os.getenv("DATABASE_URL")
        if env_url:
            return env_url
        project_root = Path(__file__).resolve().parent.parent.parent
        db_path = (project_root / "data" / "obelisk.db").resolve().as_posix()
        return f"sqlite+aiosqlite:///{db_path}"

    # Database URL for internal SQLite brain (aiosqlite)
    DATABASE_URL: str = Field(
        default_factory=lambda: Settings._default_database_url()
    )

    @property
    def sqlite_db_path(self) -> str:
        """Resolves the canonical absolute filesystem path for the SQLite database.

        Subprocesses (pipeline steps, dbt project) run with different working
        directories, so a relative DATABASE_URL would resolve to different files
        (split-brain). Always returns an absolute POSIX path, normalized for
        3-slash relative URIs, 4-slash absolute URIs, and Windows drive letters.
        """
        raw = self.DATABASE_URL
        for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break
        # Normalize the leading double slash of a 4-slash URI (sqlite+aiosqlite:////app/...)
        if raw.startswith("//") and not raw.startswith("///"):
            raw = raw[1:]
        # Normalize a Windows drive letter (/C:/path -> C:/path)
        if re.match(r"^/[a-zA-Z]:", raw):
            raw = raw[1:]
        p = Path(raw)
        if not p.is_absolute():
            project_root = Path(__file__).resolve().parent.parent.parent
            p = (project_root / p).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p.as_posix()

    # Zero-Cost Storage URI Abstraction
    STORAGE_URI: str = Field(
        default_factory=lambda: os.getenv("STORAGE_URI", "file:///data/lakehouse")
    )

    # DuckDB session budget (Rule 5 of Rules_for_scalability.md). DuckDB spills to
    # `temp_directory` when a query exceeds `memory_limit`; without a temp directory it
    # raises Out of Memory instead of using the disk it was given. Spill space is capped
    # so a runaway query cannot eat the volume, and threads are capped so concurrent
    # queries do not each try to use every core.
    DUCKDB_THREADS: int | None = Field(
        default_factory=lambda: int(os.getenv("DUCKDB_THREADS", "0")) or None
    )
    # DuckDB's community repository hosts the *mongo* scanner; installing from it runs
    # unsigned code downloaded at startup (core scanners — postgres, mysql, sqlite, odbc —
    # are signed and unaffected). Off means a MongoDB source stays pull-only, and nothing
    # is downloaded.
    ALLOW_COMMUNITY_EXTENSIONS: bool = Field(
        default_factory=lambda: os.getenv(
            "OBELISK_ALLOW_COMMUNITY_EXTENSIONS", "true"
        ).lower()
        in ("1", "true", "yes", "on")
    )
    DUCKDB_TEMP_SIZE_LIMIT: str = Field(
        default_factory=lambda: os.getenv("DUCKDB_TEMP_SIZE_LIMIT", "50GB")
    )
    # Interactive JSON query responses are capped: `SELECT *` on a 200 GB table must
    # never turn into millions of Python dicts. The cap is a *display* limit — exports
    # stream the full result to disk instead.
    DUCKDB_MAX_QUERY_ROWS: int = Field(
        default_factory=lambda: int(os.getenv("DUCKDB_MAX_QUERY_ROWS", "10000"))
    )
    # How many DuckDB connections may be doing heavy work at once. The per-slot memory
    # limit is derived from this so concurrent queries cannot together exceed the host.
    DUCKDB_MAX_CONCURRENT_HEAVY: int = Field(
        default_factory=lambda: int(os.getenv("DUCKDB_MAX_CONCURRENT_HEAVY", "4"))
    )
    # How many ingests may run at once. Each one holds a DuckDB connection and a heavy
    # slot for its whole run, so this must stay below DUCKDB_MAX_CONCURRENT_HEAVY or the
    # ingest queue simply waits on the slots instead of on this cap.
    INGEST_MAX_CONCURRENT: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_INGEST_MAX_CONCURRENT", "2"))
    )

    # The SDK's eager readers (``to_polars``/``to_df``/``to_arrow``) pull a whole table into
    # memory. Past a point that is not a slow notebook cell, it is the kernel dying with
    # every other result in it. The guard warns at the first threshold and refuses at the
    # second; 0 for the refusal means "derive it from the host" (max of 8 GiB and a quarter
    # of RAM). Sizes are measured from the Parquet files on disk, so they are a lower bound
    # on what the dataframe will occupy.
    SDK_EAGER_WARN_BYTES: int = Field(
        default_factory=lambda: int(
            os.getenv("OBELISK_SDK_EAGER_WARN_BYTES", str(2 * 1024 ** 3))
        )
    )
    SDK_EAGER_REFUSE_BYTES: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_SDK_EAGER_REFUSE_BYTES", "0"))
    )
    # The most rows ``obelisk.query()`` / ``query_arrow()`` will materialise. They build
    # the whole result in memory; past the cap the SDK raises and points at
    # ``iter_batches()``/``lazy_query()`` instead of taking the kernel with it. 0 removes
    # the cap for an operator who knows what they are asking for.
    SDK_QUERY_ROW_LIMIT: int = Field(
        default_factory=lambda: int(os.getenv("OBELISK_SDK_QUERY_ROW_LIMIT", "1000000"))
    )

    @property
    def lakehouse_catalog_path(self) -> Path:
        """DuckLake's metadata database, kept with the data it describes.

        DuckLake records the data path inside its catalog and refuses to attach a catalog
        to a different one, which is the right safety check — so the two must travel
        together: move, copy or restore the lakehouse folder and its metadata comes with
        it. It also means a second warehouse (a test's scratch directory, a second
        install) gets its own catalog instead of fighting over one.

        The dot-folder keeps it out of the table namespace: the warehouse walk skips
        dot-directories, so the catalog is never mistaken for a table.

        The directory is created here, not only in :mod:`app.core.catalog`. A dbt run
        attaches the same catalog straight from its profile, without going through
        ``attach_lakehouse``, so on a warehouse whose ``.obelisk/`` did not exist yet
        DuckDB answered ``unable to open database file`` — a fresh install could not
        compile a single model until something else happened to create the folder.
        """
        catalog = Path(self.lakehouse_root_path) / ".obelisk" / "catalog.sqlite"
        catalog.parent.mkdir(parents=True, exist_ok=True)
        return catalog

    @property
    def duckdb_temp_path(self) -> Path:
        """Local scratch space for DuckDB's spills. Always local, never network storage."""
        target = Path(self.sqlite_db_path).parent / "duckdb_tmp"
        target.mkdir(parents=True, exist_ok=True)
        return target

    # Marimo notebook kernel expiry: seconds a notebook's kernel survives after its
    # browser tab disconnects. In `marimo edit` mode Marimo disables this by default,
    # so every notebook ever opened leaves a kernel (~155 MB) resident until the
    # backend restarts. 0 disables the expiry (pre-change behaviour).
    MARIMO_SESSION_TTL_SECONDS: int = 600

    # Where the notebook server binds. Marimo's own default is loopback, and it is kept
    # explicit here so a deployment cannot drift into exposing an unauthenticated
    # ``edit`` server (which is remote code execution) just by publishing a port. A
    # deployment that needs another container to reach it must publish it through a
    # proxy that authenticates first.
    MARIMO_HOST: str = Field(
        default_factory=lambda: os.getenv("MARIMO_HOST", "127.0.0.1").strip() or "127.0.0.1"
    )

    # Docker network and host
    OBELISK_NET: str = "obelisk_net"
    DOCKER_HOST: str | None = Field(default_factory=lambda: os.getenv("DOCKER_HOST", None))
    OBELISK_SERVICE_BIND_HOST: str = Field(
        default_factory=lambda: os.getenv("OBELISK_SERVICE_BIND_HOST", "127.0.0.1")
    )

    # Optional remote Spark cluster URL (e.g., sc://...)
    SPARK_REMOTE_URL: str | None = Field(default_factory=lambda: os.getenv("SPARK_REMOTE_URL", None))

    @property
    def is_sqlite(self) -> bool:
        """True when the brain is a SQLite file (the supported control-plane store)."""
        return self.DATABASE_URL.startswith(("sqlite+aiosqlite:///", "sqlite:///"))

    @property
    def storage_scheme(self) -> str:
        """Lower-cased scheme of STORAGE_URI ('file' when the URI has none)."""
        return (urlparse(self.STORAGE_URI).scheme or "file").lower()

    def validate_storage_uri(self) -> None:
        """Fails fast on a STORAGE_URI this build cannot actually serve.

        Object-store writes are not implemented yet (see the StorageProvider
        seam). Previously an s3:// URI was silently rewritten onto local disk
        while s3:// paths were recorded in the catalog, so ingestion reported
        success while nothing reached the bucket.
        """
        scheme = self.storage_scheme
        if scheme in REMOTE_STORAGE_SCHEMES:
            raise RuntimeError(
                f"STORAGE_URI={self.STORAGE_URI!r} uses the '{scheme}://' scheme, which is not "
                "implemented yet. Obelisk currently writes to the local filesystem only. "
                "Use a file:// URI or an absolute directory path."
            )
        if scheme not in LOCAL_STORAGE_SCHEMES:
            raise RuntimeError(
                f"STORAGE_URI={self.STORAGE_URI!r} uses an unsupported scheme '{scheme}://'. "
                "Use a file:// URI or an absolute directory path."
            )

    @property
    def lakehouse_root_path(self) -> Path:
        """Absolute local directory that backs the lakehouse.

        Resolution is strict: the scheme decides what happens and nothing is
        guessed from substrings. A remote scheme raises instead of quietly
        pointing at local disk, and the container default
        ``file:///data/lakehouse`` is remapped into the checkout only when the
        host has no such directory.
        """
        self.validate_storage_uri()
        project_root = Path(__file__).resolve().parent.parent.parent
        parsed = urlparse(self.STORAGE_URI)

        if parsed.scheme.lower() == "file":
            if parsed.netloc and parsed.netloc.lower() not in ("", "localhost"):
                raw = f"//{parsed.netloc}{unquote(parsed.path)}"  # UNC share
            else:
                raw = unquote(parsed.path)
                # Windows drive letter: /C:/data -> C:/data
                if re.match(r"^/[a-zA-Z]:", raw):
                    raw = raw[1:]
        else:
            raw = self.STORAGE_URI.strip()

        if ".." in Path(raw).parts:
            raise RuntimeError(
                f"STORAGE_URI={self.STORAGE_URI!r} contains a '..' segment; the lakehouse "
                "root must be an explicit, stable directory."
            )

        # Container default -> checkout, but only when the host has no such
        # directory (a real bind mount must win).
        global _lakehouse_remap_reported
        remapped = resolve_lakehouse_directory(raw, project_root)
        if remapped != raw:
            raw = remapped
            if not _lakehouse_remap_reported:
                _lakehouse_remap_reported = True
                logger.warning(
                    "STORAGE_URI=%s is the container default and %s does not exist; "
                    "using %s instead. Set STORAGE_URI explicitly to silence this.",
                    self.STORAGE_URI,
                    CONTAINER_LAKEHOUSE_PATH,
                    raw,
                )

        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = project_root / candidate
        candidate = Path(os.path.normpath(str(candidate)))
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate.resolve()

    @property
    def duckdb_storage_path(self) -> str:
        """POSIX string form of :attr:`lakehouse_root_path` for DuckDB."""
        return self.lakehouse_root_path.as_posix()

    @property
    def git_repo_path(self) -> Path:
        """
        Resolves the root git_repo directory on the local filesystem.
        """
        project_root = Path(__file__).resolve().parent.parent.parent
        target = project_root / "git_repo"
        target.mkdir(parents=True, exist_ok=True)
        return target

settings = Settings()

