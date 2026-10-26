import os
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

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

# Shipped defaults, named once so "is this still the default?" is a comparison rather than
# a string literal repeated in the guard, the login route and the docs.
DEFAULT_MASTER_KEY = "dGhpcy1pcy1hLXNhbXBsZS0zMi1ieXRlLWtleS0xMjM0NTY="
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASSWORD = "obelisk2026"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
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
        in ("1", "true", "yes", "on")
    )
    # Extra browser origins allowed to open sockets, comma separated. Same-host,
    # loopback and Vite's dev server are always allowed; this is for a reverse proxy
    # that rewrites the Host header to something else.
    ALLOWED_ORIGINS: str = Field(
        default_factory=lambda: os.getenv("OBELISK_ALLOWED_ORIGINS", "")
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
        """
        return Path(self.lakehouse_root_path) / ".obelisk" / "catalog.sqlite"

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

    # Docker network and host
    OBELISK_NET: str = "obelisk_net"
    DOCKER_HOST: str | None = Field(default_factory=lambda: os.getenv("DOCKER_HOST", None))

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
        normalised = raw.replace("\\", "/").rstrip("/")
        if normalised == CONTAINER_LAKEHOUSE_PATH or normalised.startswith(CONTAINER_LAKEHOUSE_PATH + "/"):
            if not Path(CONTAINER_LAKEHOUSE_PATH).exists():
                suffix = normalised[len(CONTAINER_LAKEHOUSE_PATH):].strip("/")
                raw = str(project_root / "data" / "lakehouse")
                if suffix:
                    raw = str(Path(raw) / suffix)

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

