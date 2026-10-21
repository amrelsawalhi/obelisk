import os
import re
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

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
        default_factory=lambda: os.getenv(
            "OBELISK_MASTER_KEY", 
            "dGhpcy1pcy1hLXNhbXBsZS0zMi1ieXRlLWtleS0xMjM0NTY=" # fallback valid 32-byte b64 key
        )
    )

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
    
    # Zero-Cost Storage URI Abstraction
    STORAGE_URI: str = Field(
        default_factory=lambda: os.getenv("STORAGE_URI", "file:///data/lakehouse")
    )

    # Docker network and host
    OBELISK_NET: str = "obelisk_net"
    DOCKER_HOST: str | None = Field(default_factory=lambda: os.getenv("DOCKER_HOST", None))

    # Optional remote Spark cluster URL (e.g., sc://...)
    SPARK_REMOTE_URL: str | None = Field(default_factory=lambda: os.getenv("SPARK_REMOTE_URL", None))

    @property
    def duckdb_storage_path(self) -> str:
        """
        DuckDB's read_parquet() and COPY do not accept the file:// URI scheme for local files.
        Resolves local file:// URIs to the project's actual data/lakehouse directory on disk
        (creating it if missing) and converts all Windows backslashes to forward slashes (.as_posix()),
        while keeping s3://..., abfss://..., etc. intact.
        """
        uri = self.STORAGE_URI
        if uri.startswith("file://"):
            stripped = uri[len("file://"):]
            # Normalize drive letters on Windows: /C:/path -> C:/path
            if re.match(r"^/[a-zA-Z]:", stripped):
                raw_path = stripped[1:]
            else:
                raw_path = stripped

            clean_raw = raw_path.rstrip("/\\").replace("\\", "/")
            project_root = Path(__file__).resolve().parent.parent.parent
            if clean_raw in ("/data/lakehouse", "data/lakehouse") or clean_raw.endswith("data/lakehouse"):
                target_dir = project_root / "data" / "lakehouse"
            elif "data/lakehouse/" in clean_raw:
                # Retain whatever sub-tier directory was configured under data/lakehouse
                # (e.g. bronze, silver, raw, landing, analytics, ...) instead of forcing
                # a hardcoded medallion structure.
                sub_tier = clean_raw.split("data/lakehouse/")[-1]
                target_dir = project_root / "data" / "lakehouse" / sub_tier
            else:
                target_dir = Path(raw_path)

            target_dir.mkdir(parents=True, exist_ok=True)
            return target_dir.resolve().as_posix()
        return uri

    @property
    def lakehouse_root_path(self) -> Path:
        """
        Resolves the root data/lakehouse directory on the local filesystem.
        """
        path_str = self.duckdb_storage_path
        if path_str.startswith(("s3://", "gs://", "abfss://", "http://", "https://")):
            project_root = Path(__file__).resolve().parent.parent.parent
            target = project_root / "data" / "lakehouse"
            target.mkdir(parents=True, exist_ok=True)
            return target
        target = Path(path_str)
        target.mkdir(parents=True, exist_ok=True)
        return target

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

