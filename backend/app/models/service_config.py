import uuid
from datetime import datetime, timezone
from sqlalchemy import String, Integer, Text, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class ServiceConfig(Base):
    __tablename__ = "service_configs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    service_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True) # postgres, superset, clickhouse, redpanda, gitea
    deployment_type: Mapped[str] = mapped_column(String(20), nullable=False, default="local") # "local" or "remote"
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="stopped") # stopped, running, connected, error
    container_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    host: Mapped[str] = mapped_column(String(255), nullable=False, default="localhost")
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    allocated_memory_mb: Mapped[int] = mapped_column(Integer, nullable=False, default=1024)
    config_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=lambda: datetime.now(timezone.utc), 
        onupdate=lambda: datetime.now(timezone.utc)
    )
