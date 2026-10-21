import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import String, Integer, Text, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)
    layer: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, default="default") # optional tier/folder name (bronze, silver, gold, raw, landing, ...)
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(String(50), default="parquet")
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    schema_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_materialized_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
