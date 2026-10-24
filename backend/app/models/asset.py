import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import String, Integer, Text, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class Asset(Base):
    """Read-only projection of ``catalog_objects`` under the original column names.

    `catalog_objects` is the table that knows what exists; ``assets`` is a view the
    migration leaves behind so older callers (and the UI) can keep reading
    ``layer``/``storage_uri``/``record_count``. Do not write through this mapping.
    """

    __tablename__ = "assets"

    # Uniqueness is per tier: `bronze.orders` and `silver.orders` are two distinct
    # assets and must both be catalogued. A unique index on `name` alone made them
    # collide, so the key is now (layer, name) — see the migration in database.py.
    __table_args__ = (
        UniqueConstraint("layer", "name", name="uq_assets_layer_name"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    layer: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, default="default") # optional tier/folder name (bronze, silver, gold, raw, landing, ...)
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(String(50), default="parquet")
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    schema_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_materialized_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
