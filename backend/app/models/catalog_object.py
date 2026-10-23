import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CatalogObject(Base):
    """One addressable data object, keyed by its fully-qualified name.

    This is the authoritative registry the architecture calls the Catalog seam.
    ``fqn`` is the identity — ``lakehouse.<layer>.<name>`` for objects Obelisk
    materialises, ``<connection>.<schema>.<name>`` for attached externals — and
    every other column is metadata about *where* and *how* it exists.

    Physical facts (``backend``, ``location``, ``capabilities_json``) are recorded,
    never inferred: a table that lives in Postgres and a Parquet file that lives on
    disk are the same kind of row with different capabilities, which is what lets
    the query layer stay format-agnostic.

    ``row_count``/``size_bytes`` are optional on purpose. Size is the product-wide
    metric (cheap, from metadata); row counts are only stored when a source can
    supply them exactly.
    """

    __tablename__ = "catalog_objects"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    fqn: Mapped[str] = mapped_column(String(512), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    layer_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)

    # table | view | model | parquet | shortcut | mirror
    kind: Mapped[str] = mapped_column(
        String(32), nullable=False, default="table", server_default="table", index=True
    )
    # local | s3 | gcs | azure | duckdb | postgres | mysql | mongo | sqlserver | oracle
    backend: Mapped[str] = mapped_column(
        String(32), nullable=False, default="local", server_default="local"
    )
    location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    connection_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)

    schema_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    capabilities_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    watermark_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    row_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    produced_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tags: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    docs: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=_utcnow)
    last_materialized_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
