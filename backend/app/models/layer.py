from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Layer(Base):
    """A node in the user-defined layer tree.

    The tree is a materialised path: ``path`` is the canonical identity
    (``marts.sales``) and ``parent_path`` is the denormalised edge, so listing one
    level is a single indexed query and moving a subtree is one ``UPDATE ... WHERE
    path LIKE 'old.%'`` — no recursive CTEs, no adjacency-list walks.

    Nothing in the schema names a medallion tier. A workspace that wants
    bronze/silver/gold creates those three layers; one that wants a flat tree
    keeps the seeded ``raw``.
    """

    __tablename__ = "layers"

    path: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    parent_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    color: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    icon: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow, onupdate=_utcnow
    )

    @property
    def depth(self) -> int:
        """1 for a root layer, 2 for its children, and so on."""
        return self.path.count(".") + 1
