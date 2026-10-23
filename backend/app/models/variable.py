"""
Variable Model: Persistent Key-Value store for environment configuration and AES-128 encrypted secrets.
Usable across Notebooks (via obelisk SDK), dbt pipelines, and ingestion jobs.
"""

import uuid
from sqlalchemy import Column, String, Boolean, Text, DateTime
from sqlalchemy.sql import func
from app.models.base import Base


class Variable(Base):
    __tablename__ = "variables"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    key = Column(String, unique=True, index=True, nullable=False)
    value = Column(Text, nullable=False)
    is_secret = Column(Boolean, default=False, nullable=False)
    description = Column(String, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
