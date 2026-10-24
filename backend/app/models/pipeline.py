"""Pipeline orchestration models — the durable state of the built-in scheduler.

The orchestrator is the only one in the product (cron and Dagster are gone). Every
fact needed to resume after a crash lives in these tables: the graph the operator
drew (``pipeline_steps`` / ``pipeline_edges``), the schedule anchor (``pipelines``
``next_run_at``), and the execution record (``pipeline_runs`` / ``step_runs``).

Status vocabularies are plain strings on purpose — SQLite has no enums, and the API
validates them at the boundary. Terminal step states are ``success``, ``failed``,
``skipped``, ``cancelled`` and ``interrupted``; a step is finished exactly when it
holds one of those.
"""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False, default="0 2 * * *")
    timezone: Mapped[str] = mapped_column(String(60), nullable=False, default="UTC")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # skip: record the occurrence and carry on; queue: start once the active run ends;
    # kill: cancel the active run and start the new one.
    overlap_policy: Mapped[str] = mapped_column(String(20), nullable=False, default="skip")

    max_parallel: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    keep_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=3600)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    auto_rerun_interrupted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    next_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    last_status: Mapped[str] = mapped_column(String(30), nullable=False, default="idle")
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)


class PipelineParam(Base):
    """A typed parameter defined once on the pipeline and resolved per run."""

    __tablename__ = "pipeline_params"
    __table_args__ = (UniqueConstraint("pipeline_id", "name", name="uq_pipeline_param"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    type: Mapped[str] = mapped_column(String(20), nullable=False, default="string")
    default_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class PipelineStep(Base):
    __tablename__ = "pipeline_steps"
    __table_args__ = (UniqueConstraint("pipeline_id", "name", name="uq_pipeline_step_name"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    # dbt_run | dbt_test | script | notebook | webhook | raw_sql
    step_type: Mapped[str] = mapped_column(String(20), nullable=False, default="dbt_run")
    target: Mapped[str] = mapped_column(Text, nullable=False, default="")
    args_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    timeout_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_retries: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    retry_delay_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    position_x: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    position_y: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class PipelineEdge(Base):
    __tablename__ = "pipeline_edges"
    __table_args__ = (UniqueConstraint("from_step", "to_step", name="uq_pipeline_edge"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True
    )
    from_step: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipeline_steps.id", ondelete="CASCADE"), nullable=False
    )
    to_step: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipeline_steps.id", ondelete="CASCADE"), nullable=False
    )
    # success | failure | completion | skipped
    condition: Mapped[str] = mapped_column(String(20), nullable=False, default="success")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False, default="manual")
    triggered_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    params_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # pending | waiting | running | success | failed | cancelled | interrupted | skipped
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending", index=True)
    scheduled_for: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class StepRun(Base):
    __tablename__ = "step_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # pending | waiting_deps | running | success | failed | skipped | cancelled | interrupted
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending", index=True)
    # claimed_at is written by the atomic claim UPDATE; started_at only after the child
    # actually spawned. A row with claimed_at and no started_at never touched the
    # outside world, so boot healing may safely re-queue it.
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    logs: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

