"""
Orchestration Routes: Native Obelisk view of Dagster jobs, schedules, and runs.

These endpoints bridge the FastAPI API to the deployed Dagster webserver via
GraphQL. When Dagster is unreachable, endpoints return [] so the frontend can
render the offline card. The native Obelisk panel uses these instead of an
iframe, surfacing curated data with consistent Obelisk styling.

Why a separate router (rather than extending `routes_pipeline.py`):
- `routes_pipeline.py` is focused on the Ingest-event pipeline definitions
  plus the Obelisk Cron scheduler — different audience.
- Lets us add mirrors for other orchestration engines later without coupling.
"""
import threading
import time
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.engines.dagster_client import (
    DagsterServiceUnavailable,
    dagster_client,
)


router = APIRouter(prefix="/api/orchestration/dagster", tags=["orchestration"])


class JobItem(BaseModel):
    name: str
    description: str = ""


class ScheduleItem(BaseModel):
    id: str
    name: str
    cron: str
    timezone: str = "UTC"
    state: str  # "RUNNING" | "STOPPED" | "PAUSED"
    description: str = ""


class RunItem(BaseModel):
    id: str
    job: str
    status: str  # QUEUED, STARTED, SUCCESS, FAILURE, CANCELED, NOT_STARTED, ...
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    dagster_url: str


class LaunchResponse(BaseModel):
    run_id: str


_CACHE_TTL_SECONDS = 30.0
_cache_lock = threading.Lock()
_cache: Dict[str, tuple] = {}


def _cached(key: str, loader, *args, **kwargs):
    """TTL-cache a loader's result; only successful results are cached.

    The native panel hydrates from sessionStorage for up to 30s and revalidates
    on every mount. Mirroring that window here keeps tab-switch refreshes from
    re-running the same GraphQL queries, while a Dagster outage still returns []
    immediately (never cached) so recovery shows up on the next poll.
    """
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit is not None and (now - hit[0]) < _CACHE_TTL_SECONDS:
            return hit[1]
    rows = loader(*args, **kwargs)
    with _cache_lock:
        _cache[key] = (time.monotonic(), rows)
    return rows


def _safe_cached(key: str, loader, *args, **kwargs) -> List[Dict[str, Any]]:
    """Run a cached Dagster helper; on any failure return [] so the frontend
    fallback renders (failures are not cached)."""
    try:
        return _cached(key, loader, *args, **kwargs)
    except Exception:
        return []


@router.post("/cache/clear", include_in_schema=False)
def clear_dagster_cache():
    """Dev helper: clears the in-process Dagster response cache immediately."""
    with _cache_lock:
        _cache.clear()
    return {"cleared": True}


@router.get("/jobs", response_model=List[JobItem])
def get_dagster_jobs() -> List[JobItem]:
    rows = _safe_cached("jobs", dagster_client.list_jobs)
    return [JobItem(name=r.get("name", ""), description=r.get("description", "")) for r in rows]


@router.get("/schedules", response_model=List[ScheduleItem])
def get_dagster_schedules() -> List[ScheduleItem]:
    rows = _safe_cached("schedules", dagster_client.list_schedules)
    return [
        ScheduleItem(
            id=r.get("id", ""),
            name=r.get("name", ""),
            cron=r.get("cron", ""),
            timezone=r.get("timezone", "UTC"),
            # list_schedules() now returns the Dagster status string verbatim
            # ("RUNNING", "STOPPED", "PAUSED"). Default STOPPED (not RUNNING)
            # so a parse failure doesn't make a schedule look active.
            state=(r.get("state") or "STOPPED").upper(),
            description=r.get("description", ""),
        )
        for r in rows
    ]


@router.get("/runs", response_model=List[RunItem])
def get_dagster_runs(limit: int = Query(20, ge=1, le=100)) -> List[RunItem]:
    rows = _safe_cached(f"runs:{limit}", dagster_client.list_recent_runs, limit=limit)
    return [
        RunItem(
            id=r.get("id", ""),
            job=r.get("job", ""),
            status=r.get("status", "UNKNOWN"),
            started_at=r.get("started_at"),
            ended_at=r.get("ended_at"),
            duration_seconds=r.get("duration_seconds"),
            dagster_url=r.get("dagster_url", ""),
        )
        for r in rows
    ]


@router.post("/launch/{job_name}", response_model=LaunchResponse)
def launch_dagster_job(job_name: str) -> LaunchResponse:
    try:
        run_id = dagster_client.launch_job(job_name)
    except (DagsterServiceUnavailable, RuntimeError) as e:
        raise HTTPException(status_code=502, detail=f"Dagster service error: {e}")
    return LaunchResponse(run_id=run_id)
