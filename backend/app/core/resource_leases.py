"""Cross-process resource leases for DuckDB heavy work.

The backend's semaphore only sees work inside the API process. Pipeline scripts,
notebooks and SDK calls run in separate Python processes, so their reservations are
published as small JSON lease files under the lakehouse metadata directory. Telemetry
aggregates live leases and ignores files left behind by dead processes.
"""
from __future__ import annotations

import json
import logging
import os
import psutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

LEASE_DIR = Path(".obelisk") / "resource_leases"


def lease_directory() -> Path:
    from app.config import settings

    path = Path(settings.lakehouse_root_path) / LEASE_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def _process_identity() -> tuple[int, float]:
    pid = os.getpid()
    try:
        started = float(psutil.Process(pid).create_time())
    except Exception:
        started = 0.0
    return pid, started


def register_resource_lease(
    *,
    kind: str,
    slots: int,
    memory_limit_mb: Optional[int] = None,
    temp_limit_mb: Optional[int] = None,
    temp_path: Optional[str] = None,
    detail: Optional[str] = None,
) -> Optional[str]:
    """Publishes one live reservation; returns its lease id, or None on failure."""
    lease_id = uuid.uuid4().hex
    pid, process_started = _process_identity()
    payload = {
        "id": lease_id,
        "pid": pid,
        "process_started_at": process_started,
        "kind": str(kind or "query"),
        "slots": max(1, int(slots)),
        "memory_limit_mb": int(memory_limit_mb) if memory_limit_mb else None,
        "temp_limit_mb": int(temp_limit_mb) if temp_limit_mb else None,
        "temp_path": str(temp_path) if temp_path else None,
        "detail": detail,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        root = lease_directory()
        target = root / f"{lease_id}.json"
        scratch = target.with_name(f".{target.name}.{pid}.tmp")
        scratch.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(scratch, target)
        return lease_id
    except Exception as exc:
        logger.debug("Could not publish resource lease %s: %s", kind, exc)
        return None


def release_resource_lease(lease_id: Optional[str]) -> None:
    if not lease_id:
        return
    try:
        (lease_directory() / f"{lease_id}.json").unlink(missing_ok=True)
    except Exception as exc:
        logger.debug("Could not release resource lease %s: %s", lease_id, exc)


def _lease_is_alive(payload: dict[str, Any]) -> bool:
    try:
        pid = int(payload.get("pid"))
    except (TypeError, ValueError):
        return False
    if not psutil.pid_exists(pid):
        return False
    expected = float(payload.get("process_started_at") or 0.0)
    if expected <= 0:
        return True
    try:
        actual = float(psutil.Process(pid).create_time())
    except psutil.AccessDenied:
        # We cannot verify reuse, but the PID is alive; do not discard its work.
        return True
    except Exception:
        return False
    # Guard against PID reuse. Filesystem precision can vary by platform.
    return abs(actual - expected) <= 1.5


def list_resource_leases() -> list[dict[str, Any]]:
    """Returns every live lease, removing stale files from dead processes."""
    try:
        root = lease_directory()
    except Exception:
        return []
    leases: list[dict[str, Any]] = []
    for path in root.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if _lease_is_alive(payload):
            leases.append(payload)
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    leases.sort(key=lambda item: str(item.get("started_at") or ""))
    return leases


def leases_by_kind(leases: list[dict[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for lease in leases:
        kind = str(lease.get("kind") or "unknown")
        totals[kind] = totals.get(kind, 0) + max(1, int(lease.get("slots") or 1))
    return totals
