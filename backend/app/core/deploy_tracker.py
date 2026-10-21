"""
Deploy Progress Tracker:
In-memory, per-service progress records for 1-click local deployments.

The deploy endpoint remains synchronous (callers await the final result), but the
tracker makes every phase observable while it runs: pre-flight -> pulling (with
layer-level pull %) -> creating -> initializing (post-start commands) -> probing
-> ready/failed. The frontend polls GET /api/services/{name}/deploy-progress while
its POST is in flight — no background jobs, no queue, no WebSocket plumbing.
Workstream 4 (job queue) will generalize this pattern into the persistent jobs table.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DeployProgress:
    service_name: str
    phase: str = "queued"  # queued|pre-flight|pulling|creating|initializing|probing|ready|starting|failed
    detail: str = ""
    pull_percent: float = 0.0
    logs: List[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service_name": self.service_name,
            "phase": self.phase,
            "detail": self.detail,
            "pull_percent": round(self.pull_percent, 1),
            "logs": self.logs[-10:],
            "active": self.finished_at is None,
            "error": self.error,
        }


class DeployTracker:
    """Registry of the latest deploy per service (GIL-protected dict updates)."""

    def __init__(self) -> None:
        self._jobs: Dict[str, DeployProgress] = {}

    def start(self, service_name: str) -> DeployProgress:
        job = DeployProgress(service_name=service_name, phase="pre-flight")
        self._jobs[service_name] = job
        return job

    def update(self, service_name: str, phase: Optional[str] = None,
               detail: Optional[str] = None, pull_percent: Optional[float] = None,
               log: Optional[str] = None) -> None:
        job = self._jobs.get(service_name)
        if job is None:
            return
        if phase is not None:
            job.phase = phase
        if detail is not None:
            job.detail = detail
        if pull_percent is not None:
            job.pull_percent = pull_percent
        if log is not None:
            job.logs.append(log)

    def finish(self, service_name: str, phase: str, detail: str = "") -> None:
        job = self._jobs.get(service_name)
        if job is None:
            return
        job.phase = phase
        job.detail = detail
        job.finished_at = time.time()

    def fail(self, service_name: str, error: str) -> None:
        job = self._jobs.get(service_name)
        if job is None:
            return
        job.phase = "failed"
        job.error = error
        job.finished_at = time.time()

    def get(self, service_name: str) -> Optional[DeployProgress]:
        return self._jobs.get(service_name)


deploy_tracker = DeployTracker()
