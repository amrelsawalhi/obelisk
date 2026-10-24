"""Task history — the durable side of the live progress objects (M3, H10).

``IngestJobManager``, ``DbtRunner`` and ``DeployTracker`` keep their working state
in memory because it is read at polling frequency and streamed over SSE. Memory
alone loses every run on restart, and worse, leaves jobs that were mid-flight when
the process died looking forever "running".

One table holds all three producers. Their records have identical access patterns
(newest first by kind, fetch by id) and no relationships, so ``kind`` discriminates
instead of a table per producer, and a new producer (mirrors, syncs) needs no
migration. The payload is stored as JSON: these are display records, not relational
facts, and a producer must be able to add a field without a schema change.

Writes are synchronous and best-effort. Failing to record history must never fail
the ingest, dbt run or deploy it describes — but it is logged, not swallowed.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from app.config import settings

logger = logging.getLogger(__name__)

KINDS: tuple[str, ...] = ("ingest", "dbt", "deploy", "pipeline")


def _iso(value: Any) -> str:
    """Normalises a datetime/epoch/ISO value into a sortable ISO-8601 string."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    if isinstance(value, str) and value:
        return value
    return datetime.now(timezone.utc).isoformat()


class TaskHistory:
    """Read/write access to ``task_history`` (see the module docstring)."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path or settings.sqlite_db_path

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=15000;")
            yield conn
        finally:
            conn.close()

    def record(
        self,
        kind: str,
        task_id: str,
        status: str,
        started_at: Any,
        payload: dict,
    ) -> bool:
        """Upserts one task row. Returns False (and logs) on any failure."""
        if kind not in KINDS:
            raise ValueError(f"Unknown task kind {kind!r}; expected one of {KINDS}.")
        now = datetime.now(timezone.utc).isoformat()
        try:
            body = json.dumps(payload, default=str)
        except (TypeError, ValueError) as exc:
            logger.warning("task_history: %s/%s is not serialisable: %s", kind, task_id, exc)
            return False

        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO task_history (kind, id, status, started_at, updated_at, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(kind, id) DO UPDATE SET
                        status=excluded.status,
                        updated_at=excluded.updated_at,
                        payload_json=excluded.payload_json
                    """,
                    (kind, task_id, status, _iso(started_at), now, body),
                )
                conn.commit()
            return True
        except sqlite3.OperationalError as exc:
            # "database is locked" happens when the orchestrator, an ingest and a dbt
            # child all commit within the same instant. SQLite is single-writer, so a
            # short retry is the correct response — history is a display record, not a
            # transaction that must succeed on the first attempt.
            if "locked" in str(exc).lower():
                for delay in (0.25, 0.75, 1.5):
                    time.sleep(delay)
                    try:
                        with self._connect() as conn:
                            conn.execute(
                                """
                                INSERT INTO task_history
                                    (kind, id, status, started_at, updated_at, payload_json)
                                VALUES (?, ?, ?, ?, ?, ?)
                                ON CONFLICT(kind, id) DO UPDATE SET
                                    status=excluded.status,
                                    updated_at=excluded.updated_at,
                                    payload_json=excluded.payload_json
                                """,
                                (kind, task_id, status, _iso(started_at), now, body),
                            )
                            conn.commit()
                        return True
                    except sqlite3.Error:
                        continue
            logger.warning("task_history: could not record %s/%s: %s", kind, task_id, exc)
            return False
        except sqlite3.Error as exc:
            logger.warning("task_history: could not record %s/%s: %s", kind, task_id, exc)
            return False

    def fetch(self, kind: str, task_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM task_history WHERE kind = ? AND id = ?",
                (kind, task_id),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def recent(self, kind: str, limit: int = 20) -> list[dict]:
        """Newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload_json FROM task_history WHERE kind = ?"
                " ORDER BY started_at DESC LIMIT ?",
                (kind, limit),
            ).fetchall()
        result = []
        for (payload,) in rows:
            try:
                result.append(json.loads(payload))
            except (TypeError, ValueError):
                continue
        return result

    def mark_interrupted(self, kinds: Iterable[str] = KINDS) -> int:
        """Rewrites rows still marked running — their process is gone (H10)."""
        marked = 0
        for kind in kinds:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT id, payload_json FROM task_history"
                    " WHERE kind = ? AND status = 'running'",
                    (kind,),
                ).fetchall()

            for task_id, payload in rows:
                try:
                    body = json.loads(payload)
                except (TypeError, ValueError):
                    body = {}
                body["status"] = "interrupted"
                body["interrupted_at"] = datetime.now(timezone.utc).isoformat()
                if self.record(kind, task_id, "interrupted", body.get("started_at"), body):
                    marked += 1
        return marked

    def prune(self, keep_per_kind: int = 500) -> int:
        """Keeps the newest ``keep_per_kind`` rows per kind so the brain stays small."""
        removed = 0
        for kind in KINDS:
            with self._connect() as conn:
                cursor = conn.execute(
                    "DELETE FROM task_history WHERE kind = ? AND id NOT IN ("
                    "  SELECT id FROM task_history WHERE kind = ?"
                    "  ORDER BY started_at DESC LIMIT ?)",
                    (kind, kind, keep_per_kind),
                )
                conn.commit()
                removed += cursor.rowcount or 0
        return removed


task_history = TaskHistory()


def restore_task_history() -> dict[str, int]:
    """Runs at startup: close out interrupted runs, then reload recent history."""
    interrupted = task_history.mark_interrupted()
    pruned = task_history.prune()

    restored = 0
    try:
        from app.engines.ingest_job_manager import ingest_job_manager

        restored = ingest_job_manager.hydrate()
    except Exception as exc:  # a broken cache must not stop the API from starting
        logger.warning("task_history: could not restore ingest jobs: %s", exc)

    return {"interrupted": interrupted, "pruned": pruned, "restored": restored}
