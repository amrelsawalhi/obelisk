"""Live query handles, by id, so a running statement can be stopped.

Query Studio had a timeout and nothing else: the HTTP request owned a DuckDB connection
inside a worker thread, and the only way to end a runaway query was to wait for the
timeout — or for the process. Registering each in-flight connection under the id the
client chose turns "stop" into one call to ``interrupt()``/``cancel()``.

Deliberately in-process: a query is owned by the worker that started it, and there is no
second worker to reach it. That is the same scope the timeout already had.
"""
from __future__ import annotations

import threading
from typing import Any, Dict


def _stop_statement(conn: Any) -> bool:
    """Stops the statement this connection is running.

    DuckDB answers ``interrupt()``; psycopg2 answers ``cancel()``. Both are safe to call
    from another thread — that is their whole purpose — and both can fail benignly when the
    statement finished a moment before the call.

    pymysql offers neither, so a MySQL query is reported as not stoppable rather than
    pretending the click worked.
    """
    for method in ("interrupt", "cancel"):
        stop = getattr(conn, method, None)
        if callable(stop):
            try:
                stop()
                return True
            except Exception:
                return False
    return False


class QueryRegistry:
    """The connections currently executing, keyed by the client's ``query_id``."""

    def __init__(self) -> None:
        self._live: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def register(self, query_id: str, conn: Any) -> None:
        if not query_id:
            return
        with self._lock:
            self._live[query_id] = conn

    def release(self, query_id: str) -> None:
        if not query_id:
            return
        with self._lock:
            self._live.pop(query_id, None)

    def is_running(self, query_id: str) -> bool:
        with self._lock:
            return query_id in self._live

    def cancel(self, query_id: str) -> str:
        """``"cancelled"``, ``"not_stoppable"`` or ``"not_running"``.

        Three answers rather than a bool: "the click worked", "the engine cannot be
        stopped from here" and "it was already finished" mean different things to a user
        watching a spinner.
        """
        with self._lock:
            conn = self._live.get(query_id)
        if conn is None:
            return "not_running"
        return "cancelled" if _stop_statement(conn) else "not_stoppable"


query_registry = QueryRegistry()
