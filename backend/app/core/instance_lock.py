"""One process owns one data directory.

Nothing underneath enforces this. SQLite tolerates another writer, DuckDB opens the same
Parquet files happily, and DuckLake will let a second connection commit to the same
catalog (proven in the format spike). Two backends pointed at one data directory
therefore corrupt state **quietly** — interleaved migrations, two schedulers running the
same pipeline, two catalog snapshots racing — instead of failing loudly.

The guard is an operating-system file lock held for the life of the process. The kernel
releases it when the process exits, including when it is killed, so a stale lock can
never block a restart. The lock file stays behind on purpose (it is empty and reusable);
the holder's details live in a sibling JSON file, and that is what a refused start
quotes back to the operator.
"""
from __future__ import annotations

import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

LOCK_FILENAME = "obelisk.instance.lock"
HOLDER_FILENAME = "obelisk.instance.json"

# Locks held by this process, keyed by resolved lock path. Holding them here means the
# claim survives the caller dropping its reference: a local variable going out of scope
# would otherwise close the file handle and silently release the lock, which is exactly
# the kind of quiet failure this module exists to prevent.
_HELD: dict[str, "InstanceLock"] = {}


class InstanceLockError(RuntimeError):
    """Raised when another process already holds this data directory."""

    def __init__(self, message: str, holder: Optional[dict] = None) -> None:
        super().__init__(message)
        self.holder = holder or {}


class InstanceLock:
    """A held-by-this-process claim on a data directory."""

    def __init__(
        self,
        directory: Path | str,
        *,
        database: Optional[str] = None,
        lakehouse: Optional[str] = None,
    ) -> None:
        self.directory = Path(directory)
        self.database = database
        self.lakehouse = lakehouse
        self.lock_path = self.directory / LOCK_FILENAME
        self.holder_path = self.directory / HOLDER_FILENAME
        self._handle = None

    def acquire(self) -> "InstanceLock":
        """Takes the lock or raises :class:`InstanceLockError`. Idempotent per instance."""
        if self._handle is not None:
            return self

        self.directory.mkdir(parents=True, exist_ok=True)
        handle = open(self.lock_path, "a+b")
        try:
            self._lock(handle)
        except OSError as exc:
            holder = self._read_holder()
            handle.close()
            raise InstanceLockError(self._conflict_message(holder), holder) from exc

        # One byte, written after the lock is held: byte-range locks cover it, and two
        # processes can never race to create it.
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        self._handle = handle
        _HELD[str(self.lock_path.resolve())] = self
        self._write_holder()
        return self

    def release(self) -> None:
        """Releases the lock and removes the holder note. Safe to call twice."""
        handle, self._handle = self._handle, None
        if handle is None:
            return
        _HELD.pop(str(self.lock_path.resolve()), None)
        try:
            self._unlock(handle)
        except OSError:
            pass
        finally:
            handle.close()
        try:
            self.holder_path.unlink()
        except OSError:
            pass  # informational only: the lock file is the truth

    def __enter__(self) -> "InstanceLock":
        return self.acquire()

    def __exit__(self, *exc_info) -> None:
        self.release()

    # -- platform locking --------------------------------------------------

    @staticmethod
    def _lock(handle) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(handle) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    # -- holder note -------------------------------------------------------

    def _write_holder(self) -> None:
        note = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "started_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "database": self.database,
            "lakehouse": self.lakehouse,
            "python": sys.version.split()[0],
        }
        try:
            self.holder_path.write_text(json.dumps(note, indent=2), encoding="utf-8")
        except OSError:
            pass  # a read-only data dir must not turn into a startup failure

    def _read_holder(self) -> dict:
        try:
            return json.loads(self.holder_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def _conflict_message(self, holder: dict) -> str:
        lines = [
            "Another Obelisk process is already using this data directory.",
            f"  data directory : {self.directory}",
            f"  lock file      : {self.lock_path}",
        ]
        if holder:
            lines.append(
                f"  holding pid    : {holder.get('pid')} "
                f"on {holder.get('host')} since {holder.get('started_at')}"
            )
            if holder.get("database"):
                lines.append(f"  database       : {holder['database']}")
            if holder.get("lakehouse"):
                lines.append(f"  lakehouse      : {holder['lakehouse']}")
        else:
            lines.append("  holding process: unknown (no holder note was left behind)")
        lines.append("Stop the other process, or point this one at a different data directory.")
        return "\n".join(lines)


def acquire_instance_lock(
    directory: Path | str,
    *,
    database: Optional[str] = None,
    lakehouse: Optional[str] = None,
) -> InstanceLock:
    """Convenience wrapper: build the lock and take it."""
    return InstanceLock(directory, database=database, lakehouse=lakehouse).acquire()


def release_instance_lock(directory: Path | str) -> None:
    """Releases a claim this process holds on ``directory``, if it holds one."""
    lock = _HELD.get(str((Path(directory) / LOCK_FILENAME).resolve()))
    if lock is not None:
        lock.release()

