"""Warehouse storage — the one module that touches the warehouse directory.

The warehouse is a directory tree of Parquet files: a table is a file, its folder is
its schema, and nothing else records what exists. Everything above this module speaks
in **relative POSIX paths** (``supabase/auth/table1.parquet``), so what is underneath
stays an implementation detail.

Only the local filesystem is implemented today. The interface exists anyway, in the
shape a bucket needs — list a prefix, open one object, stat one key, write one object
— because ``STORAGE_URI`` already names a directory and the next backend should be a
new class rather than an audit of every caller. A bucket changes exactly three things,
and this is the seam that absorbs them:

* there is no atomic rename, so ``write_atomic`` becomes a single-object PUT (which is
  atomic when it completes) instead of write-then-``os.replace``;
* ``list_parquet`` is a paginated ``LIST`` and ``stat`` is a ``HEAD`` per object, so a
  scan costs a round trip per file and callers must cache;
* ``location`` returns an ``s3://`` URI that DuckDB reads through ``httpfs``, rather
  than a local path.

Nothing here knows about dbt, the catalog or the API.
"""
from __future__ import annotations

import errno
import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Protocol


logger = logging.getLogger(__name__)

_FSYNC_UNSUPPORTED = {errno.EOPNOTSUPP, errno.EINVAL, errno.ENOSYS}
_FSYNC_WARNING_LOCK = threading.Lock()
_FSYNC_WARNING_EMITTED = False


def _is_unsupported_fsync_error(exc: OSError) -> bool:
    """Whether a filesystem says it cannot fsync this file (rather than failing I/O)."""
    return getattr(exc, "errno", None) in _FSYNC_UNSUPPORTED


def _fsync_best_effort(handle: BinaryIO) -> None:
    """fsync when the filesystem supports it; warn once when it explicitly does not.

    Atomic rename still guarantees readers never see a partial file. On a virtual
    filesystem without fsync passthrough, refusing the write would make the whole
    platform unusable; silently ignoring every other OSError would hide real disk
    failures. Only the explicit "operation not supported" family is tolerated.
    """
    global _FSYNC_WARNING_EMITTED
    try:
        os.fsync(handle.fileno())
    except OSError as exc:
        if not _is_unsupported_fsync_error(exc):
            raise
        with _FSYNC_WARNING_LOCK:
            if not _FSYNC_WARNING_EMITTED:
                logger.warning(
                    "This filesystem does not support fsync (%s). Atomic writes still "
                    "prevent partial files, but power-loss durability depends on the "
                    "underlying storage. Use a local POSIX filesystem for production data.",
                    exc,
                )
                _FSYNC_WARNING_EMITTED = True


class InvalidPath(ValueError):
    """Raised when a relative warehouse path could escape the root."""


@dataclass(frozen=True)
class FileInfo:
    """One file in the warehouse, described by the storage layer."""

    path: str  # relative, POSIX, e.g. "supabase/auth/table1.parquet"
    size_bytes: int
    modified_at: datetime


def normalize_rel(path: str) -> str:
    """Returns ``path`` as a clean relative POSIX path, or raises :class:`InvalidPath`.

    The warehouse root is the boundary: a table path that climbs out of it, or that
    arrives absolute, is a bug in the caller rather than a table that lives elsewhere.
    """
    if path is None:
        raise InvalidPath("Warehouse path is empty.")
    raw = str(path).strip().replace("\\", "/")
    if not raw:
        raise InvalidPath("Warehouse path is empty.")
    if raw.startswith("/") or "://" in raw or (len(raw) > 1 and raw[1] == ":"):
        raise InvalidPath(f"Warehouse path {raw!r} must be relative to the warehouse root.")
    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if any(part == ".." for part in parts):
        raise InvalidPath(f"Warehouse path {raw!r} escapes the warehouse root.")
    if not parts:
        raise InvalidPath(f"Warehouse path {raw!r} names no file.")
    return "/".join(parts)


class WarehouseStorage(Protocol):
    """The six operations the warehouse needs. A bucket backend implements these."""

    def list_parquet(self) -> Iterable[FileInfo]: ...

    def list_directories(self) -> Iterable[str]: ...

    def open_binary(self, rel: str) -> BinaryIO: ...

    def stat(self, rel: str) -> FileInfo: ...

    def exists(self, rel: str) -> bool: ...

    def write_atomic(self, rel: str, data: bytes) -> None: ...

    def delete(self, rel: str) -> None: ...



class LocalWarehouse:
    """The warehouse as a directory on this machine."""

    def __init__(self, root: Path | str):
        self._root = Path(root).expanduser().resolve()

    @property
    def root(self) -> Path:
        return self._root

    def display_root(self) -> str:
        return self._root.as_posix()

    def _resolve(self, rel: str) -> Path:
        candidate = (self._root / normalize_rel(rel)).resolve()
        if candidate != self._root and self._root not in candidate.parents:
            raise InvalidPath(f"Warehouse path {rel!r} escapes the warehouse root.")
        return candidate

    def list_parquet(self) -> list[FileInfo]:
        """Every ``*.parquet`` under the root, skipping dot-directories and dot-files.

        ``.gitkeep``, editor swap files and half-written temporaries live under dot
        names on purpose, so the walk simply ignores them: a table is a visible file.
        """
        if not self._root.is_dir():
            return []
        found: list[FileInfo] = []
        for absolute in self._root.rglob("*.parquet"):
            relative = absolute.relative_to(self._root)
            if any(part.startswith(".") for part in relative.parts):
                continue
            if not absolute.is_file():
                continue
            found.append(self._info(relative.as_posix(), absolute))
        found.sort(key=lambda item: item.path)
        return found

    def list_directories(self) -> list[str]:
        """Every folder under the root, relative and POSIX, skipping dot-folders.

        A folder is not an object that gets registered anywhere — it is where files
        are — but the UI still has to show empty ones, so the walk lists what the
        filesystem holds rather than inferring folders from the files.
        """
        if not self._root.is_dir():
            return []
        found: list[str] = []
        for current, subdirs, _ in os.walk(self._root):
            subdirs[:] = sorted(name for name in subdirs if not name.startswith("."))
            current_path = Path(current)
            if current_path == self._root:
                continue
            found.append(current_path.relative_to(self._root).as_posix())
        found.sort(key=str.lower)
        return found

    def _info(self, rel: str, absolute: Path | None = None) -> FileInfo:
        target = absolute or self._resolve(rel)
        stat = target.stat()
        return FileInfo(
            path=normalize_rel(rel),
            size_bytes=stat.st_size,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
        )

    def open_binary(self, rel: str) -> BinaryIO:
        return self._resolve(rel).open("rb")

    def stat(self, rel: str) -> FileInfo:
        return self._info(rel)

    def exists(self, rel: str) -> bool:
        return self._resolve(rel).is_file()

    def write_atomic(self, rel: str, data: bytes) -> None:
        """Writes one file so a reader never sees a partial table.

        The bytes land in a dot-prefixed temporary in the same directory and are then
        renamed over the target, which is atomic on a POSIX filesystem and on NTFS.
        The dot name keeps the temporary out of :meth:`list_parquet` while it is being
        written and lets a failed write clean up after itself.
        """
        target = self._resolve(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.parent / f".{target.name}.{uuid.uuid4().hex[:8]}.tmp"
        try:
            with temporary.open("wb") as handle:
                handle.write(data)
                handle.flush()
                _fsync_best_effort(handle)
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink(missing_ok=True)

    def delete(self, rel: str) -> None:
        target = self._resolve(rel)
        if target.exists():
            target.unlink()
        # A folder exists because a file exists, so the ones this delete emptied go with
        # it. The walk stops at the warehouse root and never removes a folder that still
        # holds something, so a user-created folder with no data is left to the user.
        parent = target.parent
        while parent != self._root and self._root in parent.parents:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    def location(self, rel: str) -> str:
        """The string DuckDB should read: a POSIX path today, a URI for a bucket."""
        return self._resolve(rel).as_posix()


def prune_empty_dirs(
    root: Path | str,
    start: Path | str | None = None,
    *,
    dry_run: bool = False,
) -> int:
    """Removes the directories under ``root`` that hold neither a file nor a directory.

    A lakehouse table is not a directory — the catalog decides what exists — but DuckLake
    still derives a data directory from the table's schema (``<root>/<schema>/<table>``),
    and dropping a table or reclaiming the files of an expired snapshot leaves that
    directory behind, empty. Nothing in the catalog points at it, yet a walk of the
    warehouse reports it as a folder, which is how a dropped table kept appearing in the
    tree.

    Two kinds of directory are deliberately kept:

    * one holding a ``.gitkeep`` — that marker is how a user says "I made this folder on
      purpose", so an empty folder with one is a folder rather than a leftover;
    * a dot-named directory anywhere in the path — the platform's own bookkeeping, which
      is never a folder a user created.

    ``start`` bounds the walk to one subtree, which is what a delete or an ingest needs:
    the directory it just emptied, not the whole warehouse. Without it the whole tree is
    swept, which is what maintenance does. ``dry_run`` reports the count and removes
    nothing.

    The walk is bottom-up so a directory whose children were all removed is itself removed
    in the same pass. Returns how many directories were removed (or would be).
    """
    base = Path(root).expanduser().resolve()
    target = Path(start).expanduser().resolve() if start is not None else base
    if not target.is_dir() or (target != base and base not in target.parents):
        return 0

    removed: set[Path] = set()
    for current, subdirs, files in os.walk(target, topdown=False):
        directory = Path(current)
        if directory == base:
            continue
        relative = directory.relative_to(base)
        if any(part.startswith(".") for part in relative.parts):
            continue
        if ".gitkeep" in files:
            continue
        # A child that survived this walk still holds something, so neither does this go.
        if any((directory / name) not in removed for name in subdirs):
            continue
        if not dry_run:
            try:
                directory.rmdir()
            except OSError:
                continue
        removed.add(directory)
    return len(removed)


_storage: LocalWarehouse | None = None
_storage_root: str | None = None


def warehouse_storage() -> LocalWarehouse:
    """The configured warehouse. Built lazily so importing this module creates nothing.

    Rebuilt when the configured root changes rather than cached forever: the root is
    configuration (a test points it at a scratch directory, an operator repoints
    ``STORAGE_URI``), and a stale handle would read the previous warehouse.
    """
    global _storage, _storage_root
    from app.config import settings

    root = settings.lakehouse_root_path
    if _storage is None or _storage_root != str(root):
        _storage = LocalWarehouse(root)
        _storage_root = str(root)
    return _storage
