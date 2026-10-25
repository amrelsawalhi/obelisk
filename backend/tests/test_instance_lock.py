"""The single-instance guard: one process per data directory.

Why this exists: nothing below the app enforces it. SQLite accepts another writer,
DuckDB reads the same Parquet, and DuckLake let a second connection commit to the same
catalog during the format spike. These tests pin the behaviour the app relies on when it
takes the claim at startup.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.core.instance_lock import (
    HOLDER_FILENAME,
    LOCK_FILENAME,
    InstanceLock,
    InstanceLockError,
    acquire_instance_lock,
    release_instance_lock,
)


def test_second_acquire_is_refused_and_names_the_holder(tmp_path):
    first = acquire_instance_lock(tmp_path, database="obelisk.db", lakehouse="lake")
    try:
        with pytest.raises(InstanceLockError) as excinfo:
            InstanceLock(tmp_path).acquire()
    finally:
        first.release()

    message = str(excinfo.value)
    assert str(os.getpid()) in message, "the refusal must name the holding pid"
    assert "obelisk.db" in message, "the refusal must name the database in use"
    assert excinfo.value.holder["pid"] == os.getpid()


def test_release_allows_the_next_process_to_start(tmp_path):
    first = acquire_instance_lock(tmp_path)
    first.release()

    second = acquire_instance_lock(tmp_path)
    second.release()


def test_different_data_directories_do_not_conflict(tmp_path):
    one = acquire_instance_lock(tmp_path / "instance-one")
    try:
        two = acquire_instance_lock(tmp_path / "instance-two")
        try:
            assert one.lock_path != two.lock_path
        finally:
            two.release()
    finally:
        one.release()


def test_holder_note_records_identity_and_clears_on_release(tmp_path):
    lock = acquire_instance_lock(tmp_path, database="db.sqlite", lakehouse="lake")
    assert lock.lock_path.name == LOCK_FILENAME

    note = json.loads(lock.holder_path.read_text(encoding="utf-8"))
    assert note["pid"] == os.getpid()
    assert note["database"] == "db.sqlite"
    assert note["lakehouse"] == "lake"

    lock.release()
    assert not lock.holder_path.exists(), "a clean shutdown leaves no stale holder note"
    assert (tmp_path / HOLDER_FILENAME).exists() is False


def test_lock_survives_the_caller_dropping_its_reference(tmp_path):
    """The claim belongs to the process, not to a variable.

    Dropping the returned object used to close the file handle and release the lock
    without a word — the failure this module exists to prevent, arriving silently.
    """
    acquire_instance_lock(tmp_path)  # deliberately not assigned to anything
    try:
        with pytest.raises(InstanceLockError):
            InstanceLock(tmp_path).acquire()
    finally:
        release_instance_lock(tmp_path)

    next_holder = InstanceLock(tmp_path).acquire()
    next_holder.release()


def test_app_startup_takes_the_claim(tmp_path):
    """The real lifespan claims the data directory on boot and drops it on shutdown.

    Run in a child process on purpose: the app's database engine is bound to whatever
    event loop first used it, and booting it inside pytest's loop drags that coupling
    into the suite. The child also proves the ordinary case a user hits — start, work,
    stop — rather than the crash case covered below.
    """
    backend_dir = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "DATABASE_URL": f"sqlite+aiosqlite:///{(tmp_path / 'obelisk.db').as_posix()}",
        "STORAGE_URI": (tmp_path / "lakehouse").as_uri(),
    }
    script = (
        "import asyncio, sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from app.main import app, lifespan\n"
        "async def main():\n"
        "    async with lifespan(app):\n"
        "        print('running', flush=True)\n"
        "        await asyncio.sleep(3)\n"
        "asyncio.run(main())\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(backend_dir)],
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "running"
        assert (tmp_path / LOCK_FILENAME).exists(), "startup must create the claim"
        with pytest.raises(InstanceLockError):
            InstanceLock(tmp_path).acquire()
    finally:
        child.wait(timeout=60)
        child.stdout.close()
        child.stdout = None

    assert child.returncode == 0, "the lifespan must shut down cleanly"
    counter_claim = InstanceLock(tmp_path).acquire()
    counter_claim.release()


def test_lock_is_released_when_the_holder_process_dies(tmp_path):
    """A crashed holder must not block a restart: the OS drops the lock with the process."""
    backend_dir = Path(__file__).resolve().parents[1]
    script = (
        "import sys, time\n"
        "sys.path.insert(0, sys.argv[2])\n"
        "from app.core.instance_lock import acquire_instance_lock\n"
        "acquire_instance_lock(sys.argv[1])\n"
        "print('locked', flush=True)\n"
        "time.sleep(60)\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path), str(backend_dir)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(InstanceLockError):
            InstanceLock(tmp_path).acquire()
    finally:
        child.kill()
        child.wait(timeout=15)
        if child.stdout:
            child.stdout.close()

    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            recovered = InstanceLock(tmp_path).acquire()
            recovered.release()
            return
        except InstanceLockError:
            time.sleep(0.1)
    pytest.fail("the lock was still held after the holding process was killed")
