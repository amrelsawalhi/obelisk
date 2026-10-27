"""Two threads attaching the lakehouse at once must not fight over a new catalog.

DuckLake creates its metadata tables on the first attach and SQLite takes a single writer,
so the thread that arrived second died with "database is locked". The catalog tree attaches
twice at the same instant — once for the table scan, once for the folder list — which is
why a fresh data path broke the tree and every read after it worked.
"""
import tempfile
import threading
import time
from pathlib import Path

import pytest

from app.config import settings
from app.core.catalog import attach_lakehouse, lakehouse_connection, lakehouse_schemas


@pytest.fixture
def scratch_warehouse():
    """A warehouse with no catalog yet, for the length of one test.

    The race only exists before the metadata tables are written, so a test cannot use the
    session warehouse: it has had a catalog since the first test that read it.
    """
    original = settings.STORAGE_URI
    with tempfile.TemporaryDirectory(prefix="obelisk-attach-race-") as tmp:
        settings.STORAGE_URI = Path(tmp).as_uri()
        try:
            yield Path(tmp)
        finally:
            settings.STORAGE_URI = original


class _AttachRecorder:
    """Stands in for a DuckDB connection and records when its ATTACH ran.

    ``attach_lakehouse`` only ever calls ``execute``, so a hand-written recorder is enough
    to see whether two attaches overlapped — no catalog, no clock resolution games.
    """

    def __init__(self, log: list, hold: float = 0.05):
        self._log = log
        self._hold = hold

    def execute(self, statement, *args, **kwargs):
        if statement.startswith("ATTACH"):
            started = time.perf_counter()
            time.sleep(self._hold)
            self._log.append((started, time.perf_counter()))
        return self


def _run_together(count: int, run) -> list:
    """Runs ``run(index)`` in ``count`` threads released at the same instant.

    Failures are returned rather than raised so the assertion can show them: a thread's
    exception would otherwise vanish into the thread.
    """
    barrier = threading.Barrier(count)
    failures: list = []

    def worker(index: int) -> None:
        barrier.wait()
        try:
            run(index)
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    return failures


def test_two_first_attaches_never_overlap(scratch_warehouse):
    """The initialising attach is the only one that writes, so it is the one held."""
    log: list = []

    failures = _run_together(2, lambda _index: attach_lakehouse(_AttachRecorder(log)))

    assert not failures, failures
    assert len(log) == 2, log
    first, second = sorted(log)
    assert first[1] <= second[0], "two catalog initialisations ran at the same time"


def test_a_settled_catalog_attaches_without_waiting_its_turn(scratch_warehouse):
    """Once the catalog exists, attaches run side by side again.

    Holding every attach would queue each query behind the previous one, and an attach
    costs about 160 ms even against a catalog that already exists.
    """
    attach_lakehouse(_AttachRecorder([]))

    log: list = []
    failures = _run_together(2, lambda _index: attach_lakehouse(_AttachRecorder(log)))

    assert not failures, failures
    assert len(log) == 2, log
    first, second = sorted(log)
    assert second[0] < first[1], "a settled catalog must attach without taking the lock"


def test_concurrent_first_attaches_all_succeed(scratch_warehouse):
    """The real thing: several connections attach a brand-new catalog at the same moment."""
    seen: list = []

    def attach_and_read(_index: int) -> None:
        conn = lakehouse_connection()
        try:
            seen.append(lakehouse_schemas(conn))
        finally:
            conn.close()

    failures = _run_together(4, attach_and_read)

    assert not failures, failures
    assert len(seen) == 4, seen
