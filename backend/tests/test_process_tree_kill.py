"""``_kill_process_tree`` stops the work, not just the process that started it.

``Popen.kill()`` reaps one PID. A pipeline step that runs dbt, a notebook or a Spark
job spawns grandchildren, and those keep running — holding their ports, memory and
file locks — after the step has been reported cancelled. This drives the helper
against a real parent/grandchild pair.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import psutil

from app.engines.pipeline_engine import _kill_process_tree


def _alive(pid: int) -> bool:
    try:
        proc = psutil.Process(pid)
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _wait_for(predicate, timeout: float = 20.0, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_kill_process_tree_stops_a_grandchild(tmp_path: Path):
    beat = tmp_path / "beat.txt"
    pid_file = tmp_path / "grandchild.pid"
    grandchild_code = (
        "import time\n"
        "from pathlib import Path\n"
        f"beat = Path({str(beat)!r})\n"
        "for _ in range(600):\n"
        "    with beat.open('a', encoding='utf-8') as fh:\n"
        "        fh.write('beat\\n')\n"
        "    time.sleep(0.1)\n"
    )
    parent_code = (
        "import subprocess, sys, time\n"
        "from pathlib import Path\n"
        f"child = subprocess.Popen([sys.executable, '-c', {grandchild_code!r}])\n"
        f"Path({str(pid_file)!r}).write_text(str(child.pid), encoding='utf-8')\n"
        "while True:\n"
        "    time.sleep(0.5)\n"
    )

    proc = subprocess.Popen(
        [sys.executable, "-c", parent_code],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_for(lambda: pid_file.exists() and beat.exists() and beat.stat().st_size > 0), (
            "the parent never started its grandchild"
        )
        grandchild = int(pid_file.read_text(encoding="utf-8").strip())
        assert _alive(grandchild)

        _kill_process_tree(proc)

        assert proc.wait(timeout=10) is not None
        assert _wait_for(lambda: not _alive(grandchild), timeout=10), (
            "the grandchild outlived the kill, which is exactly the leak this guards"
        )
        size = beat.stat().st_size
        time.sleep(1.0)
        assert beat.stat().st_size == size, "the grandchild is still writing"
    finally:
        if proc.poll() is None:
            proc.kill()
