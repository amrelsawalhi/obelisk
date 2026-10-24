"""The launcher's port guard decides what may be killed on port 8000.

It already reaped *stale* Obelisk sockets (open port, no HTTP answer). The case it
could not handle was a backend that answers ``/health`` while the Vite window that
serves the page is gone: the launch aborted with "already running" and no way
forward. ``--restart`` covers it, but only for processes that really are Obelisk
backends - anything else must be left alone.
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "preflight_port.py"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        timeout=90,
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_free_port_passes():
    result = _run("--port", str(_free_port()))

    assert result.returncode == 0, result.stdout + result.stderr


def test_restart_refuses_a_port_held_by_another_program():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = sock.getsockname()[1]

        result = _run("--port", str(port), "--restart")

    assert result.returncode == 1, result.stdout
    assert "Refusing to kill" in result.stdout


def test_restart_reaps_an_obelisk_shaped_listener(tmp_path):
    """A process that looks like `uvicorn app.main:app --port N` is stopped.

    Stands in for a leftover backend: it holds the port and carries the command
    line the launcher uses to decide ownership, without booting the whole app.
    The holder signals readiness through a file so the test never connects first
    (a probe would occupy the accept backlog and make the next connect refused).
    """
    port = _free_port()
    ready = tmp_path / "listening"
    holder_code = (
        "import socket, time\n"
        "s = socket.socket()\n"
        f"s.bind(('127.0.0.1', {port}))\n"
        "s.listen(64)\n"
        f"open({str(ready)!r}, 'w').write('ready')\n"
        "time.sleep(120)\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, f"uvicorn app.main:app --port {port}"],
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            time.sleep(0.1)
        assert ready.exists(), "holder never started listening"

        result = _run("--port", str(port), "--restart")

        assert result.returncode == 0, result.stdout + result.stderr
        assert "is free" in result.stdout
        assert holder.wait(timeout=10) is not None
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=10)
