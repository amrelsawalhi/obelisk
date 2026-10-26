"""Marimo may only manage the server it started.

The audit's finding was concrete: ``start(port=8000)`` was accepted and remembered, and
``stop()`` found the PID **by port** — so stopping the notebook server terminated whatever
listened there, up to and including the app itself.

These pin the two halves of the fix: a port other than the configured one is refused, and
stop never terminates a process whose command line is not marimo.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from app.engines.marimo_service import MarimoService


def test_a_different_port_is_refused() -> None:
    service = MarimoService()
    with pytest.raises(ValueError) as exc:
        service.start(port=service.port + 1)
    assert "configured to run on port" in str(exc.value)


def test_stop_while_idle_is_a_no_op() -> None:
    service = MarimoService()
    assert service.stop()["status"] == "not_running"


def test_a_stranger_process_is_never_terminated(tmp_path) -> None:
    """A recorded PID that is not marimo must survive ``stop()``, and the file is dropped."""
    stranger = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        service = MarimoService()
        service.pid_file = tmp_path / "marimo.pid"
        service.pid_file.write_text(str(stranger.pid), encoding="utf-8")

        result = service.stop()

        assert result["status"] == "not_running"
        assert not service.pid_file.exists()
        assert stranger.poll() is None, "stop() terminated a process it did not start"
    finally:
        stranger.terminate()
        stranger.wait(timeout=10)


def test_a_stale_pid_file_is_ignored(tmp_path) -> None:
    service = MarimoService()
    service.pid_file = tmp_path / "marimo.pid"
    service.pid_file.write_text("999999", encoding="utf-8")  # no such process

    assert service.stop()["status"] == "not_running"
    assert not service.pid_file.exists()
