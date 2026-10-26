"""Readiness probes must work from a host install and from inside the container.

``127.0.0.1:<published port>`` is correct when the backend runs on the host. It is
useless inside ``obelisk_net``, where localhost is the backend itself — there the
probe falls back to ``<container name>:<container port>``. The fallback must never
engage on a host install, where the container name does not resolve.
"""
from __future__ import annotations

import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from app.config import settings
from app.core.docker_manager import docker_mgr
from app.core.service_blueprints import get_blueprint


def _inside_container() -> bool:
    return Path("/.dockerenv").exists() or os.environ.get("OBELISK_IN_CONTAINER") == "1"


class _HealthHandler(BaseHTTPRequestHandler):
    """Answers ``/health`` with 200, so the probe sees exactly what Superset serves."""

    def do_GET(self):  # noqa: N802 — http.server's naming
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "OK"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


def test_a_host_install_never_probes_a_container_name():
    """Regression guard: the sibling fallback exists only for containerised backends."""
    if _inside_container():
        pytest.skip("this test session is itself running inside a container")
    assert docker_mgr._sibling_probe_target("superset", 8088) is None
    assert docker_mgr._sibling_probe_target("redpanda", 9092) is None


def test_http_probe_reads_a_real_server_on_localhost():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _HealthHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = docker_mgr.probe_readiness("superset", host_port=port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert result["ready"] is True
    assert result["detail"] == "HTTP 200", "a host-side probe must not report a sibling"


def test_tcp_probe_accepts_a_listening_port_and_rejects_a_closed_one():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    open_port = listener.getsockname()[1]

    closed_port = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    closed_port.bind(("127.0.0.1", 0))
    dead_port = closed_port.getsockname()[1]
    closed_port.close()  # nothing listens on dead_port any more

    try:
        ready = docker_mgr.probe_readiness("redpanda", host_port=open_port)
        refused = docker_mgr.probe_readiness("redpanda", host_port=dead_port)
    finally:
        listener.close()

    assert ready["ready"] is True
    assert ready["detail"] == f"port {open_port} accepting connections"
    assert refused["ready"] is False
    assert refused["detail"], "a refused connection must be reported, not raised"


def test_a_containerised_backend_probes_the_sibling_on_its_container_port():
    """Inside ``obelisk_net`` the fallback is ``<container name>:<container port>``.

    Skipped unless a managed service is actually running on the network, and unless
    we can pretend to be the containerised backend, which is the only situation the
    fallback exists for.
    """
    running = [
        service_name
        for service_name in ("postgres", "clickhouse", "redpanda", "gitea", "superset")
        if (container := docker_mgr.get_service_container(service_name)) is not None
        and container.status == "running"
        and settings.OBELISK_NET
        in ((container.attrs.get("NetworkSettings") or {}).get("Networks") or {})
    ]
    if not running:
        pytest.skip("no managed service is running on obelisk_net in this environment")
    if _inside_container():
        pytest.skip("this test session is already inside a container")

    service_name = running[0]
    blueprint = get_blueprint(service_name)
    container_port = int(str(blueprint.primary_container_port).split("/")[0])
    os.environ["OBELISK_IN_CONTAINER"] = "1"
    try:
        target = docker_mgr._sibling_probe_target(service_name, container_port)
    finally:
        os.environ.pop("OBELISK_IN_CONTAINER", None)

    assert target == f"obelisk_{service_name}:{container_port}"
