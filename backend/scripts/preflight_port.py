#!/usr/bin/env python3
"""Makes a double-started backend impossible to miss.

A stale ``uvicorn --reload`` parent keeps its listening socket open after the
worker dies. TCP connects then succeed while HTTP never answers, so every API
call from the UI times out and the SPA renders an empty shell. This preflight
turns that silent state into an explicit one:

  * port free                     -> exit 0
  * port answers ``/health``      -> exit 1 ("already running", with the PID to stop)
  * port held by Obelisk itself   -> terminate the stale processes, exit 0
  * port held by anything else    -> exit 1, owner named, nothing killed

``--restart`` turns the "already running" case into a restart: an Obelisk backend
holding the port is stopped together with its reloader children and the launch
continues. That is the state where the API answers but the UI cannot be opened,
because only the backend window survived and the Vite window was closed.

Run it before binding the API port:

    python backend/scripts/preflight_port.py --port 8000
    python backend/scripts/preflight_port.py --port 8000 --restart
"""
from __future__ import annotations

import argparse
import socket
import sys
import time
import urllib.error
import urllib.request

import psutil


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def _healthy(port: int, timeout: float = 4.0) -> bool:
    """True when a real HTTP server answers /health (not just an open socket)."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=timeout) as res:
            return 200 <= res.status < 300
    except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
        return False


def _obelisk_owners(port: int) -> list[psutil.Process]:
    """Finds Obelisk uvicorn processes for this port (reloader parents)."""
    owners: list[psutil.Process] = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            name = (proc.info["name"] or "").lower()
            cmdline = " ".join(proc.info["cmdline"] or [])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if "python" not in name:
            continue
        if "uvicorn" not in cmdline or "app.main:app" not in cmdline:
            continue
        if f"--port {port}" not in cmdline and f"--port={port}" not in cmdline:
            continue
        owners.append(proc)
    return owners


def _with_children(owners: list[psutil.Process]) -> list[psutil.Process]:
    collected: list[psutil.Process] = []
    for proc in owners:
        collected.append(proc)
        try:
            collected.extend(proc.children(recursive=True))
        except psutil.Error:
            continue
    return collected


def _terminate(targets: list[psutil.Process]) -> None:
    # Children first: killing the reloader parent alone would orphan its worker.
    for proc in reversed(targets):
        try:
            proc.terminate()
        except psutil.Error:
            continue
    _, alive = psutil.wait_procs(targets, timeout=5)
    for proc in alive:
        try:
            proc.kill()
        except psutil.Error:
            continue
    psutil.wait_procs(alive, timeout=3)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure the API port is usable.")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Stop an Obelisk backend that already holds the port, then continue.",
    )
    args = parser.parse_args()

    if not _port_open(args.port):
        return 0

    owners = _obelisk_owners(args.port)

    if args.restart:
        if not owners:
            print(f"[!] Port {args.port} is held by something that is not an Obelisk backend.")
            print("    Refusing to kill it. Inspect the owner with:")
            print(f"      netstat -ano | findstr :{args.port}")
            return 1
        targets = _with_children(owners)
        print(f"[*] --restart: stopping the backend on port {args.port} ({len(targets)} process(es)).")
        _terminate(targets)
        for _ in range(20):
            if not _port_open(args.port):
                print(f"[+] Port {args.port} is free.")
                return 0
            time.sleep(0.25)
        print(f"[!] Port {args.port} is still in use after cleanup.")
        return 1

    if _healthy(args.port):
        print(f"[!] Obelisk is already running on port {args.port} (health check passed).")
        if owners:
            print(f"    Stop that instance:  taskkill /PID {owners[0].pid} /T /F")
        else:
            print(f"    Find that instance:  netstat -ano | findstr :{args.port}")
        print("    Replace it instead:  run_obelisk.bat --restart")
        print("    Note: the page you open is served by the Vite window, not the API.")
        print("    If the browser shows nothing, that window is the one that is gone.")
        return 1

    if not owners:
        print(f"[!] Port {args.port} is in use but unresponsive, and it is not an Obelisk process.")
        print("    Refusing to kill it. Inspect the owner with:")
        print(f"      netstat -ano | findstr :{args.port}")
        return 1

    targets = _with_children(owners)
    print(f"[*] Port {args.port} is held by a stale Obelisk process; reaping {len(targets)} process(es).")
    _terminate(targets)

    for _ in range(20):
        if not _port_open(args.port):
            print(f"[+] Port {args.port} is free.")
            return 0
        time.sleep(0.25)

    print(f"[!] Port {args.port} is still in use after cleanup.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
