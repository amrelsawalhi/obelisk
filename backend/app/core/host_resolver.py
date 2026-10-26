import os
import socket
import subprocess

# Ports a sibling container on the compose network serves. Used only as a last
# resort when the backend is itself a container and its own localhost has nothing
# on the port: the service it needs is on obelisk_net under its container name.
_SIBLING_SERVICE_HOSTS = {
    5432: "obelisk_postgres",
    8123: "obelisk_clickhouse",
}


def _is_tcp_port_open(target_host: str, target_port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((target_host, target_port), timeout=timeout):
            return True
    except Exception:
        return False


def _runs_inside_container() -> bool:
    """True when this process is a container, where 127.0.0.1 is not the host."""
    return os.path.exists("/.dockerenv") or os.environ.get("OBELISK_IN_CONTAINER") == "1"


def resolve_connection_host(host: str, port: int) -> str:
    """
    Cross-platform connection host resolver:
    1. If the provided host/port is directly listening and reachable, use it directly (e.g. 127.0.0.1 for localhost).
    2. Inside a container, fall back to the Docker gateway and to the sibling service
       container that serves this port -- localhost there is the backend itself, so a
       service publishing 5432 is reachable only as ``obelisk_postgres``.
    3. On Windows with WSL2, if localhost is NOT reachable on this port, probe the WSL Docker bridge as a fallback.
    """
    is_local = host in ("localhost", "127.0.0.1", "::1")
    if is_local:
        # 1. Directly check if 127.0.0.1 is reachable
        if _is_tcp_port_open("127.0.0.1", port):
            return "127.0.0.1"

        # 2. Container-hosted backend: the gateway first (a service bound on the host),
        #    then the sibling container that owns this port on obelisk_net.
        if _runs_inside_container():
            candidates = ["host.docker.internal", _SIBLING_SERVICE_HOSTS.get(port)]
            for candidate in candidates:
                if candidate and _is_tcp_port_open(candidate, port):
                    return candidate

        # 3. Fallback to WSL2 bridge IP on Windows if 127.0.0.1 is not responding
        if os.name == "nt":
            try:
                out = subprocess.check_output(["wsl", "hostname", "-I"], text=True, timeout=1).strip()
                if out:
                    wsl_ip = out.split()[0]
                    if _is_tcp_port_open(wsl_ip, port):
                        return wsl_ip
            except Exception:
                pass

        return "127.0.0.1"

    return host
