import os
import socket
import subprocess

def _is_tcp_port_open(target_host: str, target_port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((target_host, target_port), timeout=timeout):
            return True
    except Exception:
        return False

def resolve_connection_host(host: str, port: int) -> str:
    """
    Cross-platform connection host resolver:
    1. If the provided host/port is directly listening and reachable, use it directly (e.g. 127.0.0.1 for localhost).
    2. On Windows with WSL2, if localhost is NOT reachable on this port, probe the WSL Docker bridge as a fallback.
    """
    is_local = host in ("localhost", "127.0.0.1", "::1")
    if is_local:
        # 1. Directly check if 127.0.0.1 is reachable
        if _is_tcp_port_open("127.0.0.1", port):
            return "127.0.0.1"

        # 2. Fallback to WSL2 bridge IP on Windows if 127.0.0.1 is not responding
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
