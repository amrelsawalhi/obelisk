import os
from pathlib import Path
import re
import psutil
from typing import Dict, Any, List

# Multipliers to megabytes, so a configured "50GB" and a computed byte count can be compared.
_SIZE_UNITS_MB = {"": 1 / (1024 * 1024), "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}


def _parse_size_to_mb(value: object) -> int | None:
    """'50GB' / '512MB' / '2T' -> megabytes. ``None`` when the text is not a size."""
    text = str(value or "").strip().upper().replace(" ", "")
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)([KMGT]?)I?B?(?:YTES?)?$", text)
    if not match:
        return None
    number = float(match.group(1))
    return int(number * _SIZE_UNITS_MB[match.group(2)])


def _existing_path(path: object) -> str:
    """Resolves to the nearest existing ancestor directory so disk_usage never fails on a not-yet-created leaf path."""
    probe = Path(str(path or "."))
    try:
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
    except Exception:
        return "."
    return str(probe)


class ResourceAdvisor:
    """
    Hardware Sizing Engine:
    Probes host specs via psutil and active container allocations to compute
    dynamic memory and CPU caps, ensuring total allocations never exceed 85% of host memory.
    """
    def __init__(self, docker_client=None):
        self._docker_client = docker_client

    def get_host_specs(self) -> Dict[str, Any]:
        """Probes live host hardware specifications via psutil."""
        vm = psutil.virtual_memory()
        cpu_count = psutil.cpu_count(logical=True) or 1
        cpu_percent = psutil.cpu_percent(interval=None)

        total_ram_gb = vm.total / (1024 ** 3)
        available_ram_gb = vm.available / (1024 ** 3)

        return {
            "total_ram_bytes": vm.total,
            "total_ram_gb": round(total_ram_gb, 2),
            "available_ram_gb": round(available_ram_gb, 2),
            "ram_percent_used": vm.percent,
            "cpu_count": cpu_count,
            "cpu_percent": cpu_percent,
            "max_safe_allocation_bytes": int(vm.total * 0.85),
            "max_safe_allocation_gb": round(total_ram_gb * 0.85, 2),
        }

    def get_disk_info(self, path: str = ".") -> Dict[str, Any]:
        """Returns free disk space for deploy and ingestion pre-flight checks."""
        usage = psutil.disk_usage(_existing_path(path))
        return {
            "free_bytes": int(usage.free),
            "free_gb": round(usage.free / (1024 ** 3), 2),
            "total_gb": round(usage.total / (1024 ** 3), 2),
            "percent_used": usage.percent,
        }

    def assert_disk_headroom(
        self, path: str = ".", min_free_bytes: int = 512 * 1024 * 1024
    ) -> None:
        """Raises RuntimeError when free disk space on ``path`` falls below safety headroom."""
        try:
            free_bytes = int(psutil.disk_usage(_existing_path(path)).free)
        except Exception:
            return
        if free_bytes < min_free_bytes:
            free_mb = free_bytes // (1024 * 1024)
            req_mb = min_free_bytes // (1024 * 1024)
            raise RuntimeError(
                f"Insufficient disk space on '{path}': {free_mb} MB free, "
                f"at least {req_mb} MB required to prevent catalog corruption."
            )

    def get_active_container_allocations(self) -> Dict[str, Any]:
        """Probes active Docker container memory allocations managed by Obelisk."""
        total_allocated_bytes = 0
        containers_info = []

        if self._docker_client is None:
            try:
                from app.core.docker_manager import docker_mgr
                self._docker_client = docker_mgr.client
            except Exception:
                pass
        if self._docker_client is not None:
            try:
                for c in self._docker_client.containers.list():
                    labels = c.attrs.get("Config", {}).get("Labels", {}) or {}
                    if labels.get("managed_by") == "obelisk":
                        mem = c.attrs.get("HostConfig", {}).get("Memory", 0)
                        total_allocated_bytes += mem
                        containers_info.append({
                            "name": c.name,
                            "allocated_bytes": mem,
                            "allocated_mb": mem // (1024 ** 2),
                        })
            except Exception:
                pass

        return {
            "total_allocated_bytes": total_allocated_bytes,
            "total_allocated_gb": round(total_allocated_bytes / (1024 ** 3), 2),
            "containers": containers_info,
        }

    def recommend_allocation(self, service_name: str) -> Dict[str, Any]:
        """
        Dynamically calculates safe RAM/CPU caps for a service.
        Prevents cumulative allocations from exceeding 85% of host memory.
        """
        specs = self.get_host_specs()
        total_gb = specs["total_ram_gb"]
        max_safe_gb = specs["max_safe_allocation_gb"]
        active = self.get_active_container_allocations()
        currently_allocated_gb = active["total_allocated_gb"]
        remaining_safe_gb = max(0.25, max_safe_gb - currently_allocated_gb)

        defaults = {
            "postgres": {"min_mb": 512, "target_ratio": 0.20, "max_mb": 12288},
            "superset": {"min_mb": 512, "target_ratio": 0.10, "max_mb": 4096},
            "clickhouse": {"min_mb": 512, "target_ratio": 0.15, "max_mb": 6144},
            "redpanda": {"min_mb": 256, "target_ratio": 0.05, "max_mb": 2048},
            "gitea": {"min_mb": 128, "target_ratio": 0.02, "max_mb": 512},
        }

        profile = defaults.get(
            service_name.lower(),
            {"min_mb": 256, "target_ratio": 0.05, "max_mb": 2048}
        )

        calculated_mb = int((total_gb * 1024) * profile["target_ratio"])
        recommended_mb = max(profile["min_mb"], min(calculated_mb, profile["max_mb"]))

        remaining_safe_mb = int(remaining_safe_gb * 1024)
        if recommended_mb > remaining_safe_mb:
            recommended_mb = max(profile["min_mb"], remaining_safe_mb)

        recommended_bytes = recommended_mb * 1024 * 1024

        cpus = specs["cpu_count"]
        recommended_cpus = max(1.0, min(float(cpus), round(cpus * profile["target_ratio"] * 2, 1)))

        return {
            "service_name": service_name,
            "recommended_ram_mb": recommended_mb,
            "recommended_ram_bytes": recommended_bytes,
            "recommended_cpus": recommended_cpus,
            "total_host_ram_gb": total_gb,
            "remaining_safe_gb": remaining_safe_gb,
        }

    def get_duckdb_memory_limit(self, concurrent_slots: int = 4) -> str:
        """Returns safe per-connection memory limit for DuckDB initialization."""
        specs = self.get_host_specs()
        active = self.get_active_container_allocations()
        remaining_safe_gb = max(
            2.0, specs["max_safe_allocation_gb"] - active["total_allocated_gb"]
        )
        slots = max(1, int(concurrent_slots))
        per_slot_gb = max(1, min(12, int((remaining_safe_gb * 0.50) / slots)))
        return f"{per_slot_gb}GB"

    def get_duckdb_temp_limit_mb(self, path: str, configured: str = "50GB") -> int:
        """Numeric megabyte spill limit bounded by configured ceiling and 50% of free disk."""
        wanted = _parse_size_to_mb(configured) or 50 * 1024
        try:
            free_bytes = int(psutil.disk_usage(_existing_path(path)).free)
        except Exception:
            return wanted
        allowed_mb = max(1024, free_bytes // (1024 * 1024) // 2)
        return min(wanted, allowed_mb)

    def get_duckdb_temp_limit(self, path: str, configured: str = "50GB") -> str:
        """How much DuckDB may spill into its temp directory, given what is actually free."""
        return f"{self.get_duckdb_temp_limit_mb(path, configured)}MB"

    def get_spark_memory_limit(self) -> str:
        """Returns safe memory limit string for PySpark driver initialization (e.g. '2g', '4g')."""
        specs = self.get_host_specs()
        total_gb = specs["total_ram_gb"]
        limit_gb = max(1, min(8, int(total_gb * 0.20)))
        return f"{limit_gb}g"

    def get_child_env_limits(self) -> Dict[str, str]:
        """Environment variables that bound glibc arena fragmentation and thread oversubscription in child processes."""
        cpu_cores = os.cpu_count() or 4
        half_cores = str(max(1, cpu_cores // 2))
        return {
            "MALLOC_ARENA_MAX": "2",
            "PYTHONMALLOC": "malloc",
            "POLARS_MAX_THREADS": half_cores,
            "OMP_NUM_THREADS": half_cores,
            "NUMEXPR_MAX_THREADS": half_cores,
            "OPENBLAS_NUM_THREADS": half_cores,
        }

    def make_posix_rlimit_preexec(
        self,
        *,
        new_session: bool = False,
        max_nofile: int = 8192,
        max_memory_bytes: int | None = None,
    ):
        """Returns a POSIX preexec_fn that raises RLIMIT_NOFILE and applies optional RLIMIT_DATA caps."""
        if os.name == "nt":
            return None

        env_cap_mb = os.environ.get("OBELISK_CHILD_MAX_RAM_MB", "").strip()
        if max_memory_bytes is None and env_cap_mb.isdigit() and int(env_cap_mb) > 0:
            max_memory_bytes = int(env_cap_mb) * 1024 * 1024

        def _preexec() -> None:
            if new_session:
                try:
                    os.setsid()
                except Exception:
                    pass
            try:
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                target_nofile = min(max(soft, max_nofile), hard if hard != resource.RLIM_INFINITY else max_nofile)
                if target_nofile > soft:
                    resource.setrlimit(resource.RLIMIT_NOFILE, (target_nofile, hard))
                if max_memory_bytes and max_memory_bytes > 0:
                    _, hard_mem = resource.getrlimit(resource.RLIMIT_DATA)
                    cap = min(max_memory_bytes, hard_mem) if hard_mem != resource.RLIM_INFINITY else max_memory_bytes
                    resource.setrlimit(resource.RLIMIT_DATA, (cap, hard_mem))
            except Exception:
                pass

        return _preexec


advisor = ResourceAdvisor()
