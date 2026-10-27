import os
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
        """Returns free disk space for deploy pre-flight checks."""
        usage = psutil.disk_usage(path)
        return {
            "free_gb": round(usage.free / (1024 ** 3), 2),
            "total_gb": round(usage.total / (1024 ** 3), 2),
            "percent_used": usage.percent,
        }

    def get_active_container_allocations(self) -> Dict[str, Any]:
        """Probes active Docker container memory allocations managed by Obelisk."""
        total_allocated_bytes = 0
        containers_info = []

        if self._docker_client is None:
            # Lazy link to the live Docker manager: container allocations are probed
            # from the real engine instead of always reporting zero.
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

        # Baseline proportions based on Obelisk Master Plan
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
        """Returns safe per-connection memory limit for DuckDB initialization.

        The limit is a share of the RAM the host can still safely give away, divided by
        the number of connections expected to be heavy at the same time. Giving every
        connection 25% of total RAM (the previous rule) let four concurrent queries ask
        for the whole machine.
        """
        specs = self.get_host_specs()
        active = self.get_active_container_allocations()
        remaining_safe_gb = max(
            2.0, specs["max_safe_allocation_gb"] - active["total_allocated_gb"]
        )
        slots = max(1, int(concurrent_slots))
        # Allocate at most 50% of remaining safe host RAM across concurrent DuckDB slots
        per_slot_gb = max(1, min(12, int((remaining_safe_gb * 0.50) / slots)))
        return f"{per_slot_gb}GB"

    def get_duckdb_temp_limit(self, path: str, configured: str = "50GB") -> str:
        """How much DuckDB may spill into its temp directory, given what is actually free.

        ``DUCKDB_TEMP_SIZE_LIMIT`` is a fixed string, so it was right for exactly one disk.
        On a 40 GB volume DuckDB believed it had 50 GB to spill into and died mid-query with
        "could not write to file"; on a 4 TB volume it stopped at 50 GB for no reason. The
        budget is now the configured ceiling *or* half the free space, whichever is smaller,
        so a spill can always leave room for the lakehouse's own Parquet and the catalog.
        """
        wanted = _parse_size_to_mb(configured) or 50 * 1024
        try:
            free_bytes = int(psutil.disk_usage(path).free)
        except Exception:
            return configured
        # Half of what is free, never below the 1 GB DuckDB needs to be useful at all.
        allowed_mb = max(1024, free_bytes // (1024 * 1024) // 2)
        return f"{min(wanted, allowed_mb)}MB"

    def get_spark_memory_limit(self) -> str:
        """Returns safe memory limit string for PySpark driver initialization (e.g. '2g', '4g')."""
        specs = self.get_host_specs()
        total_gb = specs["total_ram_gb"]
        limit_gb = max(1, min(8, int(total_gb * 0.20)))
        return f"{limit_gb}g"

advisor = ResourceAdvisor()
