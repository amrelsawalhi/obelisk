import os
import pytest
import docker
from docker.errors import NotFound
from app.core.docker_manager import docker_mgr, MANAGED_LABEL, OBELISK_TAG
from app.core.resource_advisor import advisor

def test_docker_manager_deploy_and_out_of_band_verification():
    """
    Section 5 C Out-of-Band Daemon Verification:
    Deploys a container via docker_manager, then instantiates an independent docker.from_env()
    client to fetch the container directly from the host Docker daemon and verify:
      1. container.status == "running"
      2. container.attrs["Config"]["Labels"].get("managed_by") == "obelisk"
      3. container.attrs["HostConfig"]["Memory"] > 0 (proving cgroup memory limit applied)
      4. "obelisk_net" in container.attrs["NetworkSettings"]["Networks"]
    """
    if docker_mgr.client is None:
        pytest.skip("Docker daemon not running on localhost")

    svc_name = "test_probe_svc"

    # Clean up any leftover test container first
    try:
        docker_mgr.stop_service(svc_name)
    except Exception:
        pass

    # Deploy test service container using alpine
    rec = advisor.recommend_allocation(svc_name)
    expected_memory_mb = rec["recommended_ram_mb"]

    created_container = docker_mgr.deploy_service(
        service_name=svc_name,
        image="alpine:latest",
        command=["tail", "-f", "/dev/null"],
        memory_mb=expected_memory_mb,
    )

    container_id = created_container.id
    container_name = f"obelisk_{svc_name}"

    try:
        # Physical Proof: Instantiate an independent docker.from_env() client
        independent_client = docker.from_env()

        # Fetch container out-of-band directly from host Docker daemon by ID
        host_container = independent_client.containers.get(container_id)

        # 1. Status must be running
        assert host_container.status == "running", (
            f"Container status expected 'running', got '{host_container.status}'"
        )

        # 2. Managed label must equal obelisk
        labels = host_container.attrs.get("Config", {}).get("Labels", {})
        assert labels.get(MANAGED_LABEL) == OBELISK_TAG, (
            f"Expected label {MANAGED_LABEL}={OBELISK_TAG}, got {labels}"
        )
        assert labels.get("service") == svc_name

        # 3. Memory limit in Linux cgroup HostConfig must be > 0 and match ResourceAdvisor
        cgroup_memory = host_container.attrs.get("HostConfig", {}).get("Memory", 0)
        assert cgroup_memory > 0, (
            "RESOURCE ADVISOR FAILURE: HostConfig Memory limit was 0, cgroup cap not applied!"
        )
        assert cgroup_memory == expected_memory_mb * 1024 * 1024, (
            f"Memory mismatch: expected {expected_memory_mb * 1024 * 1024}, got {cgroup_memory}"
        )

        # 4. Attached to obelisk_net network
        networks = host_container.attrs.get("NetworkSettings", {}).get("Networks", {})
        assert "obelisk_net" in networks, (
            f"NETWORK FAILURE: 'obelisk_net' not found in container networks: {list(networks.keys())}"
        )

        # Also verify via docker_mgr status query
        status_info = docker_mgr.get_service_status(svc_name)
        assert status_info["status"] == "running"
        assert status_info["allocated_memory_mb"] == expected_memory_mb
        assert status_info["container_id"] == container_id

    finally:
        # Clean up container
        docker_mgr.stop_service(svc_name)

        # Verify removal out-of-band
        with pytest.raises(NotFound):
            independent_client.containers.get(container_id)

def test_docker_manager_safety_rule():
    """
    Safety invariant:
    docker_manager must never stop or inspect containers that do not carry
    the managed_by=obelisk label.
    """
    if docker_mgr.client is None:
        pytest.skip("Docker daemon not running on localhost")

    independent_client = docker.from_env()
    unmanaged_name = "obelisk_rogue_container"

    # Ensure clean state
    try:
        c = independent_client.containers.get(unmanaged_name)
        c.remove(force=True)
    except NotFound:
        pass

    # Create an unmanaged container directly via docker client (no obelisk label)
    unmanaged = independent_client.containers.run(
        image="alpine:latest",
        name=unmanaged_name,
        command=["tail", "-f", "/dev/null"],
        detach=True,
        labels={"different_label": "true"},
    )

    try:
        # Attempting to inspect via get_service_container must raise ValueError
        with pytest.raises(ValueError, match="not managed by Obelisk"):
            docker_mgr.get_service_container("rogue_container")

        # Attempting to stop via stop_service must raise ValueError
        with pytest.raises(ValueError, match="not managed by Obelisk"):
            docker_mgr.stop_service("rogue_container")

    finally:
        try:
            unmanaged.remove(force=True)
        except Exception:
            pass

