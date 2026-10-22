"""
Dagster Service Client:
Submits runs to the deployed Dagster service over GraphQL (plain httpx — no dagster
import on the trigger path), with an in-process dg.materialize fallback when the
service is not deployed/reachable (preserves ad-hoc behavior and the test suite).
"""
import os
import time
from typing import Any, Dict, List, Optional
import httpx

DAGSTER_HOST = os.environ.get("OBELISK_DAGSTER_HOST", "localhost")
DAGSTER_PORT = int(os.environ.get("OBELISK_DAGSTER_PORT", "3000"))
_GRAPHQL = f"http://{DAGSTER_HOST}:{DAGSTER_PORT}/graphql"
_TERMINAL = {"SUCCESS", "FAILURE", "CANCELED"}

# Shared client keeps TCP + HTTP alive across GraphQL calls (and the readiness
# probe). Reusing one client drops the per-request connect/handshake overhead,
# which is what made the first panel-open feel slow after a cold Dagster deploy.
_http_client = httpx.Client(timeout=15.0)


class DagsterServiceUnavailable(RuntimeError):
    pass


def service_status(timeout: float = 3.0) -> Dict[str, Any]:
    """Probes the Dagster webserver (/server_info)."""
    try:
        resp = _http_client.get(f"http://{DAGSTER_HOST}:{DAGSTER_PORT}/server_info", timeout=timeout)
        if resp.status_code == 200:
            return {"available": True, "info": resp.json()}
        return {"available": False, "info": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"available": False, "info": str(e)}


def _graphql(query: str, variables: Optional[dict] = None, timeout: float = 15.0) -> dict:
    try:
        resp = _http_client.post(_GRAPHQL, json={"query": query, "variables": variables or {}}, timeout=timeout)
        data = resp.json()
    except Exception as e:
        raise DagsterServiceUnavailable(f"Dagster GraphQL unreachable at {_GRAPHQL}: {e}")
    if "errors" in data:
        raise RuntimeError(f"Dagster GraphQL error: {data['errors']}")
    return data["data"]


def discover_workspace() -> Dict[str, Any]:
    """Finds repository location/repo/job names at runtime (never hardcode)."""
    data = _graphql(
        """
      query Repositories {
        repositoriesOrError {
          __typename
          ... on RepositoryConnection {
            nodes { name location { name } pipelines { name } }
          }
          ... on PythonError { message stack }
        }
      }
        """
    )
    conn = data["repositoriesOrError"]
    if conn["__typename"] != "RepositoryConnection":
        raise RuntimeError(f"Workspace discovery failed: {conn.get('message')}")
    nodes = conn["nodes"]
    if not nodes:
        raise RuntimeError("No repositories loaded in Dagster workspace")
    repo = nodes[0]
    return {
        "location": repo["location"]["name"],
        "repository": repo["name"],
        "jobs": [p["name"] for p in repo.get("pipelines") or []],
    }


def reload_workspace(timeout: float = 60.0) -> None:
    """Reloads the code location after dag files are written."""
    loc = discover_workspace()["location"]
    # __typename-only selection: union member types drift across Dagster versions
    _graphql(
        """
      mutation Reload($loc: String!) {
        reloadRepositoryLocation(repositoryLocationName: $loc) {
          __typename
        }
      }
        """,
        {"loc": loc},
        timeout=timeout,
    )


def launch_job(job_name: str, tags: Optional[List[Dict[str, str]]] = None) -> str:
    """Launches a job run; returns the Dagster run id."""
    sel = discover_workspace()
    for attempt in range(4):
        if job_name in sel["jobs"]:
            break
        try:
            reload_workspace()
        except Exception:
            pass
        time.sleep(2.5)
        sel = discover_workspace()
    if job_name not in sel["jobs"]:
        raise RuntimeError(f"Job '{job_name}' not found in workspace. Jobs: {sel['jobs']}")
    # Dagster 1.13 schema: launchRun(executionParams: ExecutionParams!),
    # selector is JobOrPipelineSelector, tags live under executionMetadata.
    data = _graphql(
        """
      mutation Launch($params: ExecutionParams!) {
        launchRun(executionParams: $params) {
          __typename
          ... on LaunchRunSuccess { run { runId status } }
          ... on PythonError { message }
        }
      }
        """,
        {"params": {
            "selector": {
                "repositoryLocationName": sel["location"],
                "repositoryName": sel["repository"],
                "jobName": job_name,
            },
            "runConfigData": {},
            "executionMetadata": {"tags": tags or []},
        }},
    )
    res = data["launchRun"]
    if res["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"launchRun failed: {res.get('message')}")
    return res["run"]["runId"]


def wait_for_run(run_id: str, timeout: float = 300.0, poll: float = 2.0) -> Dict[str, Any]:
    """Polls run status until terminal; returns {"status": str, "run_id": str}."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = _graphql(
            """
          query RunStatus($runId: ID!) {
            runOrError(runId: $runId) {
              __typename
              ... on Run { runId status }
              ... on RunNotFoundError { message }
              ... on PythonError { message }
            }
          }
            """,
            {"runId": run_id},
        )
        res = data["runOrError"]
        if res["__typename"] == "Run":
            if res["status"] in _TERMINAL:
                return {"status": res["status"], "run_id": run_id}
        else:
            raise RuntimeError(f"Run status query failed: {res.get('message')}")
        time.sleep(poll)
    return {"status": "TIMEOUT", "run_id": run_id}


def trigger_ingest(
    source_type: str,
    connection_params: Dict[str, Any],
    entity: str,
    mode: str = "snapshot",
    watermark_column: Optional[str] = None,
    target_name: Optional[str] = None,
    target_folder: Optional[str] = None,
    limit: Optional[int] = None,
    write_strategy: str = "append",
    primary_key: Optional[str] = None,
    job_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Triggers an ingestion run: via the deployed Dagster service when reachable
    (launches `job_name` if provided), else in-process materialize fallback.
    Response shape preserved for routes_ingest compatibility.
    """
    clean_stem = (target_name or (entity.split(".", 1)[-1] if "." in entity else entity))
    clean_stem = clean_stem.replace(".", "_").replace("/", "_").replace("\\", "_")
    asset_name = clean_stem

    if job_name and service_status()["available"]:
        try:
            run_id = launch_job(job_name, tags=[{"key": "obelisk.ingest", "value": asset_name}])
            result = wait_for_run(run_id)
            return {
                "success": result["status"] == "SUCCESS",
                "run_id": run_id,
                "asset_name": asset_name,
                "metadata": {"dagster_run_status": result["status"]},
                "logs": [f"[launchRun] {job_name} -> {run_id}", f"[status] {result['status']}"],
            }
        except (DagsterServiceUnavailable, RuntimeError) as e:
            # Service down, job not visible yet, or GraphQL drift -> in-process fallback
            fallback_note = f"[fallback] Dagster service path unavailable: {str(e)[:160]}"
            print(fallback_note)

    # ---- In-process fallback (service not deployed / just went down) ----
    import dagster as dg
    from dagster_workspace.repository import create_ingest_asset

    ingest_asset = create_ingest_asset(
        asset_name=asset_name, source_type=source_type, entity=entity, mode=mode,
        watermark_column=watermark_column, target_name=target_name,
        target_folder=target_folder, limit=limit, write_strategy=write_strategy,
        primary_key=primary_key, connection_params=connection_params,
    )
    result = dg.materialize([ingest_asset])
    logs = [f"[{e.event_type_value}] {e.message}" for e in result.all_events if e.message]
    metadata_dict = {}
    mat_events = [e for e in result.all_events if e.is_step_materialization]
    if mat_events:
        mat = mat_events[0].event_specific_data.materialization
        metadata_dict = {k: v.value for k, v in mat.metadata.items()}
    return {"success": result.success, "run_id": result.run_id,
            "asset_name": asset_name, "metadata": metadata_dict, "logs": logs}


# Native-panel helpers: jobs, schedules, recent runs. All go through `_graphql`
# so a DagsterServiceUnavailable bubbles up and the routes layer can convert to
# a 200-with-[] response (frontend falls back to offline card, no 502 cascade).
#
# GraphQL drift philosophy: ``__typename`` unions on every result and Python
# errors on every result. Older Dagster deployments return slightly different
# shapes — we ignore what we don't need instead of crashing.

_JOBS_QUERY = """
query ObeliskJobsPanel {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
        pipelines { name description }
      }
    }
    ... on PythonError { message }
  }
}
"""

# Dagster 1.13: schedulesOrError requires a mandatory repositorySelector argument
# and errors when called without one. The safe approach is to read schedules from
# repositoriesOrError (no selector needed) — identical data, works in all versions.
_SCHEDULES_QUERY = """
query ObeliskSchedulesPanel {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
        schedules {
          id
          name
          description
          cronSchedule
          executionTimezone
          scheduleState {
            status
          }
        }
      }
    }
    ... on PythonError { message }
  }
}
"""

_RUNS_QUERY = """
query ObeliskRecentRuns($limit: Int!) {
  runsOrError(limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId
        status
        startTime
        endTime
        jobName
        assetSelection { path }
      }
    }
    ... on PythonError { message }
  }
}
"""


def _normalize_jobs_payload(data: dict) -> List[Dict[str, str]]:
    """Flatten ``repositoriesOrError`` to a list of {name, description} dicts.

    De-duplicates by name (Dagster occasionally surfaces the same job twice
    across reloaded locations during the workspace reload window).
    """
    conn = data.get("repositoriesOrError") or {}
    if conn.get("__typename") != "RepositoryConnection":
        return []
    out: List[Dict[str, str]] = []
    for repo in conn.get("nodes") or []:
        for p in repo.get("pipelines") or []:
            out.append({
                "name": p.get("name", ""),
                "description": p.get("description") or "",
            })
    seen, dedup = set(), []
    for j in out:
        if not j["name"] or j["name"] in seen:
            continue
        seen.add(j["name"])
        dedup.append(j)
    return dedup


def list_jobs(timeout: float = 10.0) -> List[Dict[str, str]]:
    """Job inventory for the Obelisk native panel.

    The frontend pairs this with ``list_recent_runs`` to render per-job last-run
    status, so the backend deliberately keeps this endpoint cheap (a single
    ``repositoriesOrError`` call).
    """
    data = _graphql(_JOBS_QUERY, timeout=timeout)
    return _normalize_jobs_payload(data)


def list_schedules(timeout: float = 10.0) -> List[Dict[str, Any]]:
    """Active schedules, normalized for direct JSON return.

    Dagster 1.13: ``schedulesOrError`` requires a mandatory ``repositorySelector``
    argument — calling it without one returns a GraphQL error. We instead read
    schedules from ``repositoriesOrError → nodes[].schedules[]``, which needs
    no selector and returns the same data.

    Field shapes (verified against Dagster 1.13.26):
    - ``cronSchedule``      bare cron string (e.g. "0 0 * * *")
    - ``executionTimezone`` top-level string field
    - ``scheduleState``     object { status: "RUNNING"|"STOPPED"|"PAUSED" }
    """
    data = _graphql(_SCHEDULES_QUERY, timeout=timeout)
    conn = data.get("repositoriesOrError") or {}
    if conn.get("__typename") != "RepositoryConnection":
        return []
    out: List[Dict[str, Any]] = []
    for repo in conn.get("nodes") or []:
        for s in repo.get("schedules") or []:
            cron = s.get("cronSchedule") or ""
            tz = s.get("executionTimezone") or "UTC"
            state_obj = s.get("scheduleState") or {}
            if isinstance(state_obj, dict):
                state = (state_obj.get("status") or "STOPPED")
            else:
                state = str(state_obj) or "STOPPED"
            out.append({
                "id": s.get("id", ""),
                "name": s.get("name", ""),
                "cron": cron,
                "timezone": tz,
                "state": state,
                "description": s.get("description") or "",
            })
    return out


def _ms_to_iso(ts: Optional[float]) -> Optional[str]:
    """Convert Dagster run timestamp to ISO string.

    Dagster 1.x returns ``startTime``/``endTime`` as epoch **seconds** (float),
    not milliseconds. Values > 1e10 are treated as milliseconds for safety
    (guard against future schema drift).
    """
    if not ts:
        return None
    try:
        from datetime import datetime, timezone
        t = float(ts)
        # Dagster 1.x: epoch seconds. Values above 1e10 are ms (sanity guard).
        if t > 1e10:
            t = t / 1000.0
        return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
    except Exception:
        return None


def list_recent_runs(limit: int = 20, timeout: float = 10.0) -> List[Dict[str, Any]]:
    """Recent runs newest-first.

    ``startTime``/``endTime`` are epoch **seconds** (Dagster 1.x). We compute
    ``duration_seconds`` server-side so the panel renders without client math.

    When a run was triggered via the Dagster UI "Materialize" button, Dagster
    uses an internal system job called ``__ASSET_JOB``. In that case we resolve
    a human-readable label from ``assetSelection`` (e.g. ``designers``) and
    prefix it so the panel doesn't just show ``__ASSET_JOB``.
    """
    data = _graphql(_RUNS_QUERY, variables={"limit": int(limit)}, timeout=timeout)
    runs = data.get("runsOrError") or {}
    if runs.get("__typename") != "Runs":
        return []
    out: List[Dict[str, Any]] = []
    for r in runs.get("results") or []:
        ts_start = r.get("startTime") or None
        ts_end = r.get("endTime") or None
        dur: Optional[float] = None
        if ts_start and ts_end:
            try:
                # Dagster 1.x: timestamps are epoch seconds.
                # If values look like ms (> 1e10), convert to seconds first.
                s = float(ts_start)
                e = float(ts_end)
                if s > 1e10:
                    s, e = s / 1000.0, e / 1000.0
                dur = round(e - s, 3)
            except Exception:
                dur = None
        run_id = r.get("runId") or ""
        job_name = r.get("jobName") or ""

        # __ASSET_JOB is Dagster's internal system job for UI-triggered
        # "Materialize" clicks. Resolve the real asset name from assetSelection
        # so the panel shows something meaningful (e.g. "materialize: designers").
        if job_name == "__ASSET_JOB":
            asset_sel = r.get("assetSelection") or []
            if asset_sel:
                asset_names = ["/".join(a.get("path") or []) for a in asset_sel if a.get("path")]
                job_name = "materialize: " + ", ".join(asset_names) if asset_names else "__ASSET_JOB"

        out.append({
            "id": run_id,
            "job": job_name,
            "status": r.get("status", "UNKNOWN"),
            "started_at": _ms_to_iso(ts_start),
            "ended_at": _ms_to_iso(ts_end),
            "duration_seconds": dur,
            "dagster_url": f"http://{DAGSTER_HOST}:{DAGSTER_PORT}/runs/{run_id}",
        })
    return out


class DagsterClient:
    service_status = staticmethod(service_status)
    discover_workspace = staticmethod(discover_workspace)
    reload_workspace = staticmethod(reload_workspace)
    launch_job = staticmethod(launch_job)
    wait_for_run = staticmethod(wait_for_run)
    trigger_ingest = staticmethod(trigger_ingest)
    list_jobs = staticmethod(list_jobs)
    list_schedules = staticmethod(list_schedules)
    list_recent_runs = staticmethod(list_recent_runs)


dagster_client = DagsterClient()
