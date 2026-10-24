"""Finds blocking calls inside ``async def`` handlers in ``app/api``.

A FastAPI ``async def`` route runs on the event loop. A psycopg2 round trip, a DuckDB
scan, a subprocess or a Docker API call inside one stalls every other request for its
duration — including the health check. Sync ``def`` routes are executed in the
threadpool by FastAPI, so blocking work there is fine, and anything handed to
``asyncio.to_thread`` (or a ``run_in_executor`` call) is off the loop too.

Usage:
    python scripts/find_blocking_calls.py           # human-readable report
    python scripts/find_blocking_calls.py --json    # machine-readable

Exit code 1 when anything is found, so it can gate a build.
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Iterable

API_DIR = Path(__file__).resolve().parent.parent / "app" / "api"

# Import roots whose calls block. Anything reached through one of these does real I/O.
BLOCKING_MODULES = {
    "psycopg2",
    "pymongo",
    "pymysql",
    "oracledb",
    "duckdb",
    "sqlite3",
    "subprocess",
    "docker",
    "shutil",
    "socket",
    "requests",
    "pyspark",
    "psutil",
    # Synchronous engine objects imported by the routes. These are the ones a plain
    # `import` check misses: the call looks like a method, but it scans Parquet,
    # shells out to dbt, or talks to the Docker daemon on the calling thread.
    "duckdb_engine",
    "native_query_engine",
    "ingest_engine",
    "dbt_runner",
    "docker_mgr",
    "process_manager",
    "marimo_service",
    "cron_engine",
    "introspector",
    "dagster_client",
}

# Attribute calls that block regardless of the module they hang off.
BLOCKING_ATTRS = {
    "rglob",
    "glob",
    "iterdir",
    "read_text",
    "read_bytes",
    "write_text",
    "write_bytes",
    "unlink",
    "mkdir",
    "rmtree",
    "walk",
    "listdir",
    "scandir",
    "system",
    "popen",
}

# Functions that hand work to another thread; calls inside them are fine.
OFFLOADERS = {"to_thread", "run_in_executor", "run_in_threadpool"}

# Pure computation with no I/O: croniter evaluating an expression against "now". A
# thread hop would cost more than the call it is meant to protect.
ALLOWED_CALLS = {"cron_engine.compute_next_run"}


def _call_name(node: ast.Call) -> str:
    """Dotted name of a call target, e.g. ``psycopg2.connect`` or ``Path.read_text``."""
    parts: list[str] = []
    current: ast.expr = node.func
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _offloaded_lines(function: ast.AST) -> list[tuple[int, int]]:
    """Line ranges covered by an offloading call, including its arguments."""
    spans = []
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        if _call_name(node).split(".")[-1] not in OFFLOADERS:
            continue
        start = getattr(node, "lineno", None)
        end = getattr(node, "end_lineno", start)
        if start is not None:
            spans.append((start, end))
    return spans


def _is_blocking(call: ast.Call) -> bool:
    name = _call_name(call)
    if not name:
        return False
    if name in ALLOWED_CALLS:
        return False

    root = name.split(".")[0]
    if root in BLOCKING_MODULES:
        return True
    if name == "open":
        return True
    if root == "time" and name.endswith(".sleep"):
        return True
    if name.split(".")[-1] in BLOCKING_ATTRS:
        return True
    return False


def scan_file(path: Path) -> list[dict]:
    """Returns one finding per blocking call inside an ``async def``."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:  # a broken file is a different problem
        return [{"file": str(path), "line": exc.lineno or 0, "function": "<syntax>",
                 "call": f"syntax error: {exc.msg}"}]

    findings = []
    for function in [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)]:
        spans = _offloaded_lines(function)
        # A call that is itself awaited returns control to the loop, even if it hangs
        # off a blocking-looking object: `await cron_engine.run_job(...)`.
        awaited = {
            id(node.value) for node in ast.walk(function) if isinstance(node, ast.Await)
        }
        for node in ast.walk(function):
            if not isinstance(node, ast.Call) or not _is_blocking(node):
                continue
            if id(node) in awaited:
                continue
            line = getattr(node, "lineno", 0)
            if any(start <= line <= end for start, end in spans):
                continue
            findings.append({
                "file": path.name,
                "line": line,
                "function": function.name,
                "call": _call_name(node),
            })
    return findings


def scan(directory: Path = API_DIR) -> list[dict]:
    findings: list[dict] = []
    for path in sorted(directory.rglob("*.py")):
        findings.extend(scan_file(path))
    return findings


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(list(argv) if argv is not None else None)

    findings = scan()
    if args.as_json:
        print(json.dumps(findings, indent=2))
    else:
        for finding in findings:
            print(
                f"{finding['file']}:{finding['line']}  {finding['function']}()  "
                f"-> {finding['call']}"
            )
        print(f"\n{len(findings)} blocking call(s) on the event loop.")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
