"""No blocking calls on the event loop (audit H7).

A FastAPI ``async def`` route runs on the loop, so a DuckDB scan, a dbt subprocess or a
pathological directory walk inside one delays every other request — including the
health check that a load balancer is watching. These tests run the same detector the
CLI uses, plus two synthetic files proving it can still fail.
"""
import importlib.util
import textwrap
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "find_blocking_calls.py"


def _load_scanner():
    spec = importlib.util.spec_from_file_location("find_blocking_calls", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_async_routes_do_not_block() -> None:
    scanner = _load_scanner()
    findings = scanner.scan()

    report = "\n".join(
        f"{finding['file']}:{finding['line']}  {finding['function']}()  -> {finding['call']}"
        for finding in findings
    )
    assert findings == [], (
        "blocking calls on the event loop — wrap them in asyncio.to_thread or make the "
        f"handler a plain def:\n{report}"
    )


def test_scanner_flags_a_blocking_call(tmp_path: Path) -> None:
    scanner = _load_scanner()
    sample = tmp_path / "routes_sample.py"
    sample.write_text(
        "import duckdb\n\n\nasync def handler():\n    return duckdb.connect(':memory:')\n",
        encoding="utf-8",
    )

    findings = scanner.scan_file(sample)

    assert [finding["call"] for finding in findings] == ["duckdb.connect"]
    assert findings[0]["function"] == "handler"


def test_scanner_ignores_offloaded_calls_and_sync_handlers(tmp_path: Path) -> None:
    scanner = _load_scanner()
    sample = tmp_path / "routes_sample.py"
    sample.write_text(
        textwrap.dedent(
            """
            import asyncio
            import duckdb


            async def offloaded():
                return await asyncio.to_thread(duckdb.connect, ':memory:')


            def sync_handler():
                # FastAPI runs plain `def` routes in the threadpool.
                return duckdb.connect(':memory:')
            """
        ),
        encoding="utf-8",
    )

    assert scanner.scan_file(sample) == []


def test_scanner_ignores_pure_computation(tmp_path: Path) -> None:
    """`compute_next_run` parses a cron expression; a thread hop would cost more."""
    scanner = _load_scanner()
    sample = tmp_path / "routes_sample.py"
    sample.write_text(
        textwrap.dedent(
            """
            async def handler():
                from app.engines.cron_engine import cron_engine
                return cron_engine.compute_next_run("* * * * *")
            """
        ),
        encoding="utf-8",
    )

    assert scanner.scan_file(sample) == []
