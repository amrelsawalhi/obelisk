# Testing

## The zero-mock discipline

There are no mocks in this suite, and the suite enforces that on itself: an autouse fixture
in `tests/conftest.py` parses every file under `backend/app` and `backend/tests` with `ast`
and fails the session if it finds `unittest.mock`, `pytest_mock`, `MagicMock`, `AsyncMock`,
`Mock`, `patch` or a `monkeypatch` parameter in either synchronous or asynchronous test functions.

So the tests exercise real DuckDB relations, real Parquet files, a real SQLite database, the
real FastAPI app and real child processes. When a dependency genuinely is not available on
the machine (a Docker daemon, a JDK, a Postgres), the test **skips** rather than pretending.

## What the suite isolates

The suite never touches your data. On start it points both stateful settings at throwaway
directories:

| Setting | Test value | Why |
| :--- | :--- | :--- |
| `DATABASE_URL` | a temp `obelisk.db`, migrated on first use | Your pipelines, runs and connections stay untouched. |
| `STORAGE_URI` | a temp lakehouse root | Tests write and delete Parquet; the real warehouse is never in the blast radius. |

Both directories are removed at exit. Tests that create files in `git_repo/` (the dbt Studio
lifecycle test) delete what they created.

## Running the suite

```bash
cd backend
python -m pytest tests -q
# 470 passed, 15 skipped across 485 collected tests
```

On Windows the two skips are usually the Docker-container tests and the Spark path (no JVM);
on a Linux host with Docker and a JDK they run.

## Orchestrator and API harnesses

The orchestrator and API are exercised by the suite itself
(`tests/test_pipeline_orchestrator.py`, `tests/test_pipeline_api.py`,
`tests/test_pipeline_outputs.py`): real pipelines, real child processes, real SQLite
state, real Parquet. There is no separate `backend/scripts/verify_pipelines.py` — it was
removed in `7f0126c` when the Scripts Studio replaced it, and this document used to
point at it anyway.

## Diagnostics and maintenance scripts

```bash
python backend/scripts/find_blocking_calls.py              # AST sweep: nothing blocking on the event loop
python backend/scripts/preflight_port.py --port 8000       # is the port free, and who holds it
python backend/scripts/lakehouse_maintenance.py --merge    # compact adjacent files (see --help)
```

## Demo pipelines

Demo pipelines are created from the **Scripts Studio** in the UI, or through
`POST /api/scripts`. The standalone `create_demo_pipelines.py` generator was removed in
`7f0126c` along with the `backend/scripts/e2e/` harness; both are gone from this
checkout, and nothing in the suite depends on them.