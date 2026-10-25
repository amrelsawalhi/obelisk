# Testing

## The zero-mock discipline

There are no mocks in this suite, and the suite enforces that on itself: an autouse fixture
in `tests/conftest.py` parses every file under `backend/app` and `backend/tests` with `ast`
and fails the session if it finds `unittest.mock`, `pytest_mock`, `MagicMock`, `AsyncMock`,
`Mock`, `patch` or a `monkeypatch` parameter.

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
# 210 passed, 2 skipped when this was written
```

On Windows the two skips are usually the Docker-container tests and the Spark path (no JVM);
on a Linux host with Docker and a JDK they run.

## Orchestrator harness

Builds real pipelines from the shipped sample assets -- Python scripts, a notebook, dbt, raw
SQL and a webhook -- runs them against a live backend and prints a pass/fail report:

```bash
python backend/scripts/verify_pipelines.py --base-url http://127.0.0.1:8000
```

## End-to-end harness

Seeds real sources, drives the real ingest engine, runs a pipeline that uses every step type,
forces every edge condition, and then reconciles the lakehouse against the sources. No
expected number is hardcoded: the harness reads the sources and asks whether the warehouse
agrees.

```bash
python backend/scripts/e2e/verify_e2e.py --scale small
python backend/scripts/e2e/verify_e2e.py --scale medium --skip-seed
```

| Flag | Meaning |
| :--- | :--- |
| `--scale small\|medium\|large` | Seed size, see below. Default `small`. |
| `--skip-seed` | Reuse what the sources already contain. |
| `--keep` | Leave the scratch pipelines behind for inspection (they are deleted otherwise). |

| Scale | Orders | Customers | Events | Sessions |
| :--- | ---: | ---: | ---: | ---: |
| `small` | 20,000 | 2,000 | 50,000 | 10,000 |
| `medium` | 200,000 | 10,000 | 500,000 | 100,000 |
| `large` | 2,000,000 | 50,000 | 5,000,000 | 500,000 |

It needs a running backend, the demo sources (`docker compose -f
docker-compose.sources.yml up -d`) and two connections in the workspace vault: an
attachable Postgres and a MongoDB. Credentials are read from the vault, never from the
script.

Every pipeline it creates is named `E2E scratch - ...`, and the harness only ever deletes
pipelines inside that namespace, so a demo or a hand-built pipeline can never be removed by
a test run.

## Demo pipelines

```bash
python backend/scripts/create_demo_pipelines.py          # create the set
python backend/scripts/create_demo_pipelines.py --run    # create and run each once
python backend/scripts/create_demo_pipelines.py --delete # remove them
```

Six pipelines covering every step type and every edge condition, idempotent by name.