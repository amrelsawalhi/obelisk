import os
import sys
import ast
import atexit
import shutil
import subprocess
import tempfile
from pathlib import Path
import pytest
import docker
import aiosqlite

# Ensure backend root is on sys.path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# git_repo/ must be importable so tests can exercise the in-process Dagster
# workspace (`dagster_workspace.repository`) exactly as app/main.py does at
# runtime; without it the ingest fallback raises ModuleNotFoundError.
git_repo_dir = backend_dir.parent / "git_repo"
if git_repo_dir.is_dir() and str(git_repo_dir) not in sys.path:
    sys.path.insert(0, str(git_repo_dir))

# Configure test environment.
#
# STORAGE_URI must never point at the operator's real lakehouse: the suite
# writes and deletes Parquet files, and the previous value
# ("file:///data/lakehouse/bronze") made every tier-aware caller append another
# /bronze, leaving nested data/lakehouse/bronze/bronze/ directories behind.
# Each test session gets a private temporary lakehouse root instead.
_TEST_STORAGE_ROOT = Path(tempfile.mkdtemp(prefix="obelisk-test-lakehouse-"))
atexit.register(shutil.rmtree, _TEST_STORAGE_ROOT, ignore_errors=True)
os.environ["STORAGE_URI"] = _TEST_STORAGE_ROOT.as_uri()

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///data/obelisk.db")
os.environ.setdefault("OBELISK_MASTER_KEY", "dGhpcy1pcy1hLXNhbXBsZS0zMi1ieXRlLWtleS0xMjM0NTY=")

# On Windows, configure DOCKER_HOST if needed so docker.from_env() reaches WSL daemon
if os.name == "nt" and "DOCKER_HOST" not in os.environ:
    try:
        out = subprocess.check_output(["wsl", "hostname", "-I"], text=True).strip()
        wsl_ip = out.split()[0]
        os.environ["DOCKER_HOST"] = f"tcp://{wsl_ip}:2375"
    except Exception:
        pass

@pytest.fixture(scope="session", autouse=True)
def enforce_anti_mock_ast_check():
    """
    Mandatory Anti-Cheat Meta-Check:
    Scans all .py files in backend/app/ and backend/tests/ using AST parsing.
    Fails immediately if any file imports unittest.mock, MagicMock, AsyncMock,
    pytest_mock, or uses monkeypatch.
    """
    forbidden_modules = {"unittest.mock", "pytest_mock", "mock"}
    forbidden_names = {"MagicMock", "AsyncMock", "Mock", "patch", "monkeypatch"}

    dirs_to_scan = [
        backend_dir / "app",
        backend_dir / "tests",
    ]

    for scan_dir in dirs_to_scan:
        if not scan_dir.exists():
            continue
        for py_file in scan_dir.rglob("*.py"):
            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            try:
                tree = ast.parse(content, filename=str(py_file))
            except SyntaxError as e:
                pytest.fail(f"Syntax error parsing {py_file}: {e}")

            for node in ast.walk(tree):
                # Check import ...
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        for forbidden in forbidden_modules:
                            if alias.name == forbidden or alias.name.startswith(f"{forbidden}."):
                                pytest.fail(
                                    f"ANTI-MOCK VIOLATION in {py_file.name}: "
                                    f"Import of forbidden mock module '{alias.name}'. Mocking is strictly forbidden."
                                )

                # Check from ... import ...
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        for forbidden in forbidden_modules:
                            if node.module == forbidden or node.module.startswith(f"{forbidden}."):
                                pytest.fail(
                                    f"ANTI-MOCK VIOLATION in {py_file.name}: "
                                    f"Import from forbidden mock module '{node.module}'. Mocking is strictly forbidden."
                                )
                    for alias in node.names:
                        if alias.name in forbidden_names:
                            pytest.fail(
                                f"ANTI-MOCK VIOLATION in {py_file.name}: "
                                f"Import of forbidden mock symbol '{alias.name}'. Mocking is strictly forbidden."
                            )

                # Check for monkeypatch fixture parameter in test functions
                elif isinstance(node, ast.FunctionDef):
                    for arg in node.args.args:
                        if arg.arg == "monkeypatch":
                            pytest.fail(
                                f"ANTI-MOCK VIOLATION in {py_file.name}: "
                                f"Use of 'monkeypatch' fixture in function '{node.name}'. Real environment testing is required."
                            )

@pytest.fixture(scope="session", autouse=True)
def prepare_test_db():
    """Initializes SQLite database before test execution."""
    from app.database import init_db
    import asyncio

    # Ensure data directory exists
    Path("data").mkdir(parents=True, exist_ok=True)
    asyncio.run(init_db())
    yield


@pytest.fixture(autouse=True)
def cleanup_test_artifacts():
    """
    Automatically cleans up any temporary test parquet files and test SQLite rows
    created during each test execution so the user's production lakehouse is never polluted.
    """
    yield
    from app.config import settings
    import sqlite3

    # A test may deliberately set an unservable STORAGE_URI (e.g. s3://) to
    # prove it is rejected. Resolution then raises by design, and that must not
    # turn every later teardown into an error.
    try:
        storage_path = Path(settings.duckdb_storage_path)
    except Exception:
        storage_path = None

    if storage_path is not None and storage_path.exists():
        for f in storage_path.glob("*.parquet"):
            if f.name.startswith((
                "bronze_api_orders_",
                "bronze_dates_",
                "bronze_nulls_",
                "bronze_tasks_",
                "bronze_test_",
                "bronze_asset_",
                "sdk_orders_",
                "test_copy",
            )):
                try:
                    f.unlink(missing_ok=True)
                except Exception:
                    pass

    db_path = Path(settings.sqlite_db_path)
    if db_path.exists():
        try:
            with sqlite3.connect(str(db_path), timeout=5.0) as con:
                cur = con.cursor()
                cur.execute("DELETE FROM assets WHERE name LIKE 'bronze_%' OR name LIKE 'test_%' OR name LIKE 'sdk_%'")
                cur.execute("DELETE FROM connections WHERE name LIKE 'test_%'")
                con.commit()
        except Exception:
            pass



def get_docker_host_ip() -> str:
    """Returns the host/IP where Docker container mapped ports are reachable."""
    if os.name == "nt":
        docker_host = os.environ.get("DOCKER_HOST", "")
        if docker_host.startswith("tcp://"):
            return docker_host.split("://")[1].split(":")[0]
        try:
            out = subprocess.check_output(["wsl", "hostname", "-I"], text=True).strip()
            return out.split()[0]
        except Exception:
            pass
    return "127.0.0.1"


@pytest.fixture(scope="session")
def source_host() -> str:
    return get_docker_host_ip()


@pytest.fixture(scope="session")
def source_postgres_params(source_host) -> dict:
    import psycopg2
    # 1. First attempt connecting with default test container credentials on port 5434 or 5433
    for test_port in (5434, 5433):
        for test_host in ("127.0.0.1", source_host):
            default_params = {
                "host": test_host,
                "port": test_port,
                "user": "testuser",
                "password": "testpass",
                "database": "sourcedb",
                "sslmode": "disable",
            }
            try:
                c = psycopg2.connect(
                    host=default_params["host"],
                    port=default_params["port"],
                    user=default_params["user"],
                    password=default_params["password"],
                    dbname=default_params["database"],
                    connect_timeout=2,
                )
                c.close()
                return default_params
            except Exception:
                pass

    # 2. Check vault connections in the canonical SQLite brain
    import sqlite3
    from app.config import settings
    from app.core.vault import decrypt
    db_path = Path(settings.sqlite_db_path)
    if db_path.exists():
        try:
            with sqlite3.connect(str(db_path)) as con:
                row = con.execute(
                    "SELECT host, port, database, username, encrypted_credentials, options FROM connections WHERE source_type = 'postgres' ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    host, port, dbname, user, enc_pw, opts = row
                    pw = decrypt(enc_pw)
                    vault_params = {
                        "host": host,
                        "port": port,
                        "user": user,
                        "password": pw,
                        "database": dbname,
                        "sslmode": "disable",
                    }
                    c = psycopg2.connect(
                        host=vault_params["host"],
                        port=vault_params["port"],
                        user=vault_params["user"],
                        password=vault_params["password"],
                        dbname=vault_params["database"],
                        connect_timeout=2,
                    )
                    c.close()
                    return vault_params
        except Exception:
            pass

    # 3. If no PostgreSQL instance is reachable, skip gracefully
    pytest.skip("No real PostgreSQL instance reachable on port 5433")


@pytest.fixture(scope="session")
def source_postgres_dsn(source_postgres_params) -> str:
    p = source_postgres_params
    return f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['database']}?sslmode={p['sslmode']}"


@pytest.fixture(scope="session")
def source_mongo_params(source_host) -> dict:
    import pymongo
    default_params = {
        "host": source_host,
        "port": 27018,
        "user": "mongoadmin",
        "password": "mongopass",
        "database": "sourcedb",
        "auth_source": "admin",
    }
    try:
        client = pymongo.MongoClient(
            f"mongodb://{default_params['user']}:{default_params['password']}@{default_params['host']}:{default_params['port']}/{default_params['database']}?authSource={default_params['auth_source']}",
            serverSelectionTimeoutMS=2000,
        )
        client.admin.command("ping")
        return default_params
    except Exception:
        pytest.skip("No real MongoDB instance reachable on port 27018")


@pytest.fixture(scope="session")
def source_mongo_uri(source_mongo_params) -> str:
    p = source_mongo_params
    return f"mongodb://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['database']}?authSource={p['auth_source']}"

