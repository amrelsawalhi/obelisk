"""
Unified PostgreSQL warehouse credential resolution.

Query Studio, the dbt runner, pipeline runs and the SDK all need the same answer
to "which warehouse do I write to, and with which credentials?". Resolution order:

  1. Explicit caller-supplied params.
  2. The deployed/connected `postgres` service (`service_configs`), whose password
     is vaulted in `config_json["encrypted_password"]`.
  3. The newest registered Postgres `connections` row (password vaulted in
     `encrypted_credentials`).
  4. Environment variables (DBT_PG_* / POSTGRES_*) or a reachable local test
     container (5434, 5433, 5432).
"""
import json
import os
import sqlite3
from typing import Any, Dict, Optional

from app.config import settings
from app.core.host_resolver import resolve_connection_host


def _decrypt(value: Optional[str]) -> str:
    """Decrypts a vaulted credential, returning "" when absent or unreadable."""
    if not value:
        return ""
    try:
        from app.core.vault import decrypt
        return decrypt(value)
    except Exception:
        return ""


def _load_json(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, (str, bytes)) else dict(raw)
    except Exception:
        return {}


def _from_service_config() -> Optional[Dict[str, Any]]:
    """Priority 2: the `postgres` row written by /api/services/deploy|connect."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            row = con.execute(
                "SELECT host, port, config_json FROM service_configs WHERE service_name = 'postgres' LIMIT 1"
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None

    host, port, raw_config = row
    config = _load_json(raw_config)
    password = _decrypt(config.get("encrypted_password"))
    # Rows written before credentials were configurable (e.g. a service connected
    # with only {"ssl": "require"}) carry no usable username/password — skip them
    # so resolution falls through to the catalog connection instead of returning a
    # credential-less target.
    if not (host and port and config.get("username") and password):
        return None
    return {
        "host": host,
        "port": int(port),
        "username": config["username"],
        "password": password,
        "database": config.get("database") or "obelisk",
        "sslmode": config.get("sslmode") or "disable",
    }


def _from_connections() -> Optional[Dict[str, Any]]:
    """Priority 3: newest registered Postgres Connection in the data catalog."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            row = con.execute(
                "SELECT host, port, database, username, encrypted_credentials, options "
                "FROM connections WHERE source_type = 'postgres' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None

    host, port, database, username, enc_pw, raw_options = row
    password = _decrypt(enc_pw)
    if not (host and port and username and password):
        return None

    options = _load_json(raw_options)
    return {
        "host": host,
        "port": int(port),
        "username": username,
        "password": password,
        "database": database or "obelisk",
        "sslmode": options.get("sslmode") or "disable",
    }


def _from_environment_or_local() -> Dict[str, Any]:
    """Priority 4: DBT_PG_*/POSTGRES_* env vars, else a reachable local test container."""
    env_host = os.environ.get("DBT_PG_HOST") or os.environ.get("POSTGRES_HOST")
    env_port = os.environ.get("DBT_PG_PORT") or os.environ.get("POSTGRES_PORT")
    env_user = os.environ.get("DBT_PG_USER") or os.environ.get("POSTGRES_USER")
    env_pw = os.environ.get("DBT_PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD")
    env_db = os.environ.get("DBT_PG_DBNAME") or os.environ.get("POSTGRES_DB")

    test_ports = [5434, 5433, 5432]
    if env_port:
        try:
            candidate_port = int(env_port)
            if candidate_port not in test_ports:
                test_ports.insert(0, candidate_port)
        except Exception:
            pass

    for candidate_port in test_ports:
        try:
            resolved = resolve_connection_host("127.0.0.1", candidate_port)
            import psycopg2
            conn = psycopg2.connect(
                host=resolved,
                port=candidate_port,
                user=env_user or "testuser",
                password=env_pw or "testpass",
                dbname=env_db or "sourcedb",
                connect_timeout=1,
            )
            conn.close()
            return {
                "host": resolved,
                "port": candidate_port,
                "username": env_user or "testuser",
                "password": env_pw or "testpass",
                "database": env_db or "sourcedb",
                "sslmode": "disable",
            }
        except Exception:
            continue

    return {
        "host": env_host or "127.0.0.1",
        "port": int(env_port or 5434),
        "username": env_user or "testuser",
        "password": env_pw or "testpass",
        "database": env_db or "sourcedb",
        "sslmode": "disable",
    }


def resolve_warehouse_postgres_params(custom_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resolves PostgreSQL warehouse params, highest-priority source first."""
    if custom_params:
        return custom_params
    for source in (_from_service_config, _from_connections):
        params = source()
        if params:
            return params
    return _from_environment_or_local()
