"""
PostgreSQL warehouse credentials, resolved from a *choice*, not from a guess.

Query Studio, the dbt runner, pipeline runs and the SDK all need the same answer to
"which warehouse do I write to, and with which credentials?". The answer is one
connection the operator selected (Settings → Warehouse, stored in ``app_settings``), or
credentials a caller supplied explicitly.

What used to happen instead: the resolver walked a priority list, and its third entry was
"the newest Postgres connection by ``created_at``". Nobody could see that decision, so
with two Postgres sources a dbt run published marts into whichever row happened to be
created last. There is no ranking here any more — an unbound warehouse is an error that
names the fix.

Reads are different and stay permissive: Query Studio federates across read-only sources,
which are never write targets.
"""
import json
import os
import sqlite3
from typing import Any, Dict, List, Optional

from app.config import settings
from app.core.host_resolver import resolve_connection_host

WAREHOUSE_SETTING = "warehouse_connection_id"


class WarehouseReadOnly(RuntimeError):
    """Raised when the selected warehouse is a read-only connection.

    A read-only connection is a *source*: DuckDB reads it live and Obelisk never writes
    back. Publishing dbt models or a DataFrame mirror into it would create schemas
    inside a database the operator explicitly protected, so the write paths refuse.
    """


class WarehouseNotSelected(RuntimeError):
    """Raised when a write needs a warehouse and none has been chosen."""


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
    """A JSON object column, always a dict.

    ``json.loads("null")`` succeeds and returns ``None``, which used to reach callers
    that immediately call ``.get()`` on it — one connection row written with an explicit
    JSON null was enough to crash warehouse resolution.
    """
    if not raw:
        return {}
    try:
        value = json.loads(raw) if isinstance(raw, (str, bytes)) else dict(raw)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def get_warehouse_connection_id() -> Optional[str]:
    """The connection the operator chose as the warehouse, if any."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            row = con.execute(
                "SELECT value FROM app_settings WHERE key = ?", (WAREHOUSE_SETTING,)
            ).fetchone()
    except Exception:
        return None
    value = (row[0] if row else "") or ""
    return value.strip() or None


def set_warehouse_connection_id(connection_id: Optional[str]) -> None:
    """Stores (or clears) the warehouse choice. An empty value means "not chosen"."""
    value = (connection_id or "").strip()
    with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
        if not value:
            con.execute("DELETE FROM app_settings WHERE key = ?", (WAREHOUSE_SETTING,))
        else:
            con.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (WAREHOUSE_SETTING, value),
            )
        con.commit()


def _postgres_row(connection_id: str) -> Optional[Dict[str, Any]]:
    """One Postgres connection by id, with its access policy attached."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            row = con.execute(
                "SELECT name, host, port, database, username, encrypted_credentials, "
                "options, read_only FROM connections WHERE id = ? "
                "AND lower(source_type) IN ('postgres', 'postgresql')",
                (connection_id,),
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None

    name, host, port, database, username, enc_pw, raw_options, read_only = row
    password = _decrypt(enc_pw)
    if not (host and port and username and password):
        return None

    options = _load_json(raw_options)
    return {
        "id": connection_id,
        "name": name,
        "host": host,
        "port": int(port),
        "username": username,
        "password": password,
        "database": database or "obelisk",
        "sslmode": options.get("sslmode") or "disable",
        "read_only": bool(read_only),
    }


def warehouse_candidates() -> List[Dict[str, Any]]:
    """Every Postgres connection that could be the warehouse, for a picker."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            rows = con.execute(
                "SELECT id, name, host, port, database, read_only, service_name "
                "FROM connections WHERE lower(source_type) IN ('postgres', 'postgresql') "
                "ORDER BY name"
            ).fetchall()
    except Exception:
        return []
    return [
        {
            "id": row[0],
            "name": row[1],
            "host": row[2],
            "port": row[3],
            "database": row[4],
            "read_only": bool(row[5]),
            "service_name": row[6],
        }
        for row in rows
    ]


def warehouse_candidate(connection_id: str) -> Optional[Dict[str, Any]]:
    """One candidate by id, or ``None`` when that id is not a Postgres connection."""
    row = _postgres_row(connection_id)
    if not row:
        return None
    return {"id": row["id"], "name": row["name"], "read_only": row["read_only"]}


def _newest_postgres_connection() -> Optional[Dict[str, Any]]:
    """Newest registered Postgres connection, with the row's own policy.

    Reads only. Publishing never uses this: "newest" is not a decision the operator
    made, so the write resolver refuses instead of ranking.
    """
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=3.0) as con:
            row = con.execute(
                "SELECT id, name, host, port, database, username, encrypted_credentials, "
                "options, read_only FROM connections "
                "WHERE lower(source_type) IN ('postgres', 'postgresql') "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None

    cid, name, host, port, database, username, enc_pw, raw_options, read_only = row
    password = _decrypt(enc_pw)
    if not (host and port and username and password):
        return None

    options = _load_json(raw_options)
    return {
        "id": cid,
        "name": name,
        "host": host,
        "port": int(port),
        "username": username,
        "password": password,
        "database": database or "obelisk",
        "sslmode": options.get("sslmode") or "disable",
        "read_only": bool(read_only),
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


def _from_environment() -> Optional[Dict[str, Any]]:
    """Credentials the operator set explicitly in the environment, if any.

    Only an explicit host counts. The fabricated ``testuser@127.0.0.1:5434`` default is
    not a choice, so it is not consulted for writes.
    """
    host = os.environ.get("DBT_PG_HOST") or os.environ.get("POSTGRES_HOST")
    if not host:
        return None
    port = os.environ.get("DBT_PG_PORT") or os.environ.get("POSTGRES_PORT") or "5432"
    return {
        "host": resolve_connection_host(host, int(port)),
        "port": int(port),
        "username": os.environ.get("DBT_PG_USER") or os.environ.get("POSTGRES_USER") or "postgres",
        "password": os.environ.get("DBT_PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD") or "",
        "database": os.environ.get("DBT_PG_DBNAME") or os.environ.get("POSTGRES_DB") or "postgres",
        "sslmode": "disable",
    }


def _credentials_only(row: Dict[str, Any]) -> Dict[str, Any]:
    """Strip the bookkeeping fields; callers pass this straight to a driver."""
    return {k: v for k, v in row.items() if k not in ("id", "name", "read_only")}


def _public(params: Dict[str, Any]) -> Dict[str, Any]:
    """The parts of a resolved target that are safe to show in the Studio."""
    return {k: v for k, v in params.items() if k != "password"}


def describe_warehouse_postgres() -> Dict[str, Any]:
    """The selected warehouse, with enough context for the Studio to be honest.

    ``read_only`` and ``message`` exist so the UI can disable publishing *before* the
    operator discovers it from a failed build, instead of showing a green tick over a
    target nothing can write to.
    """
    selected = get_warehouse_connection_id()
    if selected:
        row = _postgres_row(selected)
        if row:
            return {"source": "selection", **_public(row)}
        return {
            "source": "selection",
            "read_only": True,
            "message": "The selected warehouse connection no longer exists. Choose one in Settings → Warehouse.",
        }
    env = _from_environment()
    if env:
        return {"source": "environment", "read_only": False, **_public(env)}
    return {
        "source": "unset",
        "read_only": True,
        "message": "No warehouse selected. Choose one in Settings → Warehouse.",
    }


def resolve_warehouse_postgres_params(
    custom_params: Optional[Dict[str, Any]] = None,
    connection_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Credentials for *reads*: the selection, then any Postgres source, then env.

    Kept permissive on purpose — Query Studio federates across read-only connections,
    and a read has no side effect to get wrong.
    """
    if custom_params:
        return custom_params
    for candidate in (connection_id, get_warehouse_connection_id()):
        if candidate:
            row = _postgres_row(candidate)
            if row:
                return _credentials_only(row)
    newest = _newest_postgres_connection()
    if newest:
        return _credentials_only(newest)
    return _from_environment_or_local()


def resolve_write_warehouse_postgres_params(
    custom_params: Optional[Dict[str, Any]] = None,
    connection_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Credentials for **publishing**, or a refusal that says what to fix.

    Resolution is a decision, never a ranking: the connection named by the caller, else
    the one the operator selected, else explicitly-set environment credentials. When none
    of those exists this raises instead of picking a Postgres at random.
    """
    if custom_params:
        return custom_params

    selected = (connection_id or "").strip() or get_warehouse_connection_id()
    if selected:
        row = _postgres_row(selected)
        if not row:
            raise WarehouseNotSelected(
                "The selected warehouse connection no longer exists. Choose one in "
                "Settings → Warehouse."
            )
        if row["read_only"]:
            raise WarehouseReadOnly(
                f"Connection {row['name']!r} is read-only, so Obelisk will not write into "
                "it. Mark it writable in Settings → Connections, or choose another "
                "warehouse in Settings → Warehouse."
            )
        return _credentials_only(row)

    env = _from_environment()
    if env:
        return env

    raise WarehouseNotSelected(
        "No warehouse selected. Choose the connection dbt marts are published into in "
        "Settings → Warehouse."
    )
