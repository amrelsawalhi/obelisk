import re
import time
import uuid
from typing import Dict, Any, List, Optional
import psycopg2
import psycopg2.extras
from psycopg2 import extensions
import pymysql
import pymysql.cursors

from app.core.host_resolver import resolve_connection_host

_READ_ONLY_SQL_RE = re.compile(
    r"^\s*(?:--[^\n]*\n|/\*.*?\*/|\s)*(SELECT|WITH|VALUES|TABLE)\b",
    re.IGNORECASE | re.DOTALL,
)


def _json_serializable(val: Any) -> Any:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if hasattr(val, "__float__"):
        try:
            return float(val)
        except Exception:
            pass
    if isinstance(val, (int, float, bool, str, list, dict)):
        return val
    return str(val)


def _fetch_bounded(cur, max_rows: Optional[int]) -> tuple[List[Any], bool]:
    """Reads at most ``max_rows`` rows, and one extra to know whether more exist."""
    if not max_rows or max_rows <= 0:
        return cur.fetchall(), False
    rows = cur.fetchmany(max_rows + 1)
    if len(rows) > max_rows:
        return rows[:max_rows], True
    return rows, False


class NativeQueryEngine:
    """
    Executes native SQL directly against operational relational and analytical databases
    (PostgreSQL, MySQL, ClickHouse) with zero mocks.
    Supports procedural SQL (PL/pgSQL), Stored Procedures, Functions, Materialized Views,
    DDL, DML, and complex analytics.
    """

    @staticmethod
    def execute_postgres(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        sql: str,
        schema_name: Optional[str] = None,
        timeout_seconds: int = 30,
        sslmode: str = "disable",
        max_rows: Optional[int] = None,
        on_connection: Optional[Any] = None,
    ) -> Dict[str, Any]:
        resolved_host = resolve_connection_host(host, port)
        t0 = time.perf_counter()
        clean_sql = (sql or "").strip()
        # Use a server-side named cursor when max_rows > 0 and the query is a single read-only
        # SELECT/WITH/VALUES statement so libpq streams only max_rows + 1 tuples rather than
        # buffering the entire table in client C heap inside cur.execute().
        stripped_body = clean_sql.rstrip(";").strip()
        use_server_cursor = bool(
            max_rows
            and max_rows > 0
            and _READ_ONLY_SQL_RE.match(clean_sql)
            and ";" not in stripped_body
        )

        conn = psycopg2.connect(
            host=resolved_host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            sslmode=sslmode,
            connect_timeout=min(timeout_seconds, 10),
            options=f"-c statement_timeout={int(timeout_seconds * 1000)}",
        )
        conn.autocommit = not use_server_cursor

        if on_connection is not None:
            on_connection(conn)

        try:
            if schema_name and schema_name.strip():
                clean_schema = schema_name.strip().replace('"', '""')
                with conn.cursor() as setup_cur:
                    setup_cur.execute(f'SET search_path TO "{clean_schema}", public;')

            cursor_name = f"obelisk_ss_{uuid.uuid4().hex[:10]}" if use_server_cursor else None
            with conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if use_server_cursor:
                    cur.itersize = min(max_rows + 1, 2000)
                cur.execute(sql)
                duration_ms = int((time.perf_counter() - t0) * 1000)
                status_msg = cur.statusmessage or ("SELECT" if use_server_cursor else "OK")

                if use_server_cursor or cur.description:
                    raw_rows, truncated = _fetch_bounded(cur, max_rows)
                    cols = [d[0] for d in (cur.description or [])]
                    rows: List[Dict[str, Any]] = [
                        {col: _json_serializable(r.get(col)) for col in cols}
                        for r in raw_rows
                    ]
                    if not conn.autocommit:
                        conn.commit()
                    return {
                        "columns": cols,
                        "rows": rows,
                        "row_count": len(rows),
                        "truncated": truncated,
                        "duration_ms": max(duration_ms, 1),
                        "status": "success",
                        "command_tag": status_msg,
                    }
                else:
                    row_count = cur.rowcount if cur.rowcount >= 0 else 0
                    if not conn.autocommit:
                        conn.commit()
                    return {
                        "columns": [],
                        "rows": [],
                        "row_count": row_count,
                        "truncated": False,
                        "duration_ms": max(duration_ms, 1),
                        "status": "success",
                        "command_tag": status_msg,
                    }
        finally:
            conn.close()

    @staticmethod
    def get_postgres_schemas(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        sslmode: str = "disable",
    ) -> List[str]:
        resolved_host = resolve_connection_host(host, port)
        conn = psycopg2.connect(
            host=resolved_host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            sslmode=sslmode,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                      AND schema_name NOT LIKE 'pg_temp_%'
                      AND schema_name NOT LIKE 'pg_toast_temp_%'
                    ORDER BY schema_name;
                """)
                rows = cur.fetchall()
                schemas = [r[0] for r in rows]
                if "public" not in schemas:
                    schemas.insert(0, "public")
                return schemas
        except Exception:
            return ["public"]
        finally:
            conn.close()

    @staticmethod
    def execute_mysql(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        sql: str,
        timeout_seconds: int = 30,
        max_rows: Optional[int] = None,
        on_connection: Optional[Any] = None,
    ) -> Dict[str, Any]:
        resolved_host = resolve_connection_host(host, port)
        t0 = time.perf_counter()

        conn = pymysql.connect(
            host=resolved_host,
            port=port,
            database=database,
            user=username,
            password=password,
            connect_timeout=min(timeout_seconds, 10),
            cursorclass=pymysql.cursors.SSCursor,
            autocommit=True,
        )
        if on_connection is not None:
            on_connection(conn)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                duration_ms = int((time.perf_counter() - t0) * 1000)

                if cur.description:
                    raw_rows, truncated = _fetch_bounded(cur, max_rows)
                    cols = [d[0] for d in cur.description]
                    rows: List[Dict[str, Any]] = [
                        dict(zip(cols, (_json_serializable(v) for v in raw))) for raw in raw_rows
                    ]

                    return {
                        "columns": cols,
                        "rows": rows,
                        "row_count": len(rows),
                        "truncated": truncated,
                        "duration_ms": max(duration_ms, 1),
                        "status": "success",
                        "command_tag": "SELECT",
                    }
                else:
                    return {
                        "columns": [],
                        "rows": [],
                        "row_count": cur.rowcount if cur.rowcount >= 0 else 0,
                        "truncated": False,
                        "duration_ms": max(duration_ms, 1),
                        "status": "success",
                        "command_tag": "OK",
                    }
        finally:
            conn.close()


native_query_engine = NativeQueryEngine()
