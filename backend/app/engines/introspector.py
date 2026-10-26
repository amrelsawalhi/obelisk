import os
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import pyarrow.parquet as pq
import psycopg2
from pymongo import MongoClient
import pymysql
import oracledb
import httpx

from app.core.host_resolver import resolve_connection_host
from app.core.dsn import sql_ident


def _preview_payload(
    columns: List[str], rows: List[Dict[str, Any]], start: float, row_count: Optional[int] = None
) -> Dict[str, Any]:
    """The preview dict every live source returns, timed so the UI can show a latency.

    The lakehouse preview gets ``duration_ms`` from the query engine; this puts the same
    field on the live-source previews, which had nothing there and rendered "Latency:  ms".
    """
    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows) if row_count is None else row_count,
        "duration_ms": int((time.perf_counter() - start) * 1000),
    }


def format_bytes(num_bytes: Optional[int]) -> Optional[str]:
    """Formats a byte count as ``B``/``KB``/``MB``/``GB``/``TB`` (binary units).

    Catalog listings show physical table size instead of a row count because size
    is served from database metadata (``pg_total_relation_size``,
    ``data_length``, ``storageSize``, segment bytes) and never requires a scan.
    Returns ``None`` when the size is unknown so the UI can render "—" rather
    than a misleading zero.
    """
    if num_bytes is None or num_bytes < 0:
        return None
    size = float(num_bytes)
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{int(round(size))} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024.0
    return None

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False

@dataclass
class TableInfo:
    name: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int] = None
    schema_name: Optional[str] = None

class Introspector:
    """
    Live schema introspection engine for PostgreSQL, MongoDB, MySQL, and Parquet.
    Executes real inspection against live database instances with zero mocking.
    """

    @staticmethod
    def introspect_postgres(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        schema: str = "public",
        sslmode: str = "disable",
    ) -> List[Dict[str, Any]]:
        """Introspects live PostgreSQL tables, columns and primary keys.

        ``size_bytes`` comes from ``pg_total_relation_size``, a catalog lookup, so
        opening the platform never scans user tables. Row counts are deliberately
        not reported: the planner's ``reltuples`` is a stale statistic that reads
        -1 until the first ANALYZE, and a wrong number is worse than none.
        """
        resolved_host = resolve_connection_host(host, port)
        conn = psycopg2.connect(
            host=resolved_host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=3,
            sslmode=sslmode,
        )
        tables = []
        try:
            with conn.cursor() as cur:
                # Fetch all user tables in target schema or all non-system schemas
                if schema and schema not in ("public", "all"):
                    schema_cond = "table_schema = %s"
                    params = (schema,)
                else:
                    schema_cond = "table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema NOT LIKE 'pg_toast%'"
                    params = ()

                q_tables = f"""
                    SELECT information_schema.tables.table_schema,
                           information_schema.tables.table_name,
                           pg_total_relation_size(c.oid)
                    FROM information_schema.tables
                    JOIN pg_namespace n
                      ON n.nspname = information_schema.tables.table_schema
                    JOIN pg_class c
                      ON c.relnamespace = n.oid
                     AND c.relname = information_schema.tables.table_name
                    WHERE {schema_cond} AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name;
                """
                if params:
                    cur.execute(q_tables, params)
                else:
                    cur.execute(q_tables)
                raw_tables = cur.fetchall()

                # Fetch primary keys
                if schema and schema not in ("public", "all"):
                    pk_schema_cond = "tc.table_schema = %s"
                else:
                    pk_schema_cond = "tc.table_schema NOT IN ('pg_catalog', 'information_schema') AND tc.table_schema NOT LIKE 'pg_toast%'"

                q_pks = f"""
                    SELECT tc.table_schema, kcu.table_name, kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY' AND {pk_schema_cond};
                """
                if params:
                    cur.execute(q_pks, params)
                else:
                    cur.execute(q_pks)
                pk_map = {}
                for s_name, t_name, c_name in cur.fetchall():
                    pk_map.setdefault((s_name, t_name), set()).add(c_name)

                # Fetch column details
                q_cols = f"""
                    SELECT table_schema, table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE {schema_cond}
                    ORDER BY table_schema, table_name, ordinal_position;
                """
                if params:
                    cur.execute(q_cols, params)
                else:
                    cur.execute(q_cols)
                col_map = {}
                for s_name, t_name, c_name, d_type, is_null in cur.fetchall():
                    col_map.setdefault((s_name, t_name), []).append({
                        "name": c_name,
                        "data_type": d_type,
                        "is_nullable": is_null == "YES",
                        "is_primary_key": c_name in pk_map.get((s_name, t_name), set()),
                    })

                # pg_total_relation_size is a catalog lookup: no table scan, and
                # no stale row estimate is reported alongside it.
                for s_name, t_name, relation_bytes in raw_tables:
                    display_name = f"{s_name}.{t_name}" if s_name != "public" else t_name
                    size_bytes = int(relation_bytes) if relation_bytes is not None else None
                    tables.append({
                        "name": display_name,
                        "schema_name": s_name,
                        "columns": col_map.get((s_name, t_name), []),
                        "size_bytes": size_bytes,
                        "size_human": format_bytes(size_bytes),
                        "size_is_estimate": False,
                    })
        finally:
            conn.close()

        return tables

    @staticmethod
    def preview_postgres(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table: Optional[str] = None,
        schema: str = "public",
        limit: int = 50,
        sslmode: str = "disable",
        table_name: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Fetches up to `limit` rows from a live PostgreSQL table for preview."""
        start = time.perf_counter()
        actual_table = table or table_name or ""
        if options and "sslmode" in options:
            sslmode = options["sslmode"]
        resolved_host = resolve_connection_host(host, port)
        if "." in actual_table:
            schema, actual_table = actual_table.split(".", 1)

        conn = psycopg2.connect(
            host=resolved_host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=3,
            sslmode=sslmode,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {sql_ident(schema)}.{sql_ident(actual_table)} LIMIT %s;",
                    (limit,),
                )
                columns = [desc[0] for desc in cur.description] if cur.description else []
                raw_rows = cur.fetchall()
                # Format to JSON-safe dictionary rows
                rows = []
                for r in raw_rows:
                    row_dict = {}
                    for col_name, val in zip(columns, r):
                        if hasattr(val, "isoformat"):
                            row_dict[col_name] = val.isoformat()
                        else:
                            row_dict[col_name] = val
                    rows.append(row_dict)
                return _preview_payload(columns, rows, start)
        finally:
            conn.close()

    @staticmethod
    def introspect_mongodb(
        host: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_db: str = "admin",
        database_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Introspects collections, sampled document schemas, and counts in MongoDB."""
        resolved_host = resolve_connection_host(host, port)
        # Kwargs avoid URI escaping problems in passwords (audit H5).
        client = MongoClient(
            host=resolved_host,
            port=port,
            username=username or None,
            password=password or None,
            authSource=auth_db if (username and password) else None,
            serverSelectionTimeoutMS=5000,
        )
        tables = []
        try:
            db_names = [database_name] if database_name else client.list_database_names()
            system_dbs = {"admin", "config", "local"}

            for db_name in db_names:
                if not database_name and db_name in system_dbs:
                    continue
                db = client[db_name]
                for coll_name in db.list_collection_names():
                    if coll_name.startswith("system."):
                        continue
                    coll = db[coll_name]
                    # Sample first few documents to determine field schema
                    sample_docs = list(coll.find().limit(5))
                    cols = {}
                    for doc in sample_docs:
                        for k, v in doc.items():
                            if k not in cols:
                                type_name = type(v).__name__
                                if type_name == "ObjectId":
                                    type_name = "string"
                                cols[k] = {
                                    "name": k,
                                    "data_type": type_name,
                                    "is_nullable": True,
                                    "is_primary_key": (k == "_id"),
                                }

                    # storageSize is the on-disk footprint reported by collStats.
                    # Document counts are omitted on purpose: count_documents({})
                    # scans the whole collection and estimated_document_count()
                    # can be far off, so the catalog reports size only.
                    try:
                        coll_size = int(db.command("collStats", coll_name).get("storageSize") or 0)
                    except Exception:
                        coll_size = None

                    tables.append({
                        "name": coll_name,
                        "schema_name": db_name,
                        "columns": list(cols.values()),
                        "size_bytes": coll_size,
                        "size_human": format_bytes(coll_size),
                        "size_is_estimate": False,
                    })
        finally:
            client.close()

        return tables

    @staticmethod
    def preview_mongodb(
        host: str,
        port: int,
        database: str,
        collection: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_db: str = "admin",
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Fetches up to `limit` documents from a live MongoDB collection."""
        start = time.perf_counter()
        resolved_host = resolve_connection_host(host, port)
        # Kwargs avoid URI escaping problems in passwords (audit H5).
        client = MongoClient(
            host=resolved_host,
            port=port,
            username=username or None,
            password=password or None,
            authSource=auth_db if (username and password) else None,
            serverSelectionTimeoutMS=5000,
        )
        try:
            db = client[database]
            coll = db[collection]
            cursor = coll.find().limit(limit)
            rows = []
            all_cols = set()
            for doc in cursor:
                clean_row = {}
                for k, v in doc.items():
                    all_cols.add(k)
                    if hasattr(v, "isoformat"):
                        clean_row[k] = v.isoformat()
                    elif str(type(v)).find("ObjectId") != -1:
                        clean_row[k] = str(v)
                    else:
                        clean_row[k] = v
                rows.append(clean_row)
            return _preview_payload(list(all_cols), rows, start)
        finally:
            client.close()

    @staticmethod
    def introspect_mysql(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
    ) -> List[Dict[str, Any]]:
        """Introspects live MySQL/MariaDB database tables and schemas."""
        resolved_host = resolve_connection_host(host, port)
        conn = pymysql.connect(
            host=resolved_host,
            port=port,
            user=username,
            password=password,
            database=database,
            connect_timeout=3,
            charset="utf8mb4",
        )
        tables = []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name,
                           COALESCE(data_length, 0) + COALESCE(index_length, 0)
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                    """,
                    (database,),
                )
                table_rows = cur.fetchall()
                table_names = [r[0] for r in table_rows]
                size_est = {r[0]: r[1] for r in table_rows}

                # Fetch primary keys
                cur.execute(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = %s AND constraint_name = 'PRIMARY';
                    """,
                    (database,),
                )
                pk_map = {}
                for t_name, c_name in cur.fetchall():
                    pk_map.setdefault(t_name, set()).add(c_name)

                # Fetch columns
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    ORDER BY table_name, ordinal_position;
                    """,
                    (database,),
                )
                col_map = {}
                for t_name, c_name, d_type, is_null in cur.fetchall():
                    col_map.setdefault(t_name, []).append({
                        "name": c_name,
                        "data_type": d_type,
                        "is_nullable": is_null == "YES",
                        "is_primary_key": c_name in pk_map.get(t_name, set()),
                    })

                # InnoDB's data_length is a statistic rather than an exact value,
                # but reading it never scans the table.
                for t_name in table_names:
                    t_bytes = size_est.get(t_name)
                    tables.append({
                        "name": t_name,
                        "schema_name": database,
                        "columns": col_map.get(t_name, []),
                        "size_bytes": int(t_bytes) if t_bytes is not None else None,
                        "size_human": format_bytes(t_bytes),
                        "size_is_estimate": True,
                    })
        finally:
            conn.close()

        return tables

    @staticmethod
    def preview_mysql(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Fetches up to `limit` rows from a live MySQL table."""
        start = time.perf_counter()
        resolved_host = resolve_connection_host(host, port)
        conn = pymysql.connect(
            host=resolved_host,
            port=port,
            user=username,
            password=password,
            database=database,
            connect_timeout=3,
            cursorclass=pymysql.cursors.DictCursor,
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{table}` LIMIT %s", (limit,))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                clean_rows = []
                for r in rows:
                    cr = {}
                    for k, v in r.items():
                        if hasattr(v, "isoformat"):
                            cr[k] = v.isoformat()
                        elif hasattr(v, "__float__"):
                            cr[k] = float(v)
                        else:
                            cr[k] = v
                    clean_rows.append(cr)
                return _preview_payload(columns, clean_rows, start)
        finally:
            conn.close()

    @staticmethod
    def introspect_oracle(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
    ) -> List[Dict[str, Any]]:
        """Introspects live Oracle database tables."""
        resolved_host = resolve_connection_host(host, port)
        dsn = f"{resolved_host}:{port}/{database}"
        conn = oracledb.connect(user=username, password=password, dsn=dsn, tcp_connect_timeout=3.0)
        tables = []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT t.table_name, s.bytes
                FROM all_tables t
                LEFT JOIN user_segments s
                  ON s.segment_name = t.table_name AND s.segment_type = 'TABLE'
                WHERE t.owner = UPPER(:1)
                ORDER BY t.table_name
            """, [username])
            table_rows = cur.fetchall()
            if not table_rows:
                cur.execute("""
                    SELECT t.table_name, s.bytes
                    FROM user_tables t
                    LEFT JOIN user_segments s
                      ON s.segment_name = t.table_name AND s.segment_type = 'TABLE'
                    ORDER BY t.table_name
                """)
                table_rows = cur.fetchall()

            for t_name, t_bytes in table_rows:
                cur.execute("""
                    SELECT column_name, data_type, nullable
                    FROM all_tab_columns
                    WHERE table_name = :1
                    ORDER BY column_id
                """, [t_name])
                cols = []
                for c_name, d_type, nullable in cur.fetchall():
                    cols.append({
                        "name": c_name,
                        "data_type": d_type,
                        "is_nullable": nullable == "Y",
                        "is_primary_key": False,
                    })
                tables.append({
                    "name": t_name,
                    "schema_name": username.upper(),
                    "columns": cols,
                    "size_bytes": int(t_bytes) if t_bytes is not None else None,
                    "size_human": format_bytes(t_bytes),
                    "size_is_estimate": True,
                })
        finally:
            conn.close()
        return tables

    @staticmethod
    def preview_oracle(
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Fetches up to `limit` rows from a live Oracle table."""
        start = time.perf_counter()
        resolved_host = resolve_connection_host(host, port)
        dsn = f"{resolved_host}:{port}/{database}"
        conn = oracledb.connect(user=username, password=password, dsn=dsn, tcp_connect_timeout=3.0)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT * FROM {sql_ident(table)} FETCH FIRST :limit ROWS ONLY", [limit]
            )
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = []
            for raw in cur.fetchall():
                row_dict = {}
                for col_name, val in zip(columns, raw):
                    if hasattr(val, "isoformat"):
                        row_dict[col_name] = val.isoformat()
                    else:
                        row_dict[col_name] = val
                rows.append(row_dict)
            return _preview_payload(columns, rows, start)
        finally:
            conn.close()

    @staticmethod
    def _extract_api_records(data: Any, data_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Helper to extract list of records from arbitrary JSON payload."""
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            if data_key and data_key in data and isinstance(data[data_key], list):
                return [r for r in data[data_key] if isinstance(r, dict)]
            for k in ["items", "data", "results", "records", "rows", "values"]:
                if k in data and isinstance(data[k], list):
                    return [r for r in data[k] if isinstance(r, dict)]
            return [data]
        return []

    @staticmethod
    def introspect_api(
        base_url: str,
        endpoint: str = "",
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        data_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Introspects a REST API / ERP endpoint (SAP, Salesforce, Custom JSON API)."""
        url = base_url.strip()
        if not url.startswith(("http://", "https://")):
            url = f"http://{url}"
        if endpoint:
            url = f"{url.rstrip('/')}/{endpoint.lstrip('/')}"

        req_headers = {"Accept": "application/json"}
        if token:
            req_headers["Authorization"] = f"Bearer {token}"
        if headers:
            req_headers.update(headers)

        with httpx.Client(timeout=10.0, follow_redirects=True) as client:
            resp = client.get(url, headers=req_headers)
            resp.raise_for_status()
            data = resp.json()

        records = Introspector._extract_api_records(data, data_key)
        cols = []
        seen_cols = set()
        for r in records[:50]:
            for k, v in r.items():
                if k not in seen_cols:
                    seen_cols.add(k)
                    t_name = "string"
                    if isinstance(v, bool):
                        t_name = "boolean"
                    elif isinstance(v, int):
                        t_name = "int64"
                    elif isinstance(v, float):
                        t_name = "float64"
                    elif isinstance(v, (dict, list)):
                        t_name = "json"
                    cols.append({
                        "name": k,
                        "data_type": t_name,
                        "is_nullable": True,
                        "is_primary_key": (k.lower() in ("id", "uuid", "pk")),
                    })

        table_name = endpoint.strip("/").replace("/", "_") or "api_records"
        return [{
            "name": table_name,
            "schema_name": "api",
            "columns": cols,
            "row_count": len(records),
        }]

    @staticmethod
    def preview_api(
        base_url: str,
        endpoint: str = "",
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        data_key: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Fetches records from REST API / ERP endpoint as table rows."""
        start = time.perf_counter()
        url = base_url.strip()
        if not url.startswith(("http://", "https://")):
            url = f"http://{url}"
        if endpoint:
            url = f"{url.rstrip('/')}/{endpoint.lstrip('/')}"

        req_headers = {"Accept": "application/json"}
        if token:
            req_headers["Authorization"] = f"Bearer {token}"
        if headers:
            req_headers.update(headers)

        with httpx.Client(timeout=10.0, follow_redirects=True) as client:
            resp = client.get(url, headers=req_headers)
            resp.raise_for_status()
            data = resp.json()

        records = Introspector._extract_api_records(data, data_key)
        slice_records = records[:limit]
        columns = []
        if slice_records:
            columns = list(slice_records[0].keys())
        return _preview_payload(columns, slice_records, start)

    @staticmethod
    def _table_from_file(f: Path, schema_name: str, fqn: str) -> Optional[Dict[str, Any]]:
        """Describes one Parquet file from its footer: columns, rows and size.

        Shared by the single-directory and whole-lakehouse paths so they cannot drift.
        A file that cannot be read is skipped rather than failing the whole listing.
        """
        try:
            parquet_file = pq.ParquetFile(str(f))
            schema = parquet_file.schema_arrow
            num_rows = parquet_file.metadata.num_rows

            cols = []
            for field in schema:
                cols.append({
                    "name": field.name,
                    "data_type": str(field.type),
                    "is_nullable": field.nullable,
                    "is_primary_key": False,
                })

            file_size = f.stat().st_size
            return {
                "name": f.stem,
                "schema_name": schema_name,
                "fqn": fqn,
                "columns": cols,
                "row_count": num_rows,
                "size_bytes": file_size,
                "size_human": format_bytes(file_size),
                "size_is_estimate": False,
                "file_path": f.as_posix(),
            }
        except Exception:
            return None

    @staticmethod
    def introspect_parquet(dir_or_file_path: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Introspects Parquet metadata and columns from disk."""
        path = Path(dir_or_file_path)
        files: List[Path] = []
        inferred_schema = schema_name

        if path.is_file() and path.suffix == ".parquet":
            files = [path]
            if not inferred_schema:
                # The schema is the folder the file sits in; nothing is reserved.
                parent_name = path.parent.name
                inferred_schema = parent_name if parent_name and parent_name != "lakehouse" else "main"
        elif path.is_dir():
            files = sorted(path.glob("*.parquet"))
            if not inferred_schema:
                inferred_schema = path.name if path.name != "lakehouse" else "main"

        final_schema = inferred_schema or "main"
        clean_schema = final_schema.replace("/", ".")

        tables = []
        for f in files:
            entry = Introspector._table_from_file(
                f, final_schema, f"lakehouse.{clean_schema}.{f.stem}"
            )
            if entry:
                tables.append(entry)
        return tables


introspector = Introspector()
