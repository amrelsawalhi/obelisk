import os
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
        """Introspects live PostgreSQL tables, columns, primary keys, and counts."""
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
                    SELECT table_schema, table_name
                    FROM information_schema.tables
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

                # Skip expensive count(*) to protect against sequential scans on large tables
                for s_name, t_name in raw_tables:
                    display_name = f"{s_name}.{t_name}" if s_name != "public" else t_name
                    tables.append({
                        "name": display_name,
                        "schema_name": s_name,
                        "columns": col_map.get((s_name, t_name), []),
                        "row_count": None,
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
                cur.execute(f'SELECT * FROM "{schema}"."{actual_table}" LIMIT %s;', (limit,))
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
                return {"columns": columns, "rows": rows, "row_count": len(rows)}
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
        if username and password:
            uri = f"mongodb://{username}:{password}@{resolved_host}:{port}/?authSource={auth_db}"
        else:
            uri = f"mongodb://{resolved_host}:{port}/"

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
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

                    try:
                        coll_row_count = coll.count_documents({})
                    except Exception:
                        coll_row_count = None

                    tables.append({
                        "name": coll_name,
                        "schema_name": db_name,
                        "columns": list(cols.values()),
                        "row_count": coll_row_count,
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
        resolved_host = resolve_connection_host(host, port)
        if username and password:
            uri = f"mongodb://{username}:{password}@{resolved_host}:{port}/?authSource={auth_db}"
        else:
            uri = f"mongodb://{resolved_host}:{port}/"

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
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
            return {"columns": list(all_cols), "rows": rows, "row_count": len(rows)}
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
                    SELECT table_name, table_rows
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                    """,
                    (database,),
                )
                table_rows = cur.fetchall()
                table_names = [r[0] for r in table_rows]
                row_count_est = {r[0]: r[1] for r in table_rows}

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

                for t_name in table_names:
                    tables.append({
                        "name": t_name,
                        "schema_name": database,
                        "columns": col_map.get(t_name, []),
                        "row_count": None,
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
                return {"columns": columns, "rows": clean_rows, "row_count": len(clean_rows)}
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
                SELECT table_name
                FROM all_tables
                WHERE owner = UPPER(:1)
                ORDER BY table_name
            """, [username])
            table_names = [r[0] for r in cur.fetchall()]
            if not table_names:
                cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
                table_names = [r[0] for r in cur.fetchall()]

            for t_name in table_names:
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
                    "row_count": None,
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
        resolved_host = resolve_connection_host(host, port)
        dsn = f"{resolved_host}:{port}/{database}"
        conn = oracledb.connect(user=username, password=password, dsn=dsn, tcp_connect_timeout=3.0)
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM "{table}" FETCH FIRST :limit ROWS ONLY', [limit])
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
            return {"columns": columns, "rows": rows, "row_count": len(rows)}
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
        return {"columns": columns, "rows": slice_records, "row_count": len(slice_records)}

    @staticmethod
    def introspect_parquet(dir_or_file_path: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Introspects Parquet metadata and columns from disk."""
        path = Path(dir_or_file_path)
        tables = []

        files = []
        inferred_schema = schema_name
        if path.is_file() and path.suffix == ".parquet":
            files = [path]
            if not inferred_schema:
                if path.parent.name in ("bronze", "silver", "gold"):
                    inferred_schema = path.parent.name
                else:
                    inferred_schema = "bronze"
        elif path.is_dir():
            files = list(path.glob("*.parquet"))
            if not inferred_schema:
                inferred_schema = path.name if path.name != "lakehouse" else "bronze"

        final_schema = inferred_schema or "bronze"

        for f in files:
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

                clean_schema = final_schema.replace("/", ".")
                tables.append({
                    "name": f.stem,
                    "schema_name": final_schema,
                    "fqn": f"lakehouse.{clean_schema}.{f.stem}",
                    "columns": cols,
                    "row_count": num_rows,
                    "file_path": f.as_posix(),
                })
            except Exception:
                continue

        return tables

introspector = Introspector()
