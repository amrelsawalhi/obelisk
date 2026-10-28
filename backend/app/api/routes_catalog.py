import asyncio
import os
import sqlite3
import time
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.config import settings
from app.core.vault import encrypt, decrypt
from app.core.host_resolver import resolve_connection_host
from app.core.dsn import sql_string
from app.models.connection import Connection
from app.engines.introspector import format_bytes, introspector
from app.engines.duckdb_engine import duckdb_engine
from app.core.resource_advisor import advisor
from app.core.sources import (
    DRIVERS,
    UnsupportedSource,
    capabilities_for,
    connection_alias,
    infer_driver,
    resolve_source,
)
from app.services.catalog import catalog_service
from app.services.superset import (
    remove_connection_quietly,
    sync_connection_quietly,
)
from app.core.catalog import (
    SOURCE,
    LOAD_TABLE_SUFFIX,
    InvalidTablePath,
    drop_load_tables,
    lakehouse_connection,
    lakehouse_schemas,
    lakehouse_tables,
    load_tables,
    quote_ident,
    table_directory,
    validate_folder,
    validate_segment,
)


# -- lakehouse identity -------------------------------------------------------
#
# A folder is a schema and a table is a table, so these two helpers are the only place
# that decides what "folder + name" means in SQL. The warehouse root is the catalog's
# default schema, which is why "root" and "main" are the same thing here.

def _lakehouse_schema(folder: str) -> str:
    clean = (folder or "").strip().strip("/")
    if clean in ("", "root", "main"):
        return "main"
    return clean.replace("/", ".")


def _lakehouse_table_exists(conn, schema: str, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
        "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s "
        "  ON s.schema_id = t.schema_id "
        "WHERE t.end_snapshot IS NULL AND s.schema_name = ? AND t.table_name = ? LIMIT 1",
        [schema, name],
    ).fetchone()
    return row is not None


def _schema_has_tables(conn, schema: str) -> bool:
    """Whether one schema still holds a live table. A nested schema is its own name."""
    rows = conn.execute(
        "SELECT 1 FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
        "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s "
        "  ON s.schema_id = t.schema_id "
        "WHERE t.end_snapshot IS NULL AND s.schema_name = ? LIMIT 1",
        [schema],
    ).fetchall()
    return bool(rows)


# The three operations below are synchronous on purpose: the routes hand them to
# ``asyncio.to_thread`` so a DuckDB call never blocks the event loop (audit: async
# routes must not block). Each returns a word the route turns into a response.

def _rename_table_sync(schema: str, old_name: str, new_name: str) -> str:
    conn = duckdb_engine.get_connection()
    try:
        if not _lakehouse_table_exists(conn, schema, old_name):
            return "missing"
        if _lakehouse_table_exists(conn, schema, new_name):
            return "exists"
        conn.execute(
            f"ALTER TABLE lakehouse.{quote_ident(schema)}.{quote_ident(old_name)} "
            f"RENAME TO {quote_ident(new_name)}"
        )
        return "ok"
    finally:
        conn.close()


def _prune_table_directory(schema: str, name: str) -> None:
    """Removes a table's leftover directory, and the folder with it when that emptied it.

    A root-level table's parent is the warehouse itself, which no table operation sweeps.
    """
    root = settings.lakehouse_root_path
    directory = table_directory(root, schema, name)
    prune_empty_dirs(root, directory if directory.parent == root else directory.parent)


def _delete_table_sync(schema: str, name: str) -> str:
    """Drops one table, and the folder with it when it was the last table in it.

    A folder is a schema, so a folder that holds nothing is a schema that holds nothing —
    and one nobody made on purpose (no ``.gitkeep`` to keep it) has no reason to outlive
    its last table. The tree showed such a folder until the schema went, and the directory
    DuckLake derived from the schema stayed on disk either way.
    """
    conn = duckdb_engine.get_connection()
    try:
        if not _lakehouse_table_exists(conn, schema, name):
            return "missing"
        conn.execute(f"DROP TABLE lakehouse.{quote_ident(schema)}.{quote_ident(name)}")
        if schema != "main" and not _schema_has_tables(conn, schema):
            try:
                conn.execute(f"DROP SCHEMA IF EXISTS {SOURCE}.{quote_ident(schema)}")
            except Exception:
                # A schema that still holds a nested one is not an empty folder; it stays.
                pass
        _prune_table_directory(schema, name)
        return "ok"
    finally:
        conn.close()


def _live_tables_in(conn, schema_prefix: str) -> List[tuple[str, str]]:
    """Every live table in the schemas a folder owns, as ``(schema, table)``.

    A folder is a schema, and a nested folder is a schema whose name carries the rest of the
    path (``raw/nested`` is ``raw.nested``), so a folder owns its own schema *and* every
    schema beneath it. ``main`` is the warehouse root.
    """
    rows = conn.execute(
        f"SELECT s.schema_name, t.table_name "
        f"FROM __ducklake_metadata_{SOURCE}.ducklake_table AS t "
        f"JOIN __ducklake_metadata_{SOURCE}.ducklake_schema AS s ON s.schema_id = t.schema_id "
        f"WHERE t.end_snapshot IS NULL"
    ).fetchall()
    return [
        (str(schema_name), str(table_name))
        for schema_name, table_name in rows
        if schema_name == schema_prefix or schema_name.startswith(f"{schema_prefix}.")
    ]


def _move_table_on(conn, src_schema: str, dst_schema: str, name: str) -> str:
    """Moves one table to another schema, on a connection the caller owns.

    ``ALTER TABLE ... SET SCHEMA`` is the one-statement move and is tried first, but this
    DuckLake build answers it with a parser error, so the copy-then-drop below is the path
    actually taken. That rewrite is also why a folder rename may not simply move the folder
    on disk: DuckLake derives a table's data directory from its schema, so files left under
    the old folder would be files the catalog no longer points at.
    """
    src = f"{SOURCE}.{quote_ident(src_schema)}.{quote_ident(name)}"
    dst = f"{SOURCE}.{quote_ident(dst_schema)}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {dst}")
    if _lakehouse_table_exists(conn, dst_schema, name):
        return "exists"
    try:
        conn.execute(f"ALTER TABLE {src} SET SCHEMA {dst}")
    except Exception:
        # DuckLake may not support SET SCHEMA yet; a copy-then-drop is the honest fallback
        # and still ends with the data in exactly one place.
        conn.execute(f"CREATE TABLE {dst}.{quote_ident(name)} AS SELECT * FROM {src}")
        conn.execute(f"DROP TABLE {src}")
    # Whichever path moved it, the directory the table used belongs to the schema it left,
    # and it holds nothing now.
    _prune_table_directory(src_schema, name)
    return "ok"


def _move_table_sync(src_schema: str, dst_schema: str, name: str) -> str:
    conn = duckdb_engine.get_connection()
    try:
        if not _lakehouse_table_exists(conn, src_schema, name):
            return "missing"
        return _move_table_on(conn, src_schema, dst_schema, name)
    finally:
        conn.close()


def _rename_directory_sync(old_rel_path: str, new_rel_path: str) -> tuple[int, int]:
    """Moves every table a folder owns into the renamed schemas.

    Returns ``(moved, skipped)``. Nothing moves when a destination table already exists:
    overwriting it would destroy rows the user did not name, so the conflicts are counted and
    the caller reports them, leaving the folder exactly as it was.

    The old schemas are dropped once they are empty — a rename that left ``raw`` behind
    beside ``marts`` would show the same tables twice in the tree.
    """
    old_prefix = _lakehouse_schema(old_rel_path)
    new_prefix = _lakehouse_schema(new_rel_path)
    conn = duckdb_engine.get_connection()
    try:
        tables = _live_tables_in(conn, old_prefix)

        def destination(schema: str) -> str:
            return new_prefix + schema[len(old_prefix):]

        conflicts = [
            table for schema, table in tables if _lakehouse_table_exists(conn, destination(schema), table)
        ]
        if conflicts:
            return 0, len(conflicts)

        moved = 0
        for schema, table in tables:
            if _move_table_on(conn, schema, destination(schema), table) == "ok":
                moved += 1
        for schema in sorted({schema for schema, _ in tables}, key=len, reverse=True):
            try:
                conn.execute(f"DROP SCHEMA IF EXISTS {SOURCE}.{quote_ident(schema)}")
            except Exception:
                # A schema that still holds something is not a reason to fail the rename
                # that already happened; it just stays until the next attempt.
                pass
        return moved, len(conflicts)
    finally:
        conn.close()


def _delete_directory_sync(rel_path: str, cascade: bool) -> int:
    """Drops every table a folder owns, then the schemas themselves.

    Returns the number of tables dropped, or ``-n`` when ``n`` tables exist and ``cascade``
    is false: deleting a folder that still holds tables is the caller's decision to confirm,
    not something to do quietly. Dropping them is what makes the folder safe to remove from
    disk — DuckLake derives a table's data directory from its schema, so a folder on its own
    means nothing to it.
    """
    prefix = _lakehouse_schema(rel_path)
    conn = duckdb_engine.get_connection()
    try:
        tables = _live_tables_in(conn, prefix)
        if tables and not cascade:
            return -len(tables)
        for schema, table in tables:
            conn.execute(
                f"DROP TABLE IF EXISTS {SOURCE}.{quote_ident(schema)}.{quote_ident(table)}"
            )
        # A dropped table leaves its data directory behind, empty, one level under the
        # schema it belonged to. The folder itself is removed by the route.
        for schema, table in tables:
            _prune_table_directory(schema, table)
        # Every schema the folder owns goes, not only the ones that held a table: an empty
        # schema *is* an empty folder, and it is what the tree shows until it is dropped.
        owned = [
            schema
            for schema in lakehouse_schemas(conn)
            if schema == prefix or schema.startswith(f"{prefix}.")
        ]
        for schema in sorted(owned, key=len, reverse=True):
            try:
                conn.execute(f"DROP SCHEMA IF EXISTS {SOURCE}.{quote_ident(schema)}")
            except Exception:
                pass
        return len(tables)
    finally:
        conn.close()
from app.core.catalog import scan
from app.core.storage import prune_empty_dirs, warehouse_storage

router = APIRouter(prefix="/api/catalog", tags=["catalog"])

_catalog_tree_cache: Optional[List[Dict[str, Any]]] = None
_catalog_tree_cache_time: float = 0.0
_CATALOG_CACHE_TTL: float = 1.0

def invalidate_catalog_cache():
    global _catalog_tree_cache, _catalog_tree_cache_time
    _catalog_tree_cache = None
    _catalog_tree_cache_time = 0.0

class CreateConnectionRequest(BaseModel):
    name: str
    source_type: str  # postgres, mongodb, mysql, oracle, api
    host: str
    port: int
    database: str
    username: str
    password: Optional[str] = ""
    options: Optional[Dict[str, Any]] = None
    driver: Optional[str] = Field(
        default=None,
        description="attach | sdk | http. Inferred from source_type when omitted.",
    )
    read_only: Optional[bool] = None

class UpdateConnectionRequest(BaseModel):
    name: Optional[str] = None
    source_type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    options: Optional[Dict[str, Any]] = None
    driver: Optional[str] = None
    read_only: Optional[bool] = None

class ConnectionResponse(BaseModel):
    id: str
    name: str
    source_type: str
    host: str
    port: int
    database: str
    username: str
    options: Optional[Dict[str, Any]] = None
    driver: str
    capabilities: List[str] = Field(default_factory=list)
    read_only: bool = True
    created_at: str


def _connection_response(conn: Connection) -> ConnectionResponse:
    """Serialises a connection together with the recipe describing how to reach it.

    The driver is reported as stored; capabilities come from the code, so a source
    type that this build cannot reach shows ``driver='unknown'`` instead of
    pretending to have capabilities it does not.
    """
    try:
        recipe = resolve_source(conn)
        driver: str = recipe.driver
        capabilities = list(recipe.capabilities)
        read_only = recipe.read_only
    except UnsupportedSource:
        driver = conn.driver or "unknown"
        capabilities = []
        read_only = bool(conn.read_only)

    return ConnectionResponse(
        id=conn.id,
        name=conn.name,
        source_type=conn.source_type,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        options=conn.options,
        driver=driver,
        capabilities=capabilities,
        read_only=read_only,
        created_at=conn.created_at.isoformat(),
    )


def _resolve_driver_or_400(source_type: str, driver: Optional[str]) -> str:
    """Pins a driver when given, infers one otherwise, and rejects nonsense."""
    chosen = (driver or "").lower().strip()
    if chosen and chosen not in DRIVERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"driver must be one of {list(DRIVERS)}, got {driver!r}.",
        )
    if chosen:
        return chosen
    try:
        return infer_driver(source_type)
    except UnsupportedSource as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

@router.post("/connections", response_model=ConnectionResponse)
async def create_connection(
    req: CreateConnectionRequest,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Registers an enterprise source connection in the encrypted vault."""
    clean_name = req.name.strip()
    stmt = select(Connection).where(Connection.name == clean_name)
    existing = await db.execute(stmt)
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"A connection named '{clean_name}' is already registered in the vault. Please choose a unique name or edit the existing connection in Settings."
        )

    encrypted_pw = encrypt(req.password or "")
    source_type = req.source_type.lower().strip()
    driver = _resolve_driver_or_400(source_type, req.driver)
    # An attached source is read live, so it is read-only unless an operator says
    # otherwise; a pulled one defaults to writable-into-the-lakehouse.
    read_only = req.read_only if req.read_only is not None else (driver == "attach")

    new_conn = Connection(
        name=clean_name,
        source_type=source_type,
        host=req.host.strip(),
        port=req.port,
        database=req.database.strip(),
        username=req.username.strip(),
        encrypted_credentials=encrypted_pw,
        options=req.options,
        driver=driver,
        capabilities_json=list(capabilities_for(driver)),
        read_only=read_only,
    )
    db.add(new_conn)
    try:
        await db.commit()
        await db.refresh(new_conn)
        invalidate_catalog_cache()
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register connection: {str(e)}"
        )

    # Superset keeps its own copy of the source list; a fire-and-forget push keeps it
    # honest without making a vault write depend on Superset being reachable.
    background.add_task(sync_connection_quietly, new_conn.name)

    return _connection_response(new_conn)

@router.get("/connections", response_model=List[ConnectionResponse])
async def list_connections(db: AsyncSession = Depends(get_db)):
    """Lists registered external connections."""
    stmt = select(Connection)
    result = await db.execute(stmt)
    conns = result.scalars().all()
    return [_connection_response(c) for c in conns]

def _execute_live_test(
    source_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    options: Optional[Dict[str, Any]] = None,
    name: str = "source",
) -> Dict[str, Any]:
    resolved_host = resolve_connection_host(host, port)
    start_time = time.time()
    try:
        st = source_type.lower()
        if st == "postgres":
            import psycopg2
            sslmode = (options or {}).get("sslmode", "disable")
            c = psycopg2.connect(
                host=resolved_host,
                port=port,
                dbname=database,
                user=username,
                password=password,
                connect_timeout=3,
                sslmode=sslmode,
            )
            with c.cursor() as cur:
                cur.execute("SELECT 1;")
            c.close()
        elif st == "mongodb":
            from pymongo import MongoClient
            auth_source = (options or {}).get("auth_source", "admin")
            # Credentials go in as kwargs so characters like @ : / % ? cannot
            # break RFC 3986 URI parsing (audit H5).
            client = MongoClient(
                host=resolved_host,
                port=port,
                username=username or None,
                password=password or None,
                authSource=auth_source if (username and password) else None,
                serverSelectionTimeoutMS=3000,
            )
            client.admin.command("ping")
            client.close()
        elif st in ("mysql", "mariadb"):
            import pymysql
            c = pymysql.connect(
                host=resolved_host,
                port=port,
                user=username,
                password=password,
                database=database,
                connect_timeout=3,
            )
            with c.cursor() as cur:
                cur.execute("SELECT 1;")
            c.close()
        elif st == "oracle":
            import oracledb
            dsn = f"{resolved_host}:{port}/{database}"
            c = oracledb.connect(user=username, password=password, dsn=dsn, tcp_connect_timeout=3.0)
            with c.cursor() as cur:
                cur.execute("SELECT 1 FROM DUAL")
            c.close()
        elif st in ("api", "rest", "rest_api", "erp"):
            import httpx
            base_url = host.strip()
            if not base_url.startswith(("http://", "https://")):
                if port and port not in (80, 443):
                    base_url = f"http://{base_url}:{port}"
                else:
                    base_url = f"http://{base_url}"
            headers = {"Accept": "application/json"}
            if password:
                headers["Authorization"] = f"Bearer {password}"
            with httpx.Client(timeout=5.0, follow_redirects=True) as client:
                resp = client.get(base_url, headers=headers)
                if resp.status_code >= 500:
                    raise Exception(f"Endpoint returned HTTP {resp.status_code}")
        else:
            raise Exception(f"Unsupported source type '{source_type}'")

        latency_ms = round((time.time() - start_time) * 1000, 2)
        return {
            "status": "success",
            "latency_ms": latency_ms,
            "message": f"Successfully connected to {name} in {latency_ms} ms"
        }
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        return {
            "status": "error",
            "latency_ms": latency_ms,
            "message": f"Connection failed: {str(e)}"
        }

@router.post("/connections/test")
async def test_connection_params(req: CreateConnectionRequest):
    """Tests live connectivity using provided parameters without saving to database."""
    # Live probes (psycopg2/pymongo/pymysql/httpx) are blocking — keep the event loop free.
    return await asyncio.to_thread(
        _execute_live_test,
        source_type=req.source_type,
        host=req.host,
        port=req.port,
        database=req.database,
        username=req.username,
        password=req.password or "",
        options=req.options,
        name=req.name,
    )

@router.post("/connections/{conn_id}/test")
async def test_connection(
    conn_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Tests live connectivity and returns latency and health status."""
    stmt = select(Connection).where(Connection.id == conn_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{conn_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    return await asyncio.to_thread(
        _execute_live_test,
        source_type=conn.source_type,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        password=plaintext_pw,
        options=conn.options,
        name=conn.name,
    )

@router.put("/connections/{conn_id}", response_model=ConnectionResponse)
async def update_connection(
    conn_id: str,
    req: UpdateConnectionRequest,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Updates an existing connection in the SQLite vault."""
    stmt = select(Connection).where(Connection.id == conn_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{conn_id}' not found."
        )

    # A rename must delete the old datasource, not leave it behind pointing at nothing.
    previous_name = conn.name

    if req.name is not None:
        conn.name = req.name.strip()
    if req.source_type is not None:
        conn.source_type = req.source_type.lower().strip()
    if req.host is not None:
        conn.host = req.host.strip()
    if req.port is not None:
        conn.port = req.port
    if req.database is not None:
        conn.database = req.database.strip()
    if req.username is not None:
        conn.username = req.username.strip()
    if req.password is not None and req.password.strip():
        conn.encrypted_credentials = encrypt(req.password)
    if req.options is not None:
        conn.options = req.options

    previous_driver = conn.driver
    if req.driver is not None:
        conn.driver = _resolve_driver_or_400(conn.source_type, req.driver)
    elif req.source_type is not None:
        # Re-infer for the new type instead of keeping a driver pinned to the old one.
        conn.driver = _resolve_driver_or_400(conn.source_type, None)

    if req.read_only is not None:
        conn.read_only = req.read_only

    try:
        conn.capabilities_json = list(resolve_source(conn).capabilities)
    except UnsupportedSource as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    if conn.driver != previous_driver:
        # Kind depends on the driver (shortcut vs external), so rows recorded under
        # the old driver would be lies; drop them and let the next reconcile rebuild.
        await catalog_service.delete_for_connection(db, conn.id)

    await db.commit()
    await db.refresh(conn)
    invalidate_catalog_cache()

    # Fire-and-forget drift control: Superset is corrected in the background, and an
    # unreachable Superset never fails the vault write.
    if previous_name != conn.name:
        background.add_task(remove_connection_quietly, previous_name)
    background.add_task(sync_connection_quietly, conn.name)

    return _connection_response(conn)

@router.delete("/connections/{conn_id}")
async def delete_connection(
    conn_id: str,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Deletes a connection from the SQLite vault."""
    stmt = select(Connection).where(Connection.id == conn_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{conn_id}' not found."
        )

    # Remove the connection's catalogued objects in one statement, then the row:
    # a detached shortcut would otherwise outlive the connection it points at (M7).
    await catalog_service.delete_for_connection(db, conn_id)
    removed_name = conn.name
    await db.delete(conn)
    await db.commit()
    invalidate_catalog_cache()
    # The datasource in Superset is keyed on this name, so it goes with it.
    background.add_task(remove_connection_quietly, removed_name)
    return {"status": "deleted", "id": conn_id}


@router.post("/connections/{conn_id}/reconcile")
async def reconcile_connection_objects(
    conn_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Introspects one source and refreshes its rows in ``catalog_objects``.

    This is the exit gate of Phase 1: registering a connection is enough for its
    tables to show up in the catalog, with no per-source code.
    """
    conn = (
        await db.execute(select(Connection).where(Connection.id == conn_id))
    ).scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{conn_id}' not found.",
        )

    tables = await _introspect_connection(conn)
    if tables is None:
        # Refuse loudly: pruning on "could not look" would erase a healthy catalog
        # because of one failed TCP connection.
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Could not introspect '{conn.name}'; the catalog was left unchanged.",
        )
    result = await catalog_service.reconcile_connection(db, conn, tables)
    invalidate_catalog_cache()
    return result


@router.get("/objects")
async def list_catalog_objects(
    kind: Optional[str] = None,
    connection_id: Optional[str] = None,
    folder: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Lists registry rows, refreshed from the files first.

    The lakehouse half of this table is a projection, so the read refreshes it: asking
    the catalog what exists is also what makes the answer true.
    """
    await catalog_service.refresh(db)
    objects = await catalog_service.list_objects(
        db, kind=kind, connection_id=connection_id
    )
    if folder is not None:
        prefix = f"{folder.strip('/')}/"
        objects = [
            obj for obj in objects
            if obj.location and (obj.location == folder or obj.location.startswith(prefix))
        ]
    return [
        {
            "id": obj.id,
            "fqn": obj.fqn,
            "name": obj.name,
            "kind": obj.kind,
            "backend": obj.backend,
            "path": obj.location,
            "folder": (obj.location.rsplit("/", 1)[0] if "/" in (obj.location or "") else None),
            "connection_id": obj.connection_id,
            "location": obj.location,
            "size_bytes": obj.size_bytes,
            "row_count": obj.row_count,
            "version": obj.version,
            "capabilities": obj.capabilities_json or [],
        }
        for obj in objects
    ]

class CreateLakehouseDirectoryRequest(BaseModel):
    path: Optional[str] = Field(None, description="Directory name or nested relative path (e.g. 'bronze/events')")
    name: Optional[str] = Field(None, description="Alternative field for directory name")

class RenameLakehouseDirectoryRequest(BaseModel):
    path: str = Field(..., description="Current relative path of the directory (e.g. 'bronze/test')")
    new_name: str = Field(..., description="New directory name (e.g. 'test_v2')")

class RenameLakehouseTableRequest(BaseModel):
    folder: str = Field(..., description="Folder containing the table (e.g. 'bronze')")
    old_name: str = Field(..., description="Current table name without .parquet extension")
    new_name: str = Field(..., description="New table name without .parquet extension")

class MoveLakehouseTableRequest(BaseModel):
    source_folder: str = Field(..., description="Source folder containing the table")
    target_folder: str = Field(..., description="Target destination folder")
    table_name: str = Field(..., description="Table name without .parquet extension")

def _bad_request(exc: InvalidTablePath) -> HTTPException:
    """A rejected name is the caller's mistake, so it is a 400 with the reason."""
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def _require_segment(name: str, what: str) -> str:
    """A table or folder name from a request, validated before it reaches any SQL."""
    try:
        return validate_segment(name, what=what)
    except InvalidTablePath as exc:
        raise _bad_request(exc) from exc


def _clean_lakehouse_path(raw_path: str) -> str:
    """A folder path from a request, normalised. ``..`` is refused, never rewritten."""
    try:
        return validate_folder(raw_path)
    except InvalidTablePath as exc:
        raise _bad_request(exc) from exc


def _folder_exists(rel_path: str) -> bool:
    """Whether the catalog still describes this folder, even with nothing on disk.

    The tree lists a folder the catalog describes even when its directory is gone — an
    empty folder is a schema with nothing in it — so deleting what the tree shows must not
    depend on a directory being there.
    """
    prefix = _lakehouse_schema(rel_path)
    conn = duckdb_engine.get_connection()
    try:
        schemas = lakehouse_schemas(conn)
    finally:
        conn.close()
    return any(
        schema == prefix or schema.startswith(f"{prefix}.") for schema in schemas
    )


def _ensure_lakehouse_directory(target_dir: Path) -> None:
    """Creates a directory and drops a .gitkeep when it is empty.

    Filesystem work, so callers hand it to a worker thread instead of the event loop.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    if not any(target_dir.iterdir()):
        (target_dir / ".gitkeep").touch(exist_ok=True)


def _prune_empty_dirs(directory: Path) -> None:
    """Removes ``directory`` and any subdirectory that holds no files.

    Kept as the one-call form the folder routes use: the shared helper wants the warehouse
    root as well, because a directory outside it must never be touched.
    """
    prune_empty_dirs(settings.lakehouse_root_path, directory)


@router.post("/lakehouse/directories")
async def create_lakehouse_directory(req: CreateLakehouseDirectoryRequest):
    """Creates a new directory or nested subfolder inside Lakehouse storage."""
    raw = req.path or req.name or ""
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Directory path or name is required.")
    rel_path = _clean_lakehouse_path(raw)
    target_dir = settings.lakehouse_root_path / rel_path
    await asyncio.to_thread(_ensure_lakehouse_directory, target_dir)
    invalidate_catalog_cache()
    return {"status": "created", "name": target_dir.name, "path": rel_path}

@router.put("/lakehouse/directories/rename")
async def rename_lakehouse_directory(
    req: RenameLakehouseDirectoryRequest,
    db: AsyncSession = Depends(get_db)
):
    """Renames a directory or subfolder and updates all associated metadata assets."""
    rel_path = _clean_lakehouse_path(req.path)
    new_name = _require_segment(req.new_name, "folder name")
    if not rel_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lakehouse root cannot be renamed.",
        )

    target_dir = settings.lakehouse_root_path / rel_path
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Directory '{rel_path}' does not exist."
        )

    dest_dir = target_dir.parent / new_name
    if dest_dir.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Directory '{dest_dir.name}' already exists in parent folder."
        )

    dest_rel_path = dest_dir.relative_to(settings.lakehouse_root_path).as_posix()
    moved, skipped = await asyncio.to_thread(
        _rename_directory_sync, rel_path, dest_rel_path
    )
    if skipped:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"{skipped} table(s) in '{rel_path}' would collide with a table of the same "
                f"name in '{dest_rel_path}'. Nothing was moved."
            ),
        )

    # The catalog now points at the new schemas. A folder that held tables is left where it
    # is: its Parquet belongs to old snapshots and is reclaimed by maintenance once those
    # expire, whereas moving it now would move files nothing points at. A folder with no
    # tables is pure scaffolding, so it is simply renamed.
    if moved:
        await asyncio.to_thread(_prune_empty_dirs, target_dir)
        await asyncio.to_thread(_ensure_lakehouse_directory, dest_dir)
    else:
        await asyncio.to_thread(target_dir.rename, dest_dir)

    await catalog_service.refresh(db)

    invalidate_catalog_cache()
    return {
        "status": "renamed",
        "old_path": rel_path,
        "new_path": dest_rel_path,
        "tables_moved": moved,
    }

@router.delete("/lakehouse/directories")
@router.delete("/lakehouse/directories/{dir_name:path}")
async def delete_lakehouse_directory(
    path: Optional[str] = None,
    dir_name: Optional[str] = None,
    cascade: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """Deletes a Lakehouse directory. If cascade=true, removes all nested tables and cleans metadata."""
    target_param = path or dir_name or ""
    if not target_param:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Path parameter is required.")
    rel_path = _clean_lakehouse_path(target_param)
    target_dir = settings.lakehouse_root_path / rel_path
    if not target_dir.is_dir() and not await asyncio.to_thread(_folder_exists, rel_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Directory '{rel_path}' does not exist in Lakehouse."
        )

    # The catalog decides what is in the folder, not a walk of it: DuckLake knows which
    # tables a schema holds, and a table's bytes are only reachable through the catalog.
    dropped = await asyncio.to_thread(_delete_directory_sync, rel_path, cascade)
    if dropped < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Directory '{rel_path}' contains {-dropped} table(s). "
                "Confirm deletion to remove the directory and its tables."
            ),
        )

    await asyncio.to_thread(shutil.rmtree, target_dir, True)
    # Rows follow the catalog: the projection drops the ones whose tables are gone.
    await catalog_service.refresh(db)
    invalidate_catalog_cache()
    return {"status": "deleted", "path": rel_path, "tables_dropped": dropped}

@router.put("/lakehouse/tables/rename")
async def rename_lakehouse_table(
    req: RenameLakehouseTableRequest,
    db: AsyncSession = Depends(get_db)
):
    """Renames a lakehouse table and refreshes the catalog."""
    clean_folder = _clean_lakehouse_path(req.folder)
    old_name = _require_segment(req.old_name, "table name")
    new_name = _require_segment(req.new_name, "table name")

    schema = _lakehouse_schema(clean_folder)
    outcome = await asyncio.to_thread(_rename_table_sync, schema, old_name, new_name)
    if outcome == "missing":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{old_name}' not found in folder '{clean_folder}'.",
        )
    if outcome == "exists":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table '{new_name}' already exists in folder '{clean_folder}'.",
        )

    await catalog_service.refresh(db)

    invalidate_catalog_cache()
    return {"status": "renamed", "old_name": old_name, "new_name": new_name}

@router.delete("/lakehouse/tables")
async def delete_lakehouse_table(
    folder: str,
    name: str,
    db: AsyncSession = Depends(get_db)
):
    """Drops a lakehouse table and refreshes the catalog."""
    clean_folder = _clean_lakehouse_path(folder)
    clean_name = _require_segment(name, "table name")
    schema = _lakehouse_schema(clean_folder)

    await asyncio.to_thread(_delete_table_sync, schema, clean_name)

    # Dropping the table is the whole operation: the row has nothing left to describe.
    await catalog_service.refresh(db)

    invalidate_catalog_cache()
    return {"status": "deleted", "folder": clean_folder, "name": clean_name}

@router.post("/lakehouse/tables/move")
async def move_lakehouse_table(
    req: MoveLakehouseTableRequest,
    db: AsyncSession = Depends(get_db)
):
    """Moves a lakehouse table between folders and refreshes the catalog."""
    src_folder = _clean_lakehouse_path(req.source_folder)
    dst_folder = _clean_lakehouse_path(req.target_folder)
    table_name = _require_segment(req.table_name, "table name")
    src_schema = _lakehouse_schema(src_folder)
    dst_schema = _lakehouse_schema(dst_folder)

    outcome = await asyncio.to_thread(_move_table_sync, src_schema, dst_schema, table_name)
    if outcome == "missing":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' does not exist in folder '{src_folder}'.",
        )
    if outcome == "exists":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table '{table_name}' already exists in folder '{dst_folder}'.",
        )

    # The table keeps its name and schema, so the catalog refresh is all that is left.
    await catalog_service.refresh(db)

    invalidate_catalog_cache()
    return {"status": "moved", "source": src_folder, "target": dst_folder, "table": table_name}

async def _introspect_connection(conn: Connection) -> Optional[List[Dict[str, Any]]]:
    """Live table list for one connection, dispatched by source type.

    Returns ``None`` — not an empty list — when the source cannot be reached or its
    type is unknown. The distinction matters: ``[]`` means "the source is empty, so
    its catalog rows should go", while ``None`` means "we could not look, so leave
    the catalog exactly as it was". A transient network blip must not erase what the
    registry knows.

    Shared by the catalog tree and the explicit reconcile endpoint so the two can
    never disagree about what a source contains.
    """
    plaintext_pw = decrypt(conn.encrypted_credentials)
    st = (conn.source_type or "").lower()
    try:
        if st == "postgres":
            sslmode = (conn.options or {}).get("sslmode", "disable")
            return await asyncio.to_thread(
                introspector.introspect_postgres,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
                sslmode=sslmode,
            )
        if st == "mongodb":
            auth_source = (conn.options or {}).get("auth_source", "admin")
            return await asyncio.to_thread(
                introspector.introspect_mongodb,
                host=conn.host,
                port=conn.port,
                username=conn.username,
                password=plaintext_pw,
                auth_db=auth_source,
                database_name=conn.database,
            )
        if st in ("mysql", "mariadb"):
            return await asyncio.to_thread(
                introspector.introspect_mysql,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
            )
        if st == "oracle":
            return await asyncio.to_thread(
                introspector.introspect_oracle,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
            )
        if st in ("api", "rest", "rest_api", "erp"):
            return await asyncio.to_thread(
                introspector.introspect_api,
                base_url=conn.host,
                endpoint=conn.database if conn.database else "",
                token=plaintext_pw,
                headers=(conn.options or {}).get("headers"),
                data_key=(conn.options or {}).get("data_key"),
            )
    except Exception:
        return None
    return None


def _lakehouse_table_entry(table: Any) -> Dict[str, Any]:
    """The tree/UI shape for one scanned table.

    Kept byte-for-byte compatible with what the introspector used to emit, so the
    frontend's ``CatalogTable`` contract does not change when the source of the data
    moves from a second walker to the catalog scan.
    """
    return {
        "name": table.name,
        "schema_name": table.folder.replace("/", ".") if table.folder else "main",
        "fqn": table.fqn,
        "sql_name": table.sql_name,
        "columns": [dict(column) for column in table.columns],
        "row_count": table.num_rows,
        "size_bytes": table.size_bytes,
        "size_human": format_bytes(table.size_bytes),
        "size_is_estimate": False,
        "file_path": table.path,
        "folder": table.folder,
        "modified_at": table.modified_at.isoformat(),
    }


def _schema_folders(conn) -> set[str]:
    """The folders the catalog owns, each with the parents a nested name implies.

    A folder is a schema, so ``raw`` and ``raw.nested`` are ``raw`` and ``raw/nested``. The
    parents are added too: a nested folder whose ``raw`` schema holds no tables is still a
    folder inside ``raw``, not a folder at the root.
    """
    folders: set[str] = set()
    for schema in lakehouse_schemas(conn):
        parts = schema.split(".")
        for depth in range(1, len(parts) + 1):
            folders.add("/".join(parts[:depth]))
    return folders


def _user_folders(root: Path) -> set[str]:
    """Folders a user made on purpose: the directories holding a ``.gitkeep``.

    Creating a folder through the API is what puts the marker there, so it is the one piece
    of on-disk evidence that a directory is a folder somebody asked for rather than
    something DuckLake made for a table.
    """
    folders: set[str] = set()
    if not root.is_dir():
        return folders
    for current, subdirs, files in os.walk(root):
        subdirs[:] = sorted(name for name in subdirs if not name.startswith("."))
        if ".gitkeep" not in files:
            continue
        directory = Path(current)
        if directory != root:
            folders.add(directory.relative_to(root).as_posix())
    return folders


def _lakehouse_folders_sync() -> List[str]:
    """The Lakehouse folders the tree shows, as relative POSIX paths.

    A walk of the warehouse cannot answer this. DuckLake gives every table a directory of
    its own (``<schema>/<table>``, with ``<table>__obelisk_load`` beside it while a run is
    loading), so a walk sees one folder per table and one per killed run — which is exactly
    what the phantom folders were.

    Two things are a folder, and the catalog answers both: a schema, which is what a folder
    is, and a directory a user marked with ``.gitkeep``. A load table's directory is
    excluded even when marked, because ``__obelisk_load`` names an ingest's working area
    rather than anything a user could have asked for.
    """
    root = settings.lakehouse_root_path
    conn = lakehouse_connection()
    try:
        folders = _schema_folders(conn)
    finally:
        conn.close()

    return sorted(
        (
            folder
            for folder in folders | _user_folders(root)
            if not folder.rsplit("/", 1)[-1].endswith(LOAD_TABLE_SUFFIX)
        ),
        key=str.lower,
    )


@router.get("/tree")
async def get_catalog_tree(
    refresh: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """
    Returns full data tree hierarchy:
    - Lakehouse root with all recursively discovered directories and subfolders
    - Registered external database sources (PostgreSQL, MongoDB, MySQL, Oracle, REST API)
    """
    global _catalog_tree_cache, _catalog_tree_cache_time
    now = time.time()
    if not refresh and _catalog_tree_cache is not None and (now - _catalog_tree_cache_time) < _CATALOG_CACHE_TTL:
        return _catalog_tree_cache

    tree: List[Dict[str, Any]] = []

    # 1. Lakehouse. The table list comes from the catalog and the folder list from the
    # schemas it describes, so the two cannot disagree about what exists — and a directory
    # DuckLake made for a table's files is never mistaken for a folder.
    store = warehouse_storage()
    scan_result, directories = await asyncio.gather(
        asyncio.to_thread(scan, store),
        asyncio.to_thread(_lakehouse_folders_sync),
    )
    await catalog_service.refresh(db, scan_result)

    tables_by_dir: Dict[str, List[Dict[str, Any]]] = {}
    for table in scan_result.tables:
        # A nested folder is one schema with dots in its name (``raw/nested`` is
        # ``raw.nested``), so the folder a table belongs to is its schema with slashes.
        tables_by_dir.setdefault(table.folder.replace(".", "/"), []).append(
            _lakehouse_table_entry(table)
        )

    lakehouse_directories: List[Dict[str, Any]] = []
    all_lakehouse_tables: List[Dict[str, Any]] = []

    for rel_posix in directories:
        parts = rel_posix.split("/")
        dir_tables = tables_by_dir.get(rel_posix, [])
        lakehouse_directories.append({
            "id": f"lakehouse:{rel_posix}",
            "name": parts[-1],
            "tier": rel_posix,
            "path": rel_posix,
            "parent_path": "/".join(parts[:-1]) or None,
            "depth": len(parts) - 1,
            "tables": dir_tables,
        })
        all_lakehouse_tables.extend(dir_tables)

    root_flat_tables = tables_by_dir.get("", [])
    if root_flat_tables:
        lakehouse_directories.insert(0, {
            "id": "lakehouse:root",
            "name": "root",
            "tier": "main",
            "path": "main",
            "parent_path": None,
            "depth": 0,
            "tables": root_flat_tables,
        })
        all_lakehouse_tables.extend(root_flat_tables)

    # Top-level Lakehouse node
    tree.append({
        "id": "lakehouse",
        "name": "Lakehouse",
        "type": "lakehouse",
        "storage_uri": settings.STORAGE_URI,
        "directories": lakehouse_directories,
        "tables": all_lakehouse_tables,
        "problems": list(scan_result.problems),
    })

    # 2. Registered external sources (Concurrent Introspection)
    stmt = select(Connection)
    result = await db.execute(stmt)
    conns = result.scalars().all()

    async def _introspect_single_conn(conn: Connection) -> Dict[str, Any]:
        fetched = await _introspect_connection(conn)
        source_tables = list(fetched or [])
        source_alias = connection_alias(conn.name)
        for t in source_tables:
            s_name = t.get("schema_name") or "public"
            raw_name = t.get("name") or "table"
            clean_tname = raw_name[len(s_name) + 1:] if raw_name.startswith(f"{s_name}.") else raw_name
            t["name"] = clean_tname
            t["schema_name"] = s_name
            t["fqn"] = f"{source_alias}.{s_name}.{clean_tname}"

        try:
            driver = resolve_source(conn).driver
        except UnsupportedSource:
            driver = conn.driver or "unknown"

        node = {
            "id": conn.id,
            "name": conn.name,
            "type": conn.source_type,
            "driver": driver,
            "host": conn.host,
            "port": conn.port,
            "database": conn.database,
            "tables": source_tables,
        }
        if fetched is None:
            # Unreachable is not the same as empty: say so, and change nothing.
            node["catalog_error"] = "source unreachable — catalog rows left untouched"
        return node

    if conns:
        conn_nodes = await asyncio.gather(*[_introspect_single_conn(c) for c in conns])
        tree.extend(conn_nodes)

        # The tree refresh doubles as the reconcile: the tables are already in hand,
        # so recording them costs no extra round trip. A failure is reported on the
        # node rather than silently leaving the registry stale.
        for conn, node in zip(conns, conn_nodes):
            if node.get("catalog_error"):
                continue
            try:
                await catalog_service.reconcile_connection(db, conn, node["tables"])
            except Exception as exc:  # one broken source must not break the tree
                node["catalog_error"] = str(exc)

    _catalog_tree_cache = tree
    _catalog_tree_cache_time = time.time()
    return tree

@router.get("/preview/{path_or_source:path}")
async def get_table_preview(
    path_or_source: str,
    schema: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Returns live 50-row preview from Lakehouse Parquet (any directory/subfolder) or external source database.

    ``schema`` is the namespace the table lives in. The catalog always has it — a table is
    only unique inside one — and without it an external preview reads ``public.<name>``,
    which is how a healthy table in ``sales`` answered "relation does not exist".
    """
    parts = path_or_source.rsplit("/", 1)
    if len(parts) == 2:
        source_id, table_name = parts[0], parts[1]
    else:
        source_id, table_name = "lakehouse", parts[0]

    # Table names arrive straight from the URL, so reject separators, glob
    # characters and parent references before anything touches disk (audit H6).
    _stem = table_name[:-len(".parquet")] if table_name.lower().endswith(".parquet") else table_name
    if not _stem or any(token in _stem for token in ("/", "\\", "*", "?", "..")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table name.",
        )
    table_name = _stem

    # 1. Lakehouse Parquet preview. The identifier after "lakehouse:" is a folder
    # path; "root"/"main"/empty all mean the warehouse root.
    lakehouse_root = settings.lakehouse_root_path
    raw_tier = source_id.split(":", 1)[1] if source_id.startswith("lakehouse:") else source_id
    is_root = raw_tier in ("root", "main", "", "lakehouse")

    tier_dir: Optional[Path] = None
    if is_root:
        clean_tier = ""
    else:
        clean_tier = _clean_lakehouse_path(raw_tier)
        if (lakehouse_root / clean_tier).is_dir():
            tier_dir = lakehouse_root / clean_tier

    if (
        source_id == "lakehouse"
        or source_id.startswith("lakehouse:")
        or is_root
        or tier_dir is not None
    ):
        # The table's address comes from the catalog, not from a file: DuckLake decides
        # which files belong to it. Nothing is resolved from the filesystem here, so there
        # is no path to escape — and the folder is part of the match, because `raw/orders`
        # and `marts/orders` are two different tables.
        from app.core.catalog import scan as scan_lakehouse

        catalogue = await asyncio.to_thread(scan_lakehouse)
        # `folder` on a scanned table is the schema's spelling, where a nested folder is one
        # dotted name (`raw/nested` is `raw.nested`), so the two are compared as schemas.
        wanted_folder = "" if is_root else _lakehouse_schema(clean_tier)
        match = next(
            (
                t
                for t in catalogue.tables
                if t.name == table_name and (t.folder or "") == wanted_folder
            ),
            None,
        )
        if match is None and is_root:
            # A bare name is a convenience, and only while it names exactly one table.
            candidates = [t for t in catalogue.tables if t.name == table_name]
            if len(candidates) == 1:
                match = candidates[0]
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"Table '{table_name}' does not exist in folder "
                    f"'{wanted_folder or 'root'}'."
                ),
            )

        query = f"SELECT * FROM {match.sql_name} LIMIT 50;"
        res = await asyncio.to_thread(duckdb_engine.execute_query, query)
        return res

    # External source database
    stmt = select(Connection).where(Connection.id == source_id)
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection '{source_id}' not found."
        )

    plaintext_pw = decrypt(conn.encrypted_credentials)
    st = conn.source_type.lower()

    # The schema travels with the request, because a table name alone is only unique inside
    # one. The catalog hands out the pair (`sales.sales_order_line` is schema `sales`, table
    # `sales_order_line`); `schema` is the explicit form of that, and a dotted name in the
    # path is the older spelling of the same thing. Query Studio was never affected because
    # it is given the fully-qualified name.
    table_schema = (schema or "").strip() or None
    if table_schema is None and "." in table_name:
        table_schema, table_name = table_name.split(".", 1)
        if not table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid table name.",
            )

    try:
        if st == "postgres":
            sslmode = (conn.options or {}).get("sslmode", "disable")
            preview = await asyncio.to_thread(
                introspector.preview_postgres,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
                table=table_name,
                schema=table_schema or "public",
                sslmode=sslmode,
            )
            return preview
        elif st == "mongodb":
            auth_source = (conn.options or {}).get("auth_source", "admin")
            preview = await asyncio.to_thread(
                introspector.preview_mongodb,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                collection=table_name,
                username=conn.username,
                password=plaintext_pw,
                auth_db=auth_source,
            )
            return preview
        elif st in ("mysql", "mariadb"):
            preview = await asyncio.to_thread(
                introspector.preview_mysql,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
                table=table_name,
                schema=table_schema,
            )
            return preview
        elif st == "oracle":
            preview = await asyncio.to_thread(
                introspector.preview_oracle,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                username=conn.username,
                password=plaintext_pw,
                table=table_name,
                schema=table_schema,
            )
            return preview
        elif st in ("api", "rest", "rest_api", "erp"):
            preview = await asyncio.to_thread(
                introspector.preview_api,
                base_url=conn.host,
                endpoint=table_name if table_name != "api_records" else "",
                token=plaintext_pw,
                headers=(conn.options or {}).get("headers"),
                data_key=(conn.options or {}).get("data_key"),
            )
            return preview
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported source type '{conn.source_type}' for preview."
            )
    except HTTPException:
        raise
    except Exception as e:
        # The source answered with a refusal (a missing table, a permission, a dead port):
        # that is the upstream's verdict, not an Obelisk bug, so it is reported as 502 and
        # the driver's own words are handed to the UI instead of a bare "status code 500".
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"{conn.name} could not answer the preview: {str(e)}",
        )


# -- lakehouse diagnostics and maintenance ------------------------------------
#
# The Settings tabs need two things the lakehouse could only tell a shell script: what it
# currently costs, and a way to compact and reclaim it. Both are one connection and a few
# catalog queries, so both are handed to a worker thread — a DuckDB call on the event loop
# would stall every other request (audit: async routes must not block).


def _file_bytes(path: Path) -> int:
    """Size of one file, or 0 when it is not there. A missing store is not an error."""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _directory_bytes(path: Path) -> int:
    """Total size of the files under a directory, skipping anything unreadable."""
    total = 0
    if not path.is_dir():
        return 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_size
        except OSError:
            continue
    return total


def _catalog_counts(conn) -> Dict[str, int]:
    """What the catalog holds right now: snapshots, live tables, data files and bytes."""
    snapshots = int(
        conn.execute(f"SELECT count(*) FROM ducklake_snapshots('{SOURCE}')").fetchall()[0][0]
    )
    return {
        "snapshots": snapshots,
        "active_tables": len(lakehouse_tables(conn)),
        **_active_file_totals(conn),
    }


def _active_file_totals(conn) -> Dict[str, int]:
    row = conn.execute(
        f"SELECT count(*), coalesce(sum(file_size_bytes), 0) "
        f"FROM __ducklake_metadata_{SOURCE}.ducklake_data_file "
        f"WHERE end_snapshot IS NULL"
    ).fetchall()[0]
    return {"data_files": int(row[0]), "data_bytes": int(row[1] or 0)}


def _sqlite_journal_mode(path: Path) -> str:
    """The journal mode the control-plane DB actually runs in.

    Asked read-only, because the Settings tab should show what SQLite answers rather
    than what the code intended to configure: "WAL is on" and "WAL was requested" are
    different statements, and the difference is the concurrency the operator thinks
    they have.
    """
    if not path.is_file():
        return "missing"
    conn = None
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        row = conn.execute("PRAGMA journal_mode").fetchone()
    except sqlite3.Error:
        return "unknown"
    finally:
        if conn is not None:
            conn.close()
    return str(row[0]).upper() if row else "unknown"


def _lakehouse_diagnostics_sync() -> Dict[str, Any]:
    """Everything the Storage & Diagnostics tabs show, in one connection."""
    root = settings.lakehouse_root_path
    conn = lakehouse_connection()
    try:
        counts = _catalog_counts(conn)
        latest_snapshot = conn.execute(
            f"SELECT max(snapshot_id) FROM ducklake_snapshots('{SOURCE}')"
        ).fetchall()[0][0]
        orphans = len(load_tables(conn))
    finally:
        conn.close()
    return {
        "storage_uri": settings.STORAGE_URI,
        "lakehouse_root": str(root),
        "catalog_sqlite_bytes": _file_bytes(Path(settings.lakehouse_catalog_path)),
        "obelisk_db_bytes": _file_bytes(Path(settings.sqlite_db_path)),
        "obelisk_db_journal_mode": _sqlite_journal_mode(Path(settings.sqlite_db_path)),
        "duckdb_tmp_bytes": _directory_bytes(settings.duckdb_temp_path),
        "duckdb_tmp_limit": advisor.get_duckdb_temp_limit(
            settings.duckdb_temp_path.as_posix(), settings.DUCKDB_TEMP_SIZE_LIMIT
        ),
        "duckdb_memory_limit_per_slot": advisor.get_duckdb_memory_limit(
            settings.DUCKDB_MAX_CONCURRENT_HEAVY
        ),
        "duckdb_max_query_rows": settings.DUCKDB_MAX_QUERY_ROWS,
        "snapshot_count": counts["snapshots"],
        "current_snapshot_id": int(latest_snapshot) if latest_snapshot is not None else None,
        "active_table_count": counts["active_tables"],
        "active_data_files": counts["data_files"],
        "total_data_bytes": counts["data_bytes"],
        # A load table is a run that was killed: the tab shows it so a user can see what a
        # crash left behind before maintenance reclaims it.
        "orphaned_load_tables": orphans,
    }


class LakehouseMaintainRequest(BaseModel):
    """How much history to keep, and whether to delete the bytes it held."""

    keep_days: int = Field(
        default=7,
        ge=1,
        le=365,
        description="Snapshots older than this are expired; time travel beyond it is gone.",
    )
    delete_files: bool = Field(
        default=True,
        description=(
            "Also delete files no snapshot references. Irreversible: keep it on for a "
            "reclaim, turn it off to only compact and expire."
        ),
    )


def _maintain_lakehouse_sync(keep_days: int, delete_files: bool) -> Dict[str, Any]:
    """Compacts, expires and reclaims, and reports what each step did.

    The order matters. Load tables go first: a killed run's leftovers reference bytes that
    nothing will ever read, and expiring snapshots before dropping them would keep those
    bytes alive for another retention window. The empty-directory sweep goes last, because
    it is what the previous steps leave behind.
    """
    conn = lakehouse_connection()
    try:
        before = _catalog_counts(conn)
        orphans = drop_load_tables(conn)

        merged = conn.execute(
            f"CALL ducklake_merge_adjacent_files('{SOURCE}');"
        ).fetchall()

        older_than = datetime.now(timezone.utc) - timedelta(days=keep_days)
        expired = conn.execute(
            f"CALL ducklake_expire_snapshots('{SOURCE}', "
            f"older_than => TIMESTAMPTZ '{older_than.isoformat()}');"
        ).fetchall()

        removed: List[Any] = []
        if delete_files:
            removed = conn.execute(
                f"CALL ducklake_cleanup_old_files('{SOURCE}', cleanup_all => true);"
            ).fetchall()

        after = _catalog_counts(conn)
    finally:
        conn.close()

    # 4. Sweep the directories those drops and expiries emptied. The connection is closed
    #    first: a directory with a file handle still open on it cannot be removed here.
    pruned = prune_empty_dirs(settings.lakehouse_root_path)

    return {
        "status": "ok",
        "orphaned_load_tables_dropped": len(orphans),
        "tables_compacted": len(merged),
        "snapshots_expired": len(expired),
        "files_removed": len(removed),
        "empty_directories_removed": pruned,
        "delete_files": delete_files,
        "keep_days": keep_days,
        "before": before,
        "after": after,
    }


@router.get("/lakehouse/diagnostics")
async def get_lakehouse_diagnostics() -> Dict[str, Any]:
    """Live storage and catalog facts for the Settings → Storage & Diagnostics tabs."""
    return await asyncio.to_thread(_lakehouse_diagnostics_sync)


@router.post("/lakehouse/maintain")
async def maintain_lakehouse(req: LakehouseMaintainRequest) -> Dict[str, Any]:
    """Compacts small files, expires old snapshots and reclaims orphaned bytes.

    Run it when no ingest is running. Deleting files is irreversible, so the response
    reports the snapshot and file counts before and after rather than just \"done\".
    """
    return await asyncio.to_thread(
        _maintain_lakehouse_sync, req.keep_days, req.delete_files
    )

