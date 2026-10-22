import asyncio
import time
import re
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from app.database import get_db
from app.config import settings
from app.core.vault import encrypt, decrypt
from app.core.host_resolver import resolve_connection_host
from app.models.connection import Connection
from app.models.asset import Asset
from app.engines.introspector import introspector
from app.engines.duckdb_engine import duckdb_engine

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

class UpdateConnectionRequest(BaseModel):
    name: Optional[str] = None
    source_type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

class ConnectionResponse(BaseModel):
    id: str
    name: str
    source_type: str
    host: str
    port: int
    database: str
    username: str
    options: Optional[Dict[str, Any]] = None
    created_at: str

@router.post("/connections", response_model=ConnectionResponse)
async def create_connection(
    req: CreateConnectionRequest,
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
    new_conn = Connection(
        name=clean_name,
        source_type=req.source_type.lower().strip(),
        host=req.host.strip(),
        port=req.port,
        database=req.database.strip(),
        username=req.username.strip(),
        encrypted_credentials=encrypted_pw,
        options=req.options,
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

    return ConnectionResponse(
        id=new_conn.id,
        name=new_conn.name,
        source_type=new_conn.source_type,
        host=new_conn.host,
        port=new_conn.port,
        database=new_conn.database,
        username=new_conn.username,
        options=new_conn.options,
        created_at=new_conn.created_at.isoformat(),
    )

@router.get("/connections", response_model=List[ConnectionResponse])
async def list_connections(db: AsyncSession = Depends(get_db)):
    """Lists registered external connections."""
    stmt = select(Connection)
    result = await db.execute(stmt)
    conns = result.scalars().all()
    return [
        ConnectionResponse(
            id=c.id,
            name=c.name,
            source_type=c.source_type,
            host=c.host,
            port=c.port,
            database=c.database,
            username=c.username,
            options=c.options,
            created_at=c.created_at.isoformat(),
        )
        for c in conns
    ]

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
            if username and password:
                uri = f"mongodb://{username}:{password}@{resolved_host}:{port}/?authSource={auth_source}"
            else:
                uri = f"mongodb://{resolved_host}:{port}/"
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
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
    return _execute_live_test(
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

    await db.commit()
    await db.refresh(conn)
    invalidate_catalog_cache()

    return ConnectionResponse(
        id=conn.id,
        name=conn.name,
        source_type=conn.source_type,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        options=conn.options,
        created_at=conn.created_at.isoformat(),
    )

@router.delete("/connections/{conn_id}")
async def delete_connection(
    conn_id: str,
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

    await db.delete(conn)
    await db.commit()
    invalidate_catalog_cache()
    return {"status": "deleted", "id": conn_id}

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

def _validate_segment(name: str) -> bool:
    return bool(name and re.match(r"^[a-zA-Z0-9_\-]+$", name))

def _clean_lakehouse_path(raw_path: str) -> str:
    parts = [p.strip() for p in raw_path.replace("\\", "/").split("/") if p.strip()]
    if not parts or any(p in (".", "..") or not _validate_segment(p) for p in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid path. Folder names must be alphanumeric and may only contain dashes or underscores."
        )
    return "/".join(parts)


def _layer_for_folder(folder: str) -> str:
    """Maps a lakehouse folder to the `Asset.layer` value it is catalogued under.

    Root-level Parquet files are registered with layer "main" (the /tree endpoint
    labels the flat root tier "main"), while callers may address that folder as
    "root" — the same alias the file lookups above already accept. Now that
    (layer, name) is the uniqueness key, this filter must be exact.
    """
    return "main" if folder == "root" else folder


@router.post("/lakehouse/directories")
async def create_lakehouse_directory(req: CreateLakehouseDirectoryRequest):
    """Creates a new directory or nested subfolder inside Lakehouse storage."""
    raw = req.path or req.name or ""
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Directory path or name is required.")
    rel_path = _clean_lakehouse_path(raw)
    target_dir = settings.lakehouse_root_path / rel_path
    target_dir.mkdir(parents=True, exist_ok=True)
    gitkeep = target_dir / ".gitkeep"
    if not any(target_dir.iterdir()):
        gitkeep.touch(exist_ok=True)
    invalidate_catalog_cache()
    return {"status": "created", "name": target_dir.name, "path": rel_path}

@router.put("/lakehouse/directories/rename")
async def rename_lakehouse_directory(
    req: RenameLakehouseDirectoryRequest,
    db: AsyncSession = Depends(get_db)
):
    """Renames a directory or subfolder and updates all associated metadata assets."""
    rel_path = _clean_lakehouse_path(req.path)
    new_name = req.new_name.strip()
    if not _validate_segment(new_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New folder name must be alphanumeric and may only contain dashes or underscores."
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
    target_dir.rename(dest_dir)

    # Synchronize SQLite metadata brain
    stmt = select(Asset)
    result = await db.execute(stmt)
    all_assets = result.scalars().all()
    for asset in all_assets:
        if asset.layer == rel_path:
            asset.layer = dest_rel_path
            asset.storage_uri = asset.storage_uri.replace(f"/{rel_path}/", f"/{dest_rel_path}/")
        elif asset.layer.startswith(f"{rel_path}/"):
            sub_part = asset.layer[len(rel_path):]
            asset.layer = f"{dest_rel_path}{sub_part}"
            asset.storage_uri = asset.storage_uri.replace(f"/{rel_path}/", f"/{dest_rel_path}/")
    await db.commit()

    invalidate_catalog_cache()
    return {"status": "renamed", "old_path": rel_path, "new_path": dest_rel_path}

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
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Directory '{rel_path}' does not exist in Lakehouse."
        )

    parquet_files = list(target_dir.rglob("*.parquet"))
    if parquet_files and not cascade:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Directory '{rel_path}' contains {len(parquet_files)} Parquet table(s). Confirm deletion to remove directory and its tables."
        )

    # Delete matching assets from SQLite brain
    stmt = select(Asset)
    result = await db.execute(stmt)
    all_assets = result.scalars().all()
    for asset in all_assets:
        if asset.layer == rel_path or asset.layer.startswith(f"{rel_path}/"):
            await db.delete(asset)
    await db.commit()

    shutil.rmtree(target_dir, ignore_errors=True)
    invalidate_catalog_cache()
    return {"status": "deleted", "path": rel_path}

@router.put("/lakehouse/tables/rename")
async def rename_lakehouse_table(
    req: RenameLakehouseTableRequest,
    db: AsyncSession = Depends(get_db)
):
    """Renames a Parquet table file and synchronizes the metadata asset in SQLite."""
    clean_folder = _clean_lakehouse_path(req.folder)
    old_name = req.old_name.strip()
    new_name = req.new_name.strip()
    if not _validate_segment(new_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New table name must be alphanumeric and may only contain dashes or underscores."
        )

    table_dir = settings.lakehouse_root_path / clean_folder
    old_file = table_dir / f"{old_name}.parquet"
    if not old_file.exists() and clean_folder in ("root", "main"):
        old_file = settings.lakehouse_root_path / f"{old_name}.parquet"
        table_dir = settings.lakehouse_root_path
    new_file = table_dir / f"{new_name}.parquet"

    if not old_file.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{old_name}' not found in folder '{clean_folder}'."
        )
    if new_file.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table '{new_name}' already exists in folder '{clean_folder}'."
        )

    old_file.rename(new_file)

    # Update metadata brain
    stmt = select(Asset).where(Asset.name == old_name, Asset.layer == _layer_for_folder(clean_folder))
    result = await db.execute(stmt)
    asset = result.scalar_one_or_none()
    if asset:
        asset.name = new_name
        asset.storage_uri = asset.storage_uri.replace(f"{old_name}.parquet", f"{new_name}.parquet")
        await db.commit()

    invalidate_catalog_cache()
    return {"status": "renamed", "old_name": old_name, "new_name": new_name}

@router.delete("/lakehouse/tables")
async def delete_lakehouse_table(
    folder: str,
    name: str,
    db: AsyncSession = Depends(get_db)
):
    """Deletes a Parquet table file and cleans up its asset record from the metadata brain."""
    clean_folder = _clean_lakehouse_path(folder)
    clean_name = name.strip()
    table_file = settings.lakehouse_root_path / clean_folder / f"{clean_name}.parquet"
    if not table_file.exists() and clean_folder in ("root", "main"):
        table_file = settings.lakehouse_root_path / f"{clean_name}.parquet"

    if table_file.exists():
        table_file.unlink()

    # Clean from SQLite
    stmt = select(Asset).where(Asset.name == clean_name, Asset.layer == _layer_for_folder(clean_folder))
    result = await db.execute(stmt)
    asset = result.scalar_one_or_none()
    if asset:
        await db.delete(asset)
        await db.commit()

    invalidate_catalog_cache()
    return {"status": "deleted", "folder": clean_folder, "name": clean_name}

@router.post("/lakehouse/tables/move")
async def move_lakehouse_table(
    req: MoveLakehouseTableRequest,
    db: AsyncSession = Depends(get_db)
):
    """Moves a Parquet table between Lakehouse folders and updates its asset metadata."""
    src_folder = _clean_lakehouse_path(req.source_folder)
    dst_folder = _clean_lakehouse_path(req.target_folder)
    table_name = req.table_name.strip()

    src_file = settings.lakehouse_root_path / src_folder / f"{table_name}.parquet"
    dst_dir = settings.lakehouse_root_path / dst_folder
    dst_file = dst_dir / f"{table_name}.parquet"

    if not src_file.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' does not exist in folder '{src_folder}'."
        )

    dst_dir.mkdir(parents=True, exist_ok=True)
    if dst_file.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table '{table_name}' already exists in folder '{dst_folder}'."
        )

    shutil.move(src_file, dst_file)

    # Update metadata brain
    stmt = select(Asset).where(Asset.name == table_name, Asset.layer == _layer_for_folder(src_folder))
    result = await db.execute(stmt)
    asset = result.scalar_one_or_none()
    if asset:
        asset.layer = dst_folder
        asset.storage_uri = asset.storage_uri.replace(f"/{src_folder}/", f"/{dst_folder}/")
        await db.commit()

    invalidate_catalog_cache()
    return {"status": "moved", "source": src_folder, "target": dst_folder, "table": table_name}

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

    # 1. Discover all Lakehouse directories and nested subfolders
    lakehouse_root = settings.lakehouse_root_path
    lakehouse_root.mkdir(parents=True, exist_ok=True)

    # Introspect any flat Parquet tables residing directly in lakehouse root
    root_flat_tables = await asyncio.to_thread(introspector.introspect_parquet, str(lakehouse_root), "main")

    all_dirs = []
    for p in sorted(lakehouse_root.rglob("*")):
        if p.is_dir():
            rel_parts = p.relative_to(lakehouse_root).parts
            if not any(part.startswith(".") for part in rel_parts):
                all_dirs.append(p)

    all_dirs.sort(key=lambda d: d.relative_to(lakehouse_root).as_posix().lower())

    lakehouse_directories: List[Dict[str, Any]] = []
    all_lakehouse_tables: List[Dict[str, Any]] = []

    for d in all_dirs:
        rel_posix = d.relative_to(lakehouse_root).as_posix()
        parent_posix = d.parent.relative_to(lakehouse_root).as_posix() if d.parent != lakehouse_root else None
        depth = len(d.relative_to(lakehouse_root).parts) - 1

        dir_tables = await asyncio.to_thread(introspector.introspect_parquet, str(d), rel_posix)
        dir_entry = {
            "id": f"lakehouse:{rel_posix}",
            "name": d.name,
            "tier": rel_posix,
            "path": rel_posix,
            "parent_path": parent_posix,
            "depth": depth,
            "tables": dir_tables,
        }
        lakehouse_directories.append(dir_entry)
        all_lakehouse_tables.extend(dir_tables)

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
    })

    # 2. Registered external sources (Concurrent Introspection)
    stmt = select(Connection)
    result = await db.execute(stmt)
    conns = result.scalars().all()

    async def _introspect_single_conn(conn: Connection) -> Dict[str, Any]:
        plaintext_pw = decrypt(conn.encrypted_credentials)
        source_tables: List[Dict[str, Any]] = []
        try:
            st = conn.source_type.lower()
            if st == "postgres":
                sslmode = (conn.options or {}).get("sslmode", "disable")
                source_tables = await asyncio.to_thread(
                    introspector.introspect_postgres,
                    host=conn.host,
                    port=conn.port,
                    database=conn.database,
                    username=conn.username,
                    password=plaintext_pw,
                    sslmode=sslmode,
                )
            elif st == "mongodb":
                auth_source = (conn.options or {}).get("auth_source", "admin")
                source_tables = await asyncio.to_thread(
                    introspector.introspect_mongodb,
                    host=conn.host,
                    port=conn.port,
                    username=conn.username,
                    password=plaintext_pw,
                    auth_db=auth_source,
                    database_name=conn.database,
                )
            elif st in ("mysql", "mariadb"):
                source_tables = await asyncio.to_thread(
                    introspector.introspect_mysql,
                    host=conn.host,
                    port=conn.port,
                    database=conn.database,
                    username=conn.username,
                    password=plaintext_pw,
                )
            elif st == "oracle":
                source_tables = await asyncio.to_thread(
                    introspector.introspect_oracle,
                    host=conn.host,
                    port=conn.port,
                    database=conn.database,
                    username=conn.username,
                    password=plaintext_pw,
                )
            elif st in ("api", "rest", "rest_api", "erp"):
                source_tables = await asyncio.to_thread(
                    introspector.introspect_api,
                    base_url=conn.host,
                    endpoint=conn.database if conn.database else "",
                    token=plaintext_pw,
                    headers=(conn.options or {}).get("headers"),
                    data_key=(conn.options or {}).get("data_key"),
                )
        except Exception:
            source_tables = []

        import re
        source_alias = re.sub(r'[^a-zA-Z0-9_]', '_', conn.name.lower()).strip('_') or 'external'
        for t in source_tables:
            s_name = t.get("schema_name") or "public"
            raw_name = t.get("name") or "table"
            clean_tname = raw_name[len(s_name) + 1:] if raw_name.startswith(f"{s_name}.") else raw_name
            t["name"] = clean_tname
            t["schema_name"] = s_name
            t["fqn"] = f"{source_alias}.{s_name}.{clean_tname}"

        return {
            "id": conn.id,
            "name": conn.name,
            "type": conn.source_type,
            "host": conn.host,
            "port": conn.port,
            "database": conn.database,
            "tables": source_tables,
        }

    if conns:
        conn_nodes = await asyncio.gather(*[_introspect_single_conn(c) for c in conns])
        tree.extend(conn_nodes)

    _catalog_tree_cache = tree
    _catalog_tree_cache_time = time.time()
    return tree

@router.get("/preview/{path_or_source:path}")
async def get_table_preview(
    path_or_source: str,
    db: AsyncSession = Depends(get_db)
):
    """Returns live 50-row preview from Lakehouse Parquet (any directory/subfolder) or external source database."""
    parts = path_or_source.rsplit("/", 1)
    if len(parts) == 2:
        source_id, table_name = parts[0], parts[1]
    else:
        source_id, table_name = "lakehouse", parts[0]

    # 1. Lakehouse Parquet preview
    lakehouse_root = settings.lakehouse_root_path
    clean_tier = source_id.split(":", 1)[1] if source_id.startswith("lakehouse:") else source_id

    if (
        source_id == "bronze"
        or source_id == "lakehouse"
        or source_id.startswith("lakehouse:")
        or clean_tier in ("root", "main", "")
        or (lakehouse_root / clean_tier).is_dir()
    ):
        parquet_file = None

        if clean_tier in ("root", "main", ""):
            candidate = lakehouse_root / f"{table_name}.parquet"
            if candidate.exists():
                parquet_file = candidate

        if not parquet_file and (source_id.startswith("lakehouse:") or (lakehouse_root / clean_tier).is_dir()):
            candidate = lakehouse_root / clean_tier / f"{table_name}.parquet"
            if candidate.exists():
                parquet_file = candidate

        if not parquet_file and source_id == "bronze":
            candidate = lakehouse_root / "bronze" / f"{table_name}.parquet"
            if candidate.exists():
                parquet_file = candidate
            else:
                candidate2 = Path(settings.duckdb_storage_path) / f"{table_name}.parquet"
                if candidate2.exists():
                    parquet_file = candidate2

        # Fallbacks: check directly in lakehouse root or search all subdirectories
        if not parquet_file or not parquet_file.exists():
            flat = lakehouse_root / f"{table_name}.parquet"
            if flat.exists():
                parquet_file = flat
            else:
                for candidate in lakehouse_root.rglob(f"{table_name}.parquet"):
                    if candidate.is_file():
                        parquet_file = candidate
                        break

        if not parquet_file or not parquet_file.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Parquet table '{table_name}' does not exist in Lakehouse."
            )

        escaped_path = parquet_file.as_posix()
        query = f"SELECT * FROM read_parquet('{escaped_path}') LIMIT 50;"
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
    try:
        st = conn.source_type.lower()
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
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch preview from source: {str(e)}"
        )

