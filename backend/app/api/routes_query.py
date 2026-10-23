import re
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.database import get_db
from app.models.query import QueryHistory, OpenQueryTab
from app.models.connection import Connection
from app.models.service_config import ServiceConfig
from app.core.vault import decrypt
from app.engines.duckdb_engine import duckdb_engine
from app.engines.native_query_engine import native_query_engine
from app.core.docker_manager import docker_mgr
from app.core.host_resolver import resolve_connection_host
from app.core.sources import UnsupportedSource, resolve_source
from app.core.warehouse_resolver import resolve_warehouse_postgres_params

router = APIRouter(prefix="/api/query", tags=["query"])

class QueryTarget(BaseModel):
    id: str
    name: str
    engine: str # duckdb, postgres, mysql, clickhouse, etc.
    is_default: bool = False
    description: Optional[str] = None
    capabilities: List[str] = []
    status: str = "ready" # ready, running, connected, stopped
    schemas: List[str] = []
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None

class QueryExecuteRequest(BaseModel):
    sql: str = Field(..., description="SQL statement to execute")
    title: Optional[str] = Field(None, description="Optional title for saved query")
    timeout_seconds: int = Field(30, description="Query execution timeout in seconds")
    target: Optional[str] = Field("duckdb", description="Execution target ('duckdb', 'service:<name>', 'connection:<id>')")
    schema_name: Optional[str] = Field(None, description="Active schema context for relational engines")
    postgres_attachments: Optional[List[Dict[str, Any]]] = Field(
        None, description="Optional Postgres sources to attach dynamically in DuckDB"
    )

class QueryExecuteResponse(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    duration_ms: int
    status: str
    command_tag: Optional[str] = None
    target_engine: Optional[str] = "duckdb"
    target_name: Optional[str] = "DuckDB"

class QueryExportRequest(BaseModel):
    sql: str = Field(..., description="SQL statement to export")
    format: str = Field("parquet", description="Export format: 'parquet' or 'csv'")
    filename: Optional[str] = Field("query_export", description="Base download filename")
    postgres_attachments: Optional[List[Dict[str, Any]]] = Field(
        None, description="Optional Postgres sources to attach dynamically"
    )

def _is_connection_referenced(sql: str, alias: str, db_name: Optional[str] = None) -> bool:
    """Checks whether an external database alias or name is referenced in the SQL statement."""
    clean_sql = re.sub(r"--[^\n]*", "", sql)
    clean_sql = re.sub(r"/\*.*?\*/", "", clean_sql, flags=re.DOTALL)
    tokens = set(re.findall(r"[a-zA-Z0-9_]+", clean_sql.lower()))
    if alias.lower() in tokens:
        return True
    if db_name and db_name.lower() in tokens:
        return True
    return False

async def _get_default_pg_attachments(db: AsyncSession, sql: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetches every source Obelisk can attach live in DuckDB.
    If sql is provided, only attaches sources that are actually referenced in the query.

    Attachability is a property of the connection's recipe (app.core.sources), not a
    hardcoded source type, so a new attachable driver works here without an edit.
    """
    attachments = []
    try:
        stmt = select(Connection)
        conns = (await db.execute(stmt)).scalars().all()
        for c in conns:
            try:
                recipe = resolve_source(c)
            except UnsupportedSource:
                continue
            if not recipe.is_attachable:
                continue

            alias = recipe.alias
            if sql is not None and not _is_connection_referenced(sql, alias, c.database):
                continue
            pw = decrypt(c.encrypted_credentials)
            sslmode = (c.options or {}).get("sslmode", "disable")
            connect_timeout = int((c.options or {}).get("connect_timeout", 8))
            resolved_h = resolve_connection_host(c.host, c.port)
            attachments.append({
                "alias": alias,
                "host": resolved_h,
                "port": c.port,
                "database": c.database,
                "username": c.username,
                "password": pw,
                "sslmode": sslmode,
                "connect_timeout": connect_timeout,
            })
    except Exception:
        pass
    return attachments

@router.get("/targets", response_model=List[QueryTarget])
async def list_query_targets(db: AsyncSession = Depends(get_db)):
    """
    Discovers and returns all available execution engines and target connections.
    Includes DuckDB (Federated Lakehouse), managed warehouse services (e.g. PostgreSQL),
    and registered enterprise database connections in the vault.
    """
    targets: List[QueryTarget] = [
        QueryTarget(
            id="duckdb",
            name="DuckDB",
            engine="duckdb",
            is_default=True,
            description="In-memory analytical engine for Lakehouse Parquet and federated queries",
            capabilities=["Lakehouse Parquet", "Federated SQL", "Cross-Source Federation", "In-Memory OLAP", "CTAS"],
            status="ready",
            schemas=["main"],
        )
    ]

    # 1. Check managed PostgreSQL warehouse service
    try:
        stmt = select(ServiceConfig).where(ServiceConfig.service_name == "postgres")
        res = await db.execute(stmt)
        svc_pg = res.scalar_one_or_none()
        
        # Check live container status if available
        live_c = await asyncio.to_thread(docker_mgr.get_service_container, "postgres")
        is_running = (live_c and live_c.status == "running") or (svc_pg and svc_pg.status == "running")
        
        if svc_pg or is_running:
            port = svc_pg.port if svc_pg else 5432
            host = svc_pg.host if svc_pg else "localhost"
            status_val = "running" if is_running else (svc_pg.status if svc_pg else "stopped")
            
            # Fetch schemas dynamically if running
            schemas = ["public"]
            pg_params: Dict[str, Any] = {}
            if is_running:
                try:
                    # Same credential resolution the dbt runner / SDK use, so the
                    # target reflects the user's configured database and password.
                    pg_params = await asyncio.to_thread(resolve_warehouse_postgres_params)
                    schemas = await asyncio.to_thread(
                        native_query_engine.get_postgres_schemas,
                        host=pg_params["host"],
                        port=pg_params["port"],
                        database=pg_params["database"],
                        username=pg_params["username"],
                        password=pg_params["password"],
                    )
                except Exception:
                    pass

            targets.append(
                QueryTarget(
                    id="service:postgres",
                    name="PostgreSQL",
                    engine="postgres",
                    is_default=False,
                    description="Relational warehouse for Silver & Gold models, dbt marts, and stored procs",
                    capabilities=["Native DDL / DML", "Stored Procedures", "Materialized Views", "Triggers", "Transactions"],
                    status=status_val,
                    schemas=schemas,
                    database=pg_params.get("database") or "obelisk",
                    host=host,
                    port=port,
                )
            )
    except Exception:
        pass

    # 2. Check registered external database connections in SQLite vault
    try:
        stmt = select(Connection)
        result = await db.execute(stmt)
        connections = result.scalars().all()

        for c in connections:
            st = c.source_type.lower()
            if st in ("postgres", "mysql", "clickhouse"):
                caps = ["Native DDL / DML", "Transactions"]
                schemas = ["public"] if st == "postgres" else [c.database]

                if st == "postgres":
                    caps.extend(["Stored Procedures", "Materialized Views", "Triggers"])
                    try:
                        pw = decrypt(c.encrypted_credentials)
                        sslmode = (c.options or {}).get("sslmode", "disable")
                        schemas = await asyncio.to_thread(
                            native_query_engine.get_postgres_schemas,
                            host=c.host,
                            port=c.port,
                            database=c.database,
                            username=c.username,
                            password=pw,
                            sslmode=sslmode,
                        )
                    except Exception:
                        pass
                elif st == "clickhouse":
                    caps = ["High-Speed OLAP", "Real-Time Aggregations", "Columnar Storage"]

                targets.append(
                    QueryTarget(
                        id=f"connection:{c.id}",
                        name=c.name,
                        engine=st,
                        is_default=False,
                        description=f"External {st.capitalize()} at {c.host}:{c.port}/{c.database}",
                        capabilities=caps,
                        status="connected",
                        schemas=schemas,
                        database=c.database,
                        host=c.host,
                        port=c.port,
                    )
                )
    except Exception:
        pass

    return targets

@router.post("/execute", response_model=QueryExecuteResponse)
async def execute_sql(
    req: QueryExecuteRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Executes analytical and operational SQL queries non-blockingly with timeout protection.
    Routes to DuckDB engine for federation or directly to native relational engines
    (PostgreSQL, MySQL, ClickHouse) for DDL, DML, Stored Procedures, and Materialized Views.
    """
    history_entry = QueryHistory(
        title=req.title,
        sql_text=req.sql,
        status="running",
    )
    db.add(history_entry)
    await db.commit()

    target_id = (req.target or "duckdb").strip()

    # Route 1: Direct Execution against Native Database (PostgreSQL / MySQL)
    if target_id.startswith("service:") or target_id.startswith("connection:"):
        target_name = "Database"
        target_engine = "postgres"

        try:
            if target_id == "service:postgres":
                target_name = "PostgreSQL"
                target_engine = "postgres"
                # Resolve the live warehouse credentials (deployed service, catalog
                # connection, env vars, or a reachable local test container).
                pg_params = await asyncio.to_thread(resolve_warehouse_postgres_params)
                host = pg_params["host"]
                port = pg_params["port"]
                user = pg_params["username"]
                pw = pg_params["password"]
                dbname = pg_params["database"]

                exec_res = await asyncio.wait_for(
                    asyncio.to_thread(
                        native_query_engine.execute_postgres,
                        host=host,
                        port=port,
                        database=dbname,
                        username=user,
                        password=pw,
                        sql=req.sql,
                        schema_name=req.schema_name,
                        timeout_seconds=req.timeout_seconds,
                    ),
                    timeout=float(req.timeout_seconds + 2),
                )
            elif target_id.startswith("connection:"):
                conn_id = target_id.split(":", 1)[1]
                stmt = select(Connection).where(Connection.id == conn_id)
                res = await db.execute(stmt)
                c = res.scalar_one_or_none()
                if not c:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Target connection '{conn_id}' was not found in the vault.",
                    )

                target_name = c.name
                target_engine = c.source_type.lower()
                pw = decrypt(c.encrypted_credentials)

                if target_engine == "postgres":
                    sslmode = (c.options or {}).get("sslmode", "disable")
                    exec_res = await asyncio.wait_for(
                        asyncio.to_thread(
                            native_query_engine.execute_postgres,
                            host=c.host,
                            port=c.port,
                            database=c.database,
                            username=c.username,
                            password=pw,
                            sql=req.sql,
                            schema_name=req.schema_name,
                            timeout_seconds=req.timeout_seconds,
                            sslmode=sslmode,
                        ),
                        timeout=float(req.timeout_seconds + 2),
                    )
                elif target_engine == "mysql":
                    exec_res = await asyncio.wait_for(
                        asyncio.to_thread(
                            native_query_engine.execute_mysql,
                            host=c.host,
                            port=c.port,
                            database=c.database,
                            username=c.username,
                            password=pw,
                            sql=req.sql,
                            timeout_seconds=req.timeout_seconds,
                        ),
                        timeout=float(req.timeout_seconds + 2),
                    )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Direct query execution is not supported yet for engine '{target_engine}'. Use DuckDB federation instead.",
                    )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unrecognized target '{target_id}'.",
                )

            history_entry.status = "success"
            history_entry.duration_ms = exec_res["duration_ms"]
            history_entry.row_count = exec_res["row_count"]
            await db.commit()

            return QueryExecuteResponse(
                columns=exec_res["columns"],
                rows=exec_res["rows"],
                row_count=exec_res["row_count"],
                duration_ms=exec_res["duration_ms"],
                status="success",
                command_tag=exec_res.get("command_tag"),
                target_engine=target_engine,
                target_name=target_name,
            )
        except asyncio.TimeoutError:
            history_entry.status = "timeout"
            history_entry.error_message = f"Query timed out after {req.timeout_seconds} seconds"
            await db.commit()
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=history_entry.error_message,
            )
        except Exception as e:
            history_entry.status = "failed"
            history_entry.error_message = str(e)
            await db.commit()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Execution error on {target_name}: {str(e)}",
            )

    # Route 2: DuckDB In-Process Engine (Default)
    pg_attachments = req.postgres_attachments
    if pg_attachments is None:
        pg_attachments = await _get_default_pg_attachments(db, sql=req.sql)

    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(
                duckdb_engine.execute_query,
                sql=req.sql,
                postgres_attachments=pg_attachments,
            ),
            timeout=float(req.timeout_seconds),
        )

        history_entry.status = "success"
        history_entry.duration_ms = res["duration_ms"]
        history_entry.row_count = res["row_count"]
        await db.commit()

        return QueryExecuteResponse(
            columns=res["columns"],
            rows=res["rows"],
            row_count=res["row_count"],
            duration_ms=res["duration_ms"],
            status="success",
            command_tag=None,
            target_engine="duckdb",
            target_name="DuckDB",
        )
    except asyncio.TimeoutError:
        history_entry.status = "timeout"
        history_entry.error_message = f"Query timed out after {req.timeout_seconds} seconds"
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=history_entry.error_message,
        )
    except Exception as e:
        history_entry.status = "failed"
        history_entry.error_message = str(e)
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution failed: {str(e)}",
        )

@router.post("/export")
async def export_sql_results(
    req: QueryExportRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Executes a query and streams the binary output as Snappy Parquet or CSV.
    """
    fmt = req.format.lower()
    if fmt not in ["parquet", "csv"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported export format '{req.format}'. Supported: 'parquet', 'csv'",
        )

    pg_attachments = req.postgres_attachments
    if pg_attachments is None:
        pg_attachments = await _get_default_pg_attachments(db, sql=req.sql)

    try:
        data_bytes = await asyncio.to_thread(
            duckdb_engine.export_query,
            sql=req.sql,
            output_format=fmt,
            postgres_attachments=pg_attachments,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Export failed: {str(e)}",
        )

    base_name = req.filename or "export"
    media_type = "application/octet-stream" if fmt == "parquet" else "text/csv"
    filename = f"{base_name}.{fmt}"

    return Response(
        content=data_bytes,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/history")
async def get_query_history(db: AsyncSession = Depends(get_db)):
    """Returns the 50 most recent executed queries."""
    stmt = select(QueryHistory).order_by(desc(QueryHistory.executed_at)).limit(50)
    result = await db.execute(stmt)
    history = result.scalars().all()
    return [
        {
            "id": h.id,
            "title": h.title,
            "sql_text": h.sql_text,
            "status": h.status,
            "executed_at": h.executed_at.isoformat(),
            "duration_ms": h.duration_ms,
            "row_count": h.row_count,
            "error_message": h.error_message,
        }
        for h in history
    ]


class OpenQueryTabResponse(BaseModel):
    id: str
    title: str
    sql: str
    target_id: str = "duckdb"
    schema_name: Optional[str] = None
    tab_order: int = 0
    updated_at: Optional[str] = None


class SaveQueryTabRequest(BaseModel):
    id: str
    title: str
    sql: str
    target_id: Optional[str] = "duckdb"
    schema_name: Optional[str] = None
    tab_order: Optional[int] = 0


@router.get("/tabs", response_model=List[OpenQueryTabResponse])
async def list_open_tabs(db: AsyncSession = Depends(get_db)):
    """Returns all open query tabs ordered by tab_order."""
    stmt = select(OpenQueryTab).order_by(OpenQueryTab.tab_order.asc(), OpenQueryTab.created_at.asc())
    result = await db.execute(stmt)
    tabs = result.scalars().all()
    return [
        OpenQueryTabResponse(
            id=t.id,
            title=t.title,
            sql=t.sql_text,
            target_id=t.target_id,
            schema_name=t.schema_name,
            tab_order=t.tab_order,
            updated_at=t.updated_at.isoformat() if t.updated_at else None,
        )
        for t in tabs
    ]


@router.post("/tabs", response_model=OpenQueryTabResponse)
async def save_open_tab(req: SaveQueryTabRequest, db: AsyncSession = Depends(get_db)):
    """Upserts an open query tab to the SQLite brain."""
    stmt = select(OpenQueryTab).where(OpenQueryTab.id == req.id)
    result = await db.execute(stmt)
    tab = result.scalar_one_or_none()
    if tab:
        tab.title = req.title
        tab.sql_text = req.sql
        tab.target_id = req.target_id or "duckdb"
        tab.schema_name = req.schema_name
        if req.tab_order is not None:
            tab.tab_order = req.tab_order
    else:
        tab = OpenQueryTab(
            id=req.id,
            title=req.title,
            sql_text=req.sql,
            target_id=req.target_id or "duckdb",
            schema_name=req.schema_name,
            tab_order=req.tab_order or 0,
        )
        db.add(tab)
    await db.commit()
    await db.refresh(tab)
    return OpenQueryTabResponse(
        id=tab.id,
        title=tab.title,
        sql=tab.sql_text,
        target_id=tab.target_id,
        schema_name=tab.schema_name,
        tab_order=tab.tab_order,
        updated_at=tab.updated_at.isoformat() if tab.updated_at else None,
    )


@router.delete("/tabs/{tab_id}")
async def delete_open_tab(tab_id: str, db: AsyncSession = Depends(get_db)):
    """Removes a closed query tab from the SQLite brain to prevent bloat."""
    stmt = select(OpenQueryTab).where(OpenQueryTab.id == tab_id)
    result = await db.execute(stmt)
    tab = result.scalar_one_or_none()
    if tab:
        await db.delete(tab)
        await db.commit()
    return {"status": "deleted", "id": tab_id}

