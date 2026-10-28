import re
import asyncio
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.config import settings
from app.database import get_db
from app.models.query import QueryHistory, OpenQueryTab
from app.models.connection import Connection
from app.models.service_config import ServiceConfig
from app.core.vault import decrypt
from app.engines.duckdb_engine import duckdb_engine
from app.engines.native_query_engine import native_query_engine
from app.core.docker_manager import docker_mgr
from app.services.connections import attachable_attachments
from app.services.query_registry import query_registry
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
    # The client's id for this execution. It is what POST /api/query/{id}/cancel addresses,
    # so a Stop button can reach the statement instead of waiting for the timeout.
    query_id: Optional[str] = Field(
        None, description="Client-generated id for this execution, used to cancel it"
    )
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
    truncated: bool = False

class QueryExportRequest(BaseModel):
    sql: str = Field(..., description="SQL statement to export")
    format: str = Field("parquet", description="Export format: 'parquet' or 'csv'")
    filename: Optional[str] = Field("query_export", description="Base download filename")
    postgres_attachments: Optional[List[Dict[str, Any]]] = Field(
        None, description="Optional Postgres sources to attach dynamically"
    )

@router.get("/telemetry")
async def get_query_engine_telemetry():
    """Returns live DuckDB engine concurrency, spill directory usage, and lakehouse disk headroom."""
    return await asyncio.to_thread(duckdb_engine.get_telemetry)


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
                    description="Relational warehouse for dbt models, marts, and stored procs",
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

        def remember_native(conn) -> None:
            """Publishes the driver connection so Stop can cancel a native statement."""
            query_registry.register(req.query_id or "", conn)

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
                        # Same cap the DuckDB path uses: without it this path built one
                        # Python dict per row of the entire result set.
                        max_rows=settings.DUCKDB_MAX_QUERY_ROWS,
                        on_connection=remember_native,
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
                            max_rows=settings.DUCKDB_MAX_QUERY_ROWS,
                            on_connection=remember_native,
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
                            max_rows=settings.DUCKDB_MAX_QUERY_ROWS,
                            on_connection=remember_native,
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
                truncated=bool(exec_res.get("truncated", False)),
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
        finally:
            query_registry.release(req.query_id or "")

    # Route 2: DuckDB In-Process Engine (Default)
    attachments = req.postgres_attachments
    if attachments is None:
        attachments = await attachable_attachments(db, sql=req.sql)

    # Holds the live DuckDB connection so a timeout can interrupt the C++ work instead of
    # leaving the background thread burning CPU and RAM after the response is gone.
    live_conn_holder: list = []

    def remember_connection(conn) -> None:
        """Publishes the live connection for timeout interruption *and* for Stop."""
        live_conn_holder.append(conn)
        query_registry.register(req.query_id or "", conn)

    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(
                duckdb_engine.execute_query,
                sql=req.sql,
                attachments=attachments,
                on_connection=remember_connection,
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
            truncated=bool(res.get("truncated", False)),
        )
    except asyncio.TimeoutError:
        if live_conn_holder:
            try:
                live_conn_holder[0].interrupt()
            except Exception:
                pass
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
    finally:
        # The query is over one way or another; a stale entry would make a later Stop look
        # like it interrupted something.
        query_registry.release(req.query_id or "")


@router.post("/{query_id}/cancel")
async def cancel_running_query(query_id: str):
    """Stops the statement running under ``query_id``, if there is one.

    Query Studio's Stop button. It answers 200 either way: "already finished" is not an
    error, and the client should not have to distinguish the two to clear its spinner.
    ``not_stoppable`` says the engine has no client-side cancel (MySQL), so the user knows
    the click did nothing rather than wondering why rows keep coming.
    """
    status_value = query_registry.cancel(query_id)
    return {"status": status_value, "query_id": query_id}


async def _delete_scratch_file(path: Path) -> None:
    """Removes an export scratch file once its response has been sent."""
    await asyncio.to_thread(path.unlink, missing_ok=True)


@router.post("/export")
async def export_sql_results(
    req: QueryExportRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Executes a query and streams the full result as Snappy Parquet or CSV.

    The result is written by DuckDB straight to a scratch file and served from disk, so a
    multi-GB export is never buffered in Python memory.
    """
    fmt = req.format.lower()
    if fmt not in ["parquet", "csv"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported export format '{req.format}'. Supported: 'parquet', 'csv'",
        )

    attachments = req.postgres_attachments
    if attachments is None:
        attachments = await attachable_attachments(db, sql=req.sql)

    temp_file = settings.duckdb_temp_path / f"export_{uuid.uuid4().hex}.{fmt}"
    try:
        await asyncio.to_thread(
            duckdb_engine.export_query_to_file,
            sql=req.sql,
            dest_path=temp_file,
            output_format=fmt,
            attachments=attachments,
        )
    except BaseException as e:
        await asyncio.to_thread(temp_file.unlink, missing_ok=True)
        if isinstance(e, Exception):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Export failed: {str(e)}",
            )
        raise

    base_name = req.filename or "export"
    media_type = "application/octet-stream" if fmt == "parquet" else "text/csv"
    filename = f"{base_name}.{fmt}"

    return FileResponse(
        path=temp_file,
        media_type=media_type,
        filename=filename,
        background=BackgroundTask(_delete_scratch_file, temp_file),
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


# ------------------------------------------------------------- tab scoping
# Query Studio tabs are per window, not per server: two windows (or two team
# members) both call their tabs "tab-1", and a shared primary key meant the second
# save silently replaced the first window's SQL. The stored key is therefore
# "<client>:<tab>"; the API keeps speaking unprefixed ids so nothing above this
# layer changes, and a client that sends no header lands in "default" exactly as
# every client did before the split.
_DEFAULT_CLIENT_ID = "default"
_CLIENT_ID_MAX_LENGTH = 64


def _client_scope(raw: Optional[str]) -> str:
    """The client id a tab request belongs to, safe to use as a key prefix."""
    cleaned = re.sub(r"[^A-Za-z0-9_.\-]", "", (raw or "").strip())[:_CLIENT_ID_MAX_LENGTH]
    return cleaned or _DEFAULT_CLIENT_ID


def _scoped_tab_id(client_id: str, tab_id: str) -> str:
    """Stored primary key for a client's tab id, idempotent if already prefixed."""
    prefix = f"{client_id}:"
    return tab_id if tab_id.startswith(prefix) else f"{prefix}{tab_id}"


def _tab_id_for_client(client_id: str, stored_id: str) -> str:
    """The id the client knows a tab by, with this client's prefix removed."""
    prefix = f"{client_id}:"
    return stored_id[len(prefix):] if stored_id.startswith(prefix) else stored_id


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
async def list_open_tabs(
    x_obelisk_client_id: Optional[str] = Header(default=None, alias="X-Obelisk-Client-Id"),
    db: AsyncSession = Depends(get_db),
):
    """Returns this client's open query tabs ordered by tab_order."""
    client_id = _client_scope(x_obelisk_client_id)
    stmt = (
        select(OpenQueryTab)
        .where(OpenQueryTab.client_id == client_id)
        .order_by(OpenQueryTab.tab_order.asc(), OpenQueryTab.created_at.asc())
    )
    result = await db.execute(stmt)
    tabs = result.scalars().all()
    return [
        OpenQueryTabResponse(
            id=_tab_id_for_client(client_id, t.id),
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
async def save_open_tab(
    req: SaveQueryTabRequest,
    x_obelisk_client_id: Optional[str] = Header(default=None, alias="X-Obelisk-Client-Id"),
    db: AsyncSession = Depends(get_db),
):
    """Upserts an open query tab to the SQLite brain, scoped to the calling client."""
    client_id = _client_scope(x_obelisk_client_id)
    stored_id = _scoped_tab_id(client_id, req.id)
    stmt = select(OpenQueryTab).where(OpenQueryTab.id == stored_id)
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
            id=stored_id,
            client_id=client_id,
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
        id=_tab_id_for_client(client_id, tab.id),
        title=tab.title,
        sql=tab.sql_text,
        target_id=tab.target_id,
        schema_name=tab.schema_name,
        tab_order=tab.tab_order,
        updated_at=tab.updated_at.isoformat() if tab.updated_at else None,
    )


@router.delete("/tabs/{tab_id}")
async def delete_open_tab(
    tab_id: str,
    x_obelisk_client_id: Optional[str] = Header(default=None, alias="X-Obelisk-Client-Id"),
    db: AsyncSession = Depends(get_db),
):
    """Removes a closed query tab from the SQLite brain to prevent bloat."""
    client_id = _client_scope(x_obelisk_client_id)
    stmt = select(OpenQueryTab).where(OpenQueryTab.id == _scoped_tab_id(client_id, tab_id))
    result = await db.execute(stmt)
    tab = result.scalar_one_or_none()
    if tab:
        await db.delete(tab)
        await db.commit()
    return {"status": "deleted", "id": tab_id}

