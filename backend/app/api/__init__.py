from app.api.routes_services import router as services_router
from app.api.routes_catalog import router as catalog_router
from app.api.routes_ingest import router as ingest_router
from app.api.routes_query import router as query_router

__all__ = ["services_router", "catalog_router", "ingest_router", "query_router"]
