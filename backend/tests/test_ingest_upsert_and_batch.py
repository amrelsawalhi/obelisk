import uuid
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.engines.ingest_engine import ingest_engine

