"""
WS-1: out-of-core query execution.

Interactive JSON responses stop at a row ceiling and say so, exports are written by
DuckDB straight to a file, SQL materialisations land in DuckLake in one statement, and
the SDK has no PostgreSQL mirror left to write to.
"""
import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import AsyncClient, ASGITransport

from app.config import settings
from app.engines.duckdb_engine import duckdb_engine
from app.main import app
from app.sdk import obelisk
from lakehouse_helpers import drop_schema


def test_execute_query_caps_rows_and_flags_truncation():
    """A cap returns exactly `max_rows` rows and reports that more existed."""
    full = duckdb_engine.execute_query("SELECT range AS i FROM range(25)", max_rows=0)
    assert full["row_count"] == 25
    assert full["truncated"] is False

    capped = duckdb_engine.execute_query("SELECT range AS i FROM range(25)", max_rows=10)
    assert capped["row_count"] == 10
    assert capped["truncated"] is True
    assert [row["i"] for row in capped["rows"]] == list(range(10))

    exact = duckdb_engine.execute_query("SELECT range AS i FROM range(10)", max_rows=10)
    assert exact["row_count"] == 10
    assert exact["truncated"] is False


def test_execute_query_default_ceiling_comes_from_settings():
    """With no explicit cap the interactive preview is bounded by DUCKDB_MAX_QUERY_ROWS."""
    assert settings.DUCKDB_MAX_QUERY_ROWS >= 1
    res = duckdb_engine.execute_query(
        f"SELECT range AS i FROM range({settings.DUCKDB_MAX_QUERY_ROWS + 1})"
    )
    assert res["row_count"] == settings.DUCKDB_MAX_QUERY_ROWS
    assert res["truncated"] is True


def test_export_query_to_file_streams_parquet_and_csv(tmp_path):
    """Exports are written by DuckDB's COPY, so the full result reaches the file."""
    parquet_dest = tmp_path / "orders.parquet"
    returned = duckdb_engine.export_query_to_file(
        "SELECT range AS i FROM range(1000)", parquet_dest
    )
    assert returned == parquet_dest
    assert pq.read_table(parquet_dest).num_rows == 1000

    csv_dest = tmp_path / "orders.csv"
    duckdb_engine.export_query_to_file("SELECT range AS i FROM range(3)", csv_dest, "csv")
    lines = csv_dest.read_text().strip().splitlines()
    assert lines[0] == "i"
    assert len(lines) == 4

    with pytest.raises(ValueError):
        duckdb_engine.export_query_to_file(
            "SELECT 1 AS i", tmp_path / "orders.json", "json"
        )


def test_materialize_sql_writes_a_ducklake_table():
    """The query result lands in the catalog without becoming a Python object."""
    schema = f"test_ws1_{uuid.uuid4().hex[:6]}"
    try:
        saved = obelisk.materialize_sql(
            "SELECT range AS i, range * 2 AS doubled FROM range(5)",
            f"{schema}.probe",
        )
        assert saved["status"] == "success"
        assert saved["num_rows"] == 5
        assert saved["file_path"] == f"lakehouse.{schema}.probe"
        assert saved["fqn"] == f"lakehouse.{schema}.probe"

        res = duckdb_engine.execute_query(
            f"SELECT count(*) AS n, sum(doubled) AS total FROM lakehouse.{schema}.probe"
        )
        assert res["rows"] == [{"n": 5, "total": 20}]
    finally:
        drop_schema(schema)


def test_save_is_lakehouse_only():
    """`save` returns no warehouse mirror, and the removed parameter is really gone."""
    schema = f"test_ws1_{uuid.uuid4().hex[:6]}"
    try:
        saved = obelisk.save(pa.table({"id": [1, 2], "label": ["a", "b"]}), f"{schema}.items")
        assert saved["status"] == "success"
        assert saved["num_rows"] == 2
        assert saved["file_path"] == f"lakehouse.{schema}.items"
        assert "warehouse_sync" not in saved
        assert "warehouse_schema" not in saved

        with pytest.raises(TypeError):
            obelisk.save(pa.table({"id": [1]}), f"{schema}.items2", sync_postgres=False)
    finally:
        drop_schema(schema)


@pytest.mark.asyncio
async def test_query_execute_route_reports_truncation():
    """The HTTP response carries the same ceiling and the truncation flag."""
    sql = f"SELECT range AS i FROM range({settings.DUCKDB_MAX_QUERY_ROWS + 5})"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post(
            "/api/query/execute",
            json={
                "sql": sql,
                "title": f"WS1 ceiling {uuid.uuid4().hex[:6]}",
                "timeout_seconds": 30,
            },
        )
        assert res.status_code == 200
        body = res.json()
        assert body["row_count"] == settings.DUCKDB_MAX_QUERY_ROWS
        assert body["truncated"] is True


@pytest.mark.asyncio
async def test_export_route_serves_the_full_result_from_disk():
    """The export route answers with a real file, not a buffered byte string."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post(
            "/api/query/export",
            json={
                "sql": "SELECT range AS i FROM range(2048)",
                "format": "parquet",
                "filename": f"ws1_export_{uuid.uuid4().hex[:6]}",
            },
        )
        assert res.status_code == 200
        assert "attachment" in res.headers["content-disposition"]
        assert pq.read_table(io.BytesIO(res.content)).num_rows == 2048
