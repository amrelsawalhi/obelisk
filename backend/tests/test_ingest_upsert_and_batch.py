import uuid
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.engines.ingest_engine import ingest_engine


def test_write_strategy_append_vs_upsert(tmp_path):
    """
    Unit test for IngestEngine._apply_write_strategy:
    Verifies:
    1. Append strategy appends delta rows (yielding duplicate primary keys).
    2. Upsert strategy deduplicates partitioned by primary_key ordered by watermark DESC,
       keeping the newest record and eliminating older duplicates.
    """
    parquet_file = tmp_path / "test_strategy_orders.parquet"

    # Initial snapshot on disk
    initial_table = pa.Table.from_pydict({
        "order_id": [1, 2, 3],
        "status": ["pending", "pending", "shipped"],
        "updated_at": [100, 100, 100],
    })
    pq.write_table(initial_table, str(parquet_file), compression="SNAPPY")

    # Delta table with an update to order_id 1 (status -> completed, updated_at -> 200)
    # and a new order_id 4
    delta_table = pa.Table.from_pydict({
        "order_id": [1, 4],
        "status": ["completed", "created"],
        "updated_at": [200, 150],
    })

    # Combine tables as done in ingest_engine
    combined_table = pa.concat_tables([initial_table, delta_table])

    # Test 1: Append Strategy (no-op deduplication)
    append_table = ingest_engine._apply_write_strategy(
        table=combined_table,
        write_strategy="append",
        primary_key="order_id",
        watermark_column="updated_at",
    )
    # Total rows = 3 + 2 = 5 rows (order_id 1 appears twice)
    assert append_table.num_rows == 5
    order_1_rows = append_table.filter(pa.compute.equal(append_table["order_id"], 1))
    assert order_1_rows.num_rows == 2

    # Test 2: Upsert Strategy with primary_key="order_id", watermark_column="updated_at"
    upsert_table = ingest_engine._apply_write_strategy(
        table=combined_table,
        write_strategy="upsert",
        primary_key="order_id",
        watermark_column="updated_at",
    )
    # Deduplicated: order_id 1, 2, 3, 4 = 4 distinct rows
    assert upsert_table.num_rows == 4
    pydict = upsert_table.to_pydict()
    order_ids = pydict["order_id"]
    statuses = dict(zip(order_ids, pydict["status"]))
    updated_ats = dict(zip(order_ids, pydict["updated_at"]))

    assert 1 in statuses
    # Must have the NEW status from delta (updated_at=200), not the old one (updated_at=100)
    assert statuses[1] == "completed"
    assert updated_ats[1] == 200
    assert statuses[4] == "created"
    assert statuses[2] == "pending"


@pytest.mark.asyncio
async def test_batch_ingest_endpoint_validation():
    """
    Tests that POST /api/ingest/batch properly validates required fields,
    rejects empty table lists, and returns 404 for non-existent connections.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Missing connection_id
        res1 = await client.post("/api/ingest/batch", json={
            "tables": ["orders", "users"],
        })
        assert res1.status_code == 422

        # 2. Empty tables list
        res2 = await client.post("/api/ingest/batch", json={
            "connection_id": "non_existent_conn_id",
            "tables": [],
        })
        assert res2.status_code == 400
        assert "No tables specified" in res2.json()["detail"]

        # 3. Non-existent connection ID
        res3 = await client.post("/api/ingest/batch", json={
            "connection_id": "non_existent_conn_id",
            "tables": ["orders"],
        })
        assert res3.status_code == 404
        assert "not found" in res3.json()["detail"]


@pytest.mark.asyncio
async def test_async_ingest_job_manager_and_sse():
    """
    Tests the decoupled IngestJobManager:
    1. Creating a job
    2. Emitting logs & progress
    3. Subscribing to SSE stream with catch-up buffer
    4. Cancelling the job
    """
    from app.engines.ingest_job_manager import ingest_job_manager

    # 1. Create job
    job = ingest_job_manager.create_job(
        title="Test Batch Ingest",
        job_type="batch",
        connection_id="test_conn_123",
        source_name="Postgres DB",
        source_type="postgres",
        mode="snapshot",
        total_tables=3,
    )
    assert job.status == "running"
    assert job.percent == 0

    # 2. Emit logs and progress
    ingest_job_manager.append_log(job.id, "Connected to source database")
    ingest_job_manager.update_progress(
        job.id,
        current_table="dim_customers",
        current_phase="Extracting rows",
        completed_tables=1,
        total_rows=100,
    )
    assert job.percent == 33
    assert len(job.logs) == 1
    assert "Connected to source database" in job.logs[0]

    # 3. Verify Active Jobs query
    active = ingest_job_manager.get_active_jobs()
    assert any(j["id"] == job.id for j in active)

    # 4. Read SSE subscription stream catch-up
    events = []
    async for chunk in ingest_job_manager.subscribe(job.id):
        events.append(chunk)
        if len(events) >= 2:
            break

    assert any("progress" in ev for ev in events)
    assert any("Connected to source database" in ev for ev in events)

    # 5. Cancel Job
    cancelled = ingest_job_manager.cancel_job(job.id)
    assert cancelled is True
    assert job.status == "cancelled"

