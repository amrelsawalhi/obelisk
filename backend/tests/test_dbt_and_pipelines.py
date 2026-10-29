import os
import json
import uuid
import datetime
import psycopg2
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app.config import settings
from app.main import app
from app.core.docker_manager import DockerManager
from app.core.resource_advisor import advisor
from app.engines.dbt_runner import dbt_runner


def test_only_changed_tables_are_copied_to_the_warehouse():
    """An unchanged lakehouse table must not be copied over TCP again."""
    to_sync, unchanged = dbt_runner._sync_plan(
        {"orders": (100, 2048), "customers": (5, 512), "fresh": None},
        {"orders": (100, 2048), "customers": (6, 512)},
    )

    assert unchanged == ["orders"]
    assert to_sync == ["customers", "fresh"], "an unreadable fingerprint must be copied"


def test_a_forced_sync_copies_every_table():
    to_sync, unchanged = dbt_runner._sync_plan(
        {"orders": (100, 2048)}, {"orders": (100, 2048)}, force=True
    )

    assert unchanged == []
    assert to_sync == ["orders"]



def test_docker_manager_service_blueprints():
    """
    Verifies that localhost deployment blueprints for all 6 catalog services are configured
    with dynamic resource sizing and subpath prefix routing flags.
    """
    dm = DockerManager(client=None)

    # 1. PostgreSQL Warehouse
    rec_pg = advisor.recommend_allocation("postgres")
    assert rec_pg["recommended_ram_mb"] >= 512

    # 2. Superset BI
    rec_sup = advisor.recommend_allocation("superset")
    assert rec_sup["recommended_ram_mb"] >= 512

    # 3. ClickHouse OLAP
    rec_ch = advisor.recommend_allocation("clickhouse")
    assert rec_ch["recommended_ram_mb"] >= 512

    # 5. Redpanda Streaming
    rec_rp = advisor.recommend_allocation("redpanda")
    assert rec_rp["recommended_ram_mb"] >= 256

    # 6. Gitea Git
    rec_git = advisor.recommend_allocation("gitea")
    assert rec_git["recommended_ram_mb"] >= 128


def test_dbt_pipeline_end_to_end(source_postgres_params):
    """
    Full pipeline integration test:
    1. Creates real Parquet files (orders & customers) in the staging folder.
    2. DuckDB copies the folder into its warehouse schema.
    3. dbt-core transforms raw tables into the staging schema (stg_orders, stg_customers).
    4. dbt-core aggregates business marts into the marts schema (fct_revenue, dim_customers).
    5. dbt test validates unique and not-null constraints.
    6. Directly queries PostgreSQL to assert data integrity and aggregated calculations.

    The two schemas are read from the project's own config, so changing a folder's
    +schema does not require editing this test.
    """
    p = source_postgres_params

    # Which schemas the run will write to, taken from the project's own config.
    folders = dbt_runner._dbt_folders()
    staging_schema = folders["staging"]
    marts_schema = folders["marts"]

    # Files live in the staging folder — that is what the pipeline copies.
    staging_dir = settings.lakehouse_root_path / staging_schema
    staging_dir.mkdir(parents=True, exist_ok=True)

    # 1. Create the source tables through the catalog (a file on disk is not a table).
    from lakehouse_helpers import write_table as create_table

    orders_table = pa.Table.from_pydict({
        "order_id": [1, 2, 3, 4],
        "customer_id": [10, 20, 10, 30],
        "amount": [100.00, 250.50, 150.00, 75.25],
        "status": ["completed", "completed", "completed", "pending"],
        "created_at": ["2026-01-10 10:00:00", "2026-01-11 11:30:00", "2026-01-12 14:15:00", "2026-01-13 09:00:00"],
    })
    create_table(f"{staging_schema}/orders.parquet", orders_table)

    customers_table = pa.Table.from_pydict({
        "customer_id": [10, 20, 30],
        "name": ["Acme Corp", "Beta LLC", "Gamma Inc"],
        "tier": ["Enterprise", "MidMarket", "Enterprise"],
        "country": ["US", "DE", "FR"],
    })
    create_table(f"{staging_schema}/customers.parquet", customers_table)

    # The project also declares two enterprise sources. Give them one row each so
    # `dbt run` can build every model in the project, not just the ones asserted on.
    create_table(
        f"{staging_schema}/ecommerce_orders.parquet",
        pa.Table.from_pydict({
            "order_id": [9001],
            "order_uuid": ["11111111-1111-1111-1111-111111111111"],
            "customer_id": ["33333333-3333-3333-3333-333333333333"],
            "order_status": ["paid"],
            "currency": ["EUR"],
            "subtotal_amount": [100.0],
            "tax_amount": [19.0],
            "discount_amount": [0.0],
            "total_amount": [119.0],
            "order_placed_at": ["2026-01-15 10:00:00+00"],
            "shipping_address": ['{"city": "Berlin", "street": "Main 1", "is_commercial": false}'],
        }),
    )
    create_table(
        f"{staging_schema}/ecommerce_customers.parquet",
        pa.Table.from_pydict({
            "customer_id": ["33333333-3333-3333-3333-333333333333"],
            "first_name": ["Ada"],
            "last_name": ["Lovelace"],
            "email": ["ada@example.com"],
            "tier": ["Platinum"],
            "country_code": ["DEU"],
            "signup_timestamp": ["2026-01-01 09:00:00+00"],
            "metadata_json": ['{"referral_source": "search", "preferred_channel": "email", "vip_flag": true}'],
        }),
    )

    pg_params = {
        "host": p["host"],
        "port": p["port"],
        "database": p["database"],
        "username": p["user"],
        "password": p["password"],
        "sslmode": p["sslmode"],
    }

    try:
        # 2. Run the pipeline end to end
        res = dbt_runner.run_pipeline(
            postgres_params=pg_params,
        )
        assert res["status"] == "success", f"Pipeline failed with logs:\n{res.get('logs')}"

        # 3. Query PostgreSQL directly to verify Silver & Gold tables
        from app.core.host_resolver import resolve_connection_host
        conn = psycopg2.connect(
            host=resolve_connection_host(p["host"], p["port"]),
            port=p["port"],
            dbname=p["database"],
            user=p["user"],
            password=p["password"],
            sslmode=p["sslmode"],
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                # Assert staging stg_orders has 4 typed rows
                cur.execute(f"SELECT count(*), sum(amount) FROM {staging_schema}.stg_orders;")
                stg_count, stg_sum = cur.fetchone()
                assert stg_count == 4
                assert float(stg_sum) == 575.75

                # Assert staging stg_customers has 3 rows
                cur.execute(f"SELECT count(*) FROM {staging_schema}.stg_customers;")
                cust_count = cur.fetchone()[0]
                assert cust_count == 3

                # Assert Gold fct_revenue has correctly aggregated customer tiers
                # Enterprise: customer 10 ($100 + $150 = $250) + customer 30 ($75.25) = $325.25 (3 orders)
                # MidMarket: customer 20 ($250.50) = $250.50 (1 order)
                cur.execute(f"""
                    SELECT customer_tier, total_orders, total_revenue
                    FROM {marts_schema}.fct_revenue
                    ORDER BY customer_tier;
                """)
                revenue_rows = cur.fetchall()
                tier_dict = {row[0]: {"orders": row[1], "revenue": float(row[2])} for row in revenue_rows}

                assert "Enterprise" in tier_dict
                assert tier_dict["Enterprise"]["orders"] == 3
                assert tier_dict["Enterprise"]["revenue"] == 325.25

                assert "MidMarket" in tier_dict
                assert tier_dict["MidMarket"]["orders"] == 1
                assert tier_dict["MidMarket"]["revenue"] == 250.50

                # Assert Gold dim_customers customer lifetime metrics
                cur.execute(f"""
                    SELECT customer_name, lifetime_orders, lifetime_spend
                    FROM {marts_schema}.dim_customers
                    WHERE customer_id = 10;
                """)
                cust10 = cur.fetchone()
                assert cust10[0] == "Acme Corp"
                assert cust10[1] == 2
                assert float(cust10[2]) == 250.00

        finally:
            conn.close()

    finally:
        # Drop the source tables the run read, so the shared lakehouse stays clean.
        from app.engines.duckdb_engine import duckdb_engine

        conn = duckdb_engine.get_connection()
        try:
            for name in ("orders", "customers", "ecommerce_orders", "ecommerce_customers"):
                conn.execute(
                    f'DROP TABLE IF EXISTS lakehouse."{staging_schema}"."{name}"'
                )
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_pipelines_api_flow(source_postgres_params):
    """Verifies REST endpoints for pipeline asset graph and execution."""
    p = source_postgres_params
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Asset Lineage Graph
        graph_resp = await client.get("/api/pipelines/assets")
        assert graph_resp.status_code == 200
        graph_data = graph_resp.json()
        assert "nodes" in graph_data
        assert "edges" in graph_data
        assert "schemas" in graph_data
        # The graph is keyed by the schema each model folder declares.
        folders = dbt_runner._dbt_folders()
        assert folders["staging"] in graph_data["schemas"]
        assert folders["marts"] in graph_data["schemas"]

        # 2. Pipeline Run Trigger
        payload = {
            "postgres_params": {
                "host": p["host"],
                "port": p["port"],
                "database": p["database"],
                "username": p["user"],
                "password": p["password"],
                "sslmode": p["sslmode"],
            }
        }
        run_resp = await client.post("/api/pipelines/run", json=payload)
        assert run_resp.status_code == 200
        run_data = run_resp.json()
        assert "run_id" in run_data
        assert "status" in run_data

        # 3. Pipeline Runs History
        history_resp = await client.get("/api/pipelines/runs")
        assert history_resp.status_code == 200
        history_data = history_resp.json()
        assert "runs" in history_data
        assert history_data["total"] >= 1


