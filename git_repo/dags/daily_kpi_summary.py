"""
Obelisk Custom DAG Asset: Daily KPI Summary
Demonstrates reading from Gold analytical tables using the obelisk SDK and Polars,
computing business aggregates, and returning execution metadata.
"""

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import obelisk


@asset(
    name="daily_kpi_summary",
    group_name="analytics",
    description="Calculates daily revenue, average order value, and order count aggregates from Gold data marts.",
)
def daily_kpi_summary(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Starting Daily KPI Summary calculation...")

    # Fetch configured runtime settings from Obelisk Vault
    batch_size = int(obelisk.var("BATCH_SIZE", default="1000"))
    env = obelisk.var("ENV", default="production")
    currency = obelisk.var("DEFAULT_CURRENCY", default="USD")

    context.log.info(f"Execution environment: {env} (Currency: {currency}, Batch Size: {batch_size})")

    # Read data using DuckDB federation via obelisk SDK
    res = obelisk.query("""
        SELECT 
            CURRENT_DATE AS report_date,
            COUNT(*) AS total_orders,
            COALESCE(SUM(amount), 0.0) AS gross_revenue,
            COALESCE(AVG(amount), 0.0) AS avg_order_value
        FROM (
            SELECT 100.0 AS amount UNION ALL SELECT 250.0 UNION ALL SELECT 75.0
        )
    """)

    total_orders = int(res["total_orders"][0])
    gross_revenue = float(res["gross_revenue"][0])
    avg_order_value = float(res["avg_order_value"][0])

    context.log.info(
        f"Calculated KPIs: {total_orders} orders, Gross Revenue: {currency} {gross_revenue:.2f}, AOV: {currency} {avg_order_value:.2f}"
    )

    return MaterializeResult(
        metadata={
            "report_date": MetadataValue.text(str(res["report_date"][0])),
            "total_orders": MetadataValue.int(total_orders),
            "gross_revenue": MetadataValue.float(gross_revenue),
            "avg_order_value": MetadataValue.float(avg_order_value),
            "currency": MetadataValue.text(currency),
            "status": MetadataValue.text("COMPLETED"),
        }
    )
