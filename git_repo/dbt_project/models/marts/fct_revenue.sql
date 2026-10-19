{{ config(materialized='table', schema='gold') }}

select
    coalesce(c.customer_tier, 'Unknown') as customer_tier,
    count(o.order_id) as total_orders,
    coalesce(sum(o.amount), 0) as total_revenue,
    coalesce(avg(o.amount), 0) as avg_order_value
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
group by coalesce(c.customer_tier, 'Unknown')
