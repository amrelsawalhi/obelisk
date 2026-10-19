{{ config(materialized='table', schema='silver') }}

-- Silver transformation computing order frequency and spending aggregates
select
    customer_id,
    count(order_id) as total_orders,
    sum(amount) as lifetime_spend,
    round(avg(amount), 2) as average_order_value,
    max(order_timestamp) as latest_order_time
from {{ ref('stg_orders') }}
where status = 'completed'
group by customer_id
