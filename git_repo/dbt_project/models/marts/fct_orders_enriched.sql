{{ config(materialized='table', schema='gold') }}

-- Curated analytical fact mart enriching transactions with customer demographics
select
    o.order_id,
    o.customer_id,
    coalesce(c.customer_name, 'Unknown') as customer_name,
    coalesce(c.customer_tier, 'Standard') as customer_tier,
    coalesce(c.country, 'Global') as customer_country,
    o.amount as order_amount,
    o.status as order_status,
    o.order_timestamp
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
