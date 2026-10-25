{{ config(materialized='table') }}

select
    region,
    count(*) as orders,
    round((sum(amount))::numeric, 2) as revenue,
    round((avg(amount))::numeric, 2) as avg_amount
from {{ ref('stg_e2e_orders') }}
where status <> 'returned'
group by 1