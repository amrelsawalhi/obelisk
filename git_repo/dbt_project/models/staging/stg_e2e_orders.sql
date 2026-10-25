{{ config(materialized='table') }}

with source as (
    select * from {{ source('landing', 'e2e_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_ts as date) as order_date,
    region,
    status,
    cast(amount as double precision) as amount
from source