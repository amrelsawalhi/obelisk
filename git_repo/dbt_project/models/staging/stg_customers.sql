{{ config(materialized='table', schema='silver') }}

select
    cast(customer_id as integer) as customer_id,
    cast(name as varchar(255)) as customer_name,
    cast(tier as varchar(50)) as customer_tier,
    cast(country as varchar(50)) as country
from {{ source('bronze', 'customers') }}
where customer_id is not null
