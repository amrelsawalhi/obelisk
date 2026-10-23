
select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,
    cast(amount as numeric(10, 2)) as amount,
    cast(status as varchar(50)) as status,
    cast(created_at as timestamp) as order_timestamp
from {{ source('landing', 'orders') }}
where order_id is not null
