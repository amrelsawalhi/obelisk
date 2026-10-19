{{ config(materialized='table', schema='silver') }}

select
    cast(order_id as integer) as order_id,
    cast(order_uuid as uuid) as order_uuid,
    cast(customer_id as uuid) as customer_id,
    cast(order_status as varchar(50)) as order_status,
    cast(currency as varchar(3)) as currency,
    cast(subtotal_amount as numeric(12, 2)) as subtotal_amount,
    cast(tax_amount as numeric(12, 2)) as tax_amount,
    cast(coalesce(discount_amount, 0.0) as numeric(12, 2)) as discount_amount,
    cast(total_amount as numeric(12, 2)) as total_amount,
    cast(order_placed_at as timestamp with time zone) as order_placed_at,
    cast(shipping_address as jsonb)->>'city' as destination_city,
    cast(shipping_address as jsonb)->>'street' as destination_street,
    cast(coalesce(cast(shipping_address as jsonb)->>'is_commercial', 'false') as boolean) as is_commercial_delivery
from {{ source('bronze', 'ecommerce_orders') }}
where order_id is not null
