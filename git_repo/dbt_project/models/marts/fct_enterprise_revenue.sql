
with orders as (
    select * from {{ ref('stg_enterprise_orders') }}
),
customers as (
    select * from {{ ref('stg_enterprise_customers') }}
)

select
    c.customer_tier,
    o.currency,
    count(distinct o.order_id) as total_order_count,
    count(distinct c.customer_id) as unique_active_customers,
    round(sum(o.subtotal_amount), 2) as gross_subtotal,
    round(sum(o.discount_amount), 2) as total_discounts_granted,
    round(sum(o.total_amount), 2) as net_realized_revenue,
    round(avg(o.total_amount), 2) as average_order_value
from orders o
join customers c on o.customer_id = c.customer_id
where o.order_status in ('completed', 'shipped', 'processing')
group by 1, 2
order by net_realized_revenue desc
