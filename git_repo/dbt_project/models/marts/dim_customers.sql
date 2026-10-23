
select
    c.customer_id,
    c.customer_name,
    c.customer_tier,
    c.country,
    count(o.order_id) as lifetime_orders,
    coalesce(sum(o.amount), 0) as lifetime_spend
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name, c.customer_tier, c.country
