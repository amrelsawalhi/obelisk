{{ config(materialized='table', schema='silver') }}

select
    cast(customer_id as uuid) as customer_id,
    cast(first_name as varchar(100)) as first_name,
    cast(last_name as varchar(100)) as last_name,
    cast(email as varchar(255)) as email,
    cast(tier as varchar(50)) as customer_tier,
    cast(country_code as varchar(3)) as country_code,
    cast(signup_timestamp as timestamp with time zone) as signup_at,
    cast(metadata_json as jsonb)->>'referral_source' as referral_source,
    cast(metadata_json as jsonb)->>'preferred_channel' as preferred_channel,
    cast(coalesce(cast(metadata_json as jsonb)->>'vip_flag', 'false') as boolean) as is_vip
from {{ source('bronze', 'ecommerce_customers') }}
where customer_id is not null
