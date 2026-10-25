with source as (
    select * from {{ source('bronze', 'events') }}
)
select
    event_id,
    event_type,
    user_id,
    session_id,
    channel,
    page,
    product_id,
    product_name,
    product_category,
    order_id,
    cast(amount_usd as double) as amount_usd,
    cast(occurred_at as timestamp)  as occurred_at,
    event_version,
    source
from source
where event_id is not null
  and event_type is not null
  and user_id is not null
