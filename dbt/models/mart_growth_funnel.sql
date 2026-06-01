with base_events as (
    select
        event_type,
        user_id,
        session_id,
        occurred_at
    from {{ ref('fact_events_seed') }}
)

select
    count(case when event_type = 'signup_completed' then 1 end) as signup_completed,
    count(case when event_type = 'checkout_started' then 1 end) as checkout_started,
    count(case when event_type = 'payment_succeeded' then 1 end) as payment_succeeded
from base_events
