-- Growth funnel mart: track user journey from signup to payment
with base_events as (
    select
        event_type,
        user_id,
        session_id,
        occurred_at
    from {{ ref('fact_events_seed') }}
),

funnel_metrics as (
    select
        count(case when event_type = 'signup_completed' then 1 end) as signup_completed,
        count(case when event_type = 'checkout_started' then 1 end) as checkout_started,
        count(case when event_type = 'payment_succeeded' then 1 end) as payment_succeeded
    from base_events
)

select
    signup_completed,
    checkout_started,
    payment_succeeded,
    case 
        when signup_completed > 0 then round(checkout_started::numeric / signup_completed::numeric * 100, 2)
        else 0 
    end as signup_to_checkout_rate,
    case 
        when checkout_started > 0 then round(payment_succeeded::numeric / checkout_started::numeric * 100, 2)
        else 0 
    end as checkout_to_payment_rate,
    case 
        when signup_completed > 0 then round(payment_succeeded::numeric / signup_completed::numeric * 100, 2)
        else 0 
    end as signup_to_payment_rate
from funnel_metrics
