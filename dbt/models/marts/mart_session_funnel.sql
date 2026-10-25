{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
),

funnel_counts as (
    select
        count(distinct case when event_type = 'page_view'          then session_id end) as sessions_with_page_view,
        count(distinct case when event_type = 'signup_completed'   then session_id end) as sessions_with_signup,
        count(distinct case when event_type = 'product_viewed'     then session_id end) as sessions_with_product_view,
        count(distinct case when event_type = 'add_to_cart'        then session_id end) as sessions_with_add_to_cart,
        count(distinct case when event_type = 'checkout_started'   then session_id end) as sessions_with_checkout,
        count(distinct case when event_type = 'payment_succeeded'  then session_id end) as sessions_with_payment,
        count(distinct session_id)                                                       as total_sessions,
        count(distinct user_id)                                                          as total_users,
        count(*)                                                                         as total_events
    from events
),

channel_funnel as (
    select
        channel,
        count(distinct session_id)                                                                  as sessions,
        count(distinct case when event_type = 'payment_succeeded' then session_id end)              as converting_sessions,
        count(distinct case when event_type = 'payment_succeeded' then user_id end)                 as converting_users,
        sum(case when event_type = 'payment_succeeded' then amount_usd else 0 end)                  as total_revenue_usd
    from events
    group by 1
)

select
    fc.total_sessions,
    fc.total_users,
    fc.total_events,
    fc.sessions_with_page_view,
    fc.sessions_with_signup,
    fc.sessions_with_product_view,
    fc.sessions_with_add_to_cart,
    fc.sessions_with_checkout,
    fc.sessions_with_payment,
    case
        when fc.sessions_with_signup > 0
        then round(fc.sessions_with_product_view * 100.0 / fc.sessions_with_signup, 2)
        else 0
    end as signup_to_product_view_pct,
    case
        when fc.sessions_with_add_to_cart > 0
        then round(fc.sessions_with_checkout * 100.0 / fc.sessions_with_add_to_cart, 2)
        else 0
    end as cart_to_checkout_pct,
    case
        when fc.sessions_with_checkout > 0
        then round(fc.sessions_with_payment * 100.0 / fc.sessions_with_checkout, 2)
        else 0
    end as checkout_to_payment_pct,
    case
        when fc.total_sessions > 0
        then round(fc.sessions_with_payment * 100.0 / fc.total_sessions, 2)
        else 0
    end as overall_conversion_pct
from funnel_counts fc
