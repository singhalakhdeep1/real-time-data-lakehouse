{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
),

orders as (
    select
        order_id,
        user_id,
        session_id,
        channel,
        product_id,
        product_name,
        product_category,
        max(amount_usd)                                                     as order_amount_usd,
        min(occurred_at)                                                    as order_created_at,
        max(case when event_type = 'order_shipped'   then occurred_at end)  as order_shipped_at,
        max(case when event_type = 'order_delivered' then occurred_at end)  as order_delivered_at,
        count(distinct event_type)                                          as lifecycle_events
    from events
    where order_id is not null
    group by 1, 2, 3, 4, 5, 6, 7
),

order_stats as (
    select
        count(distinct order_id)                                                        as total_orders,
        count(distinct user_id)                                                         as ordering_users,
        sum(order_amount_usd)                                                           as total_revenue_usd,
        avg(order_amount_usd)                                                           as avg_order_value_usd,
        max(order_amount_usd)                                                           as max_order_value_usd,
        min(order_amount_usd)                                                           as min_order_value_usd,
        count(distinct case when order_shipped_at is not null then order_id end)        as shipped_orders,
        count(distinct case when order_delivered_at is not null then order_id end)      as delivered_orders,
        avg(
            case when order_shipped_at is not null
            then datediff('hour', order_created_at, order_shipped_at)
            end
        )                                                                               as avg_hours_to_ship,
        avg(
            case when order_delivered_at is not null
            then datediff('hour', order_created_at, order_delivered_at)
            end
        )                                                                               as avg_hours_to_deliver
    from orders
),

category_stats as (
    select
        product_category,
        count(distinct order_id)    as orders,
        sum(order_amount_usd)       as revenue_usd,
        avg(order_amount_usd)       as avg_order_value_usd,
        count(distinct user_id)     as unique_buyers
    from orders
    group by 1
)

select
    os.total_orders,
    os.ordering_users,
    round(os.total_revenue_usd, 2)      as total_revenue_usd,
    round(os.avg_order_value_usd, 2)    as avg_order_value_usd,
    round(os.max_order_value_usd, 2)    as max_order_value_usd,
    round(os.min_order_value_usd, 2)    as min_order_value_usd,
    os.shipped_orders,
    os.delivered_orders,
    case
        when os.total_orders > 0
        then round(os.shipped_orders * 100.0 / os.total_orders, 2)
        else 0
    end                                 as shipped_rate_pct,
    case
        when os.total_orders > 0
        then round(os.delivered_orders * 100.0 / os.total_orders, 2)
        else 0
    end                                 as delivered_rate_pct,
    round(coalesce(os.avg_hours_to_ship, 0), 1)     as avg_hours_to_ship,
    round(coalesce(os.avg_hours_to_deliver, 0), 1)  as avg_hours_to_deliver
from order_stats os
