{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
)

select 
    cast(created_at as date) as event_date,
    date_trunc('hour', created_at) as event_hour,
    traffic_source,
    browser,
    
    -- Session counts by funnel stage
    count(distinct case when event_type = 'product' then session_id end) as product_view_sessions,
    count(distinct case when event_type = 'cart' then session_id end) as add_to_cart_sessions,
    count(distinct case when event_type = 'purchase' then session_id end) as purchase_sessions,
    
    -- User counts
    count(distinct case when event_type = 'product' then user_id end) as product_view_users,
    count(distinct case when event_type = 'cart' then user_id end) as add_to_cart_users,
    count(distinct case when event_type = 'purchase' then user_id end) as purchase_users,
    
    -- Conversion rates
    round(
        count(distinct case when event_type = 'cart' then session_id end)::numeric /
        nullif(count(distinct case when event_type = 'product' then session_id end), 0) * 100, 2
    ) as view_to_cart_rate,
    
    round(
        count(distinct case when event_type = 'purchase' then session_id end)::numeric /
        nullif(count(distinct case when event_type = 'cart' then session_id end), 0) * 100, 2
    ) as cart_to_purchase_rate,
    
    round(
        count(distinct case when event_type = 'purchase' then session_id end)::numeric /
        nullif(count(distinct case when event_type = 'product' then session_id end), 0) * 100, 2
    ) as overall_conversion_rate,
    
    
from events
where event_type in ('product', 'cart', 'purchase')
group by 1, 2, 3, 4
