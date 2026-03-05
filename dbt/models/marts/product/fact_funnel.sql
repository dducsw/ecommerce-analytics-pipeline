{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
)

select
    cast(e.created_at as date)                                                      as event_creation_date,
    e.traffic_source,
    e.browser,

    -- Session counts by funnel stage
    count(distinct e.session_id)                                                    as total_sessions,
    count(distinct case when e.event_type = 'product'  then e.session_id end)       as product_count,
    count(distinct case when e.event_type = 'cart'     then e.session_id end)       as cart_count,
    count(distinct case when e.event_type = 'purchase' then e.session_id end)       as purchase_count,

    -- Conversion Rates
    round(
        cast(count(distinct case when e.event_type = 'cart' then e.session_id end) as numeric) /
        nullif(count(distinct case when e.event_type = 'product' then e.session_id end), 0) * 100, 2
    )                                                                                as view_to_cart_rate,

    round(
        cast(count(distinct case when e.event_type = 'purchase' then e.session_id end) as numeric) /
        nullif(count(distinct case when e.event_type = 'cart' then e.session_id end), 0) * 100, 2
    )                                                                                as cart_to_purchase_rate,

    round(
        cast(count(distinct case when e.event_type = 'purchase' then e.session_id end) as numeric) /
        nullif(count(distinct case when e.event_type = 'product' then e.session_id end), 0) * 100, 2
    )                                                                                as overall_conversion_rate

from events as e
where e.event_type in ('product', 'cart', 'purchase')
group by cast(e.created_at as date), e.traffic_source, e.browser
