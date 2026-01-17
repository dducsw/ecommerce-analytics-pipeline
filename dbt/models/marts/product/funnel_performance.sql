{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
)

products as (
    select * from {{ ref('dim_products') }}
),

-- Extract product_id from URI for product page views
product_views as (
    select
        user_id,
        session_id,
        created_at,
        -- Extract product ID from URI (assuming format: /products/12345)
        case 
            when uri like '%/products/%' then 
                cast(regexp_replace(split_part(uri, '/products/', 2), '[^0-9].*$', '') as integer)
            else null
        end as product_id,
        event_type
    from events
    where event_type in ('product', 'cart', 'purchase')
      and uri like '%/products/%'
),

-- Aggregate funnel metrics by product
product_funnel as (
    select
        product_id,
        
        -- Funnel Stages
        count(distinct case when event_type = 'product' then session_id end) as product_views_sessions,
        count(distinct case when event_type = 'product' then user_id end) as product_views_users,
        count(case when event_type = 'product' then 1 end) as product_view_events,
        
        count(distinct case when event_type = 'cart' then session_id end) as add_to_cart_sessions,
        count(distinct case when event_type = 'cart' then user_id end) as add_to_cart_users,
        count(case when event_type = 'cart' then 1 end) as add_to_cart_events,
        
        count(distinct case when event_type = 'purchase' then session_id end) as purchase_sessions,
        count(distinct case when event_type = 'purchase' then user_id end) as purchase_users,
        count(case when event_type = 'purchase' then 1 end) as purchase_events
        
    from product_views
    where product_id is not null
    group by product_id
),

-- Actual purchase data from orders
actual_purchases as (
    select
        product_id,
        count(distinct order_id) as actual_orders,
        count(distinct order_item_id) as actual_units_sold,
        count(distinct user_id) as actual_customers,
        sum(sale_price) as actual_revenue
    from order_items
    where status = 'Complete'
    group by product_id
)

select
    -- Product Info
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.department,
    
    -- Funnel Metrics - Sessions
    coalesce(pf.product_views_sessions, 0) as product_views_sessions,
    coalesce(pf.add_to_cart_sessions, 0) as add_to_cart_sessions,
    coalesce(pf.purchase_sessions, 0) as purchase_sessions,
    
    -- Funnel Metrics - Users
    coalesce(pf.product_views_users, 0) as product_views_users,
    coalesce(pf.add_to_cart_users, 0) as add_to_cart_users,
    coalesce(pf.purchase_users, 0) as purchase_users,
    
    -- Funnel Metrics - Events
    coalesce(pf.product_view_events, 0) as product_view_events,
    coalesce(pf.add_to_cart_events, 0) as add_to_cart_events,
    coalesce(pf.purchase_events, 0) as purchase_events,
    
    -- Actual Sales (from orders)
    coalesce(ap.actual_orders, 0) as actual_orders,
    coalesce(ap.actual_units_sold, 0) as actual_units_sold,
    coalesce(ap.actual_customers, 0) as actual_customers,
    coalesce(ap.actual_revenue, 0) as actual_revenue,
    
    -- Conversion Rates (Session-based)
    round(
        coalesce(pf.add_to_cart_sessions, 0)::numeric / 
        nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 
        2
    ) as view_to_cart_conversion_rate,
    
    round(
        coalesce(pf.purchase_sessions, 0)::numeric / 
        nullif(coalesce(pf.add_to_cart_sessions, 0), 0) * 100, 
        2
    ) as cart_to_purchase_conversion_rate,
    
    round(
        coalesce(pf.purchase_sessions, 0)::numeric / 
        nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 
        2
    ) as overall_conversion_rate,
    
    -- Engagement Metrics
    round(
        coalesce(pf.product_view_events, 0)::numeric / 
        nullif(coalesce(pf.product_views_sessions, 0), 0), 
        2
    ) as avg_views_per_session,
    
    round(
        coalesce(pf.add_to_cart_events, 0)::numeric / 
        nullif(coalesce(pf.add_to_cart_sessions, 0), 0), 
        2
    ) as avg_carts_per_session,
    
    -- Drop-off Analysis
    coalesce(pf.product_views_sessions, 0) - coalesce(pf.add_to_cart_sessions, 0) as view_to_cart_dropoff,
    coalesce(pf.add_to_cart_sessions, 0) - coalesce(pf.purchase_sessions, 0) as cart_to_purchase_dropoff,
    
    round(
        (coalesce(pf.product_views_sessions, 0) - coalesce(pf.add_to_cart_sessions, 0))::numeric / 
        nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 
        2
    ) as view_to_cart_dropoff_rate,
    
    round(
        (coalesce(pf.add_to_cart_sessions, 0) - coalesce(pf.purchase_sessions, 0))::numeric / 
        nullif(coalesce(pf.add_to_cart_sessions, 0), 0) * 100, 
        2
    ) as cart_to_purchase_dropoff_rate,
    
    -- Performance Indicators
    case 
        when coalesce(pf.product_views_sessions, 0) = 0 then 'No Traffic'
        when round(coalesce(pf.purchase_sessions, 0)::numeric / nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 2) < 1 then 'Poor Conversion'
        when round(coalesce(pf.purchase_sessions, 0)::numeric / nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 2) < 5 then 'Average Conversion'
        when round(coalesce(pf.purchase_sessions, 0)::numeric / nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 2) < 10 then 'Good Conversion'
        else 'Excellent Conversion'
    end as conversion_performance,
    
    case 
        when coalesce(pf.product_views_sessions, 0) = 0 then 'No Data'
        when round((coalesce(pf.product_views_sessions, 0) - coalesce(pf.add_to_cart_sessions, 0))::numeric / nullif(coalesce(pf.product_views_sessions, 0), 0) * 100, 2) > 80 then 'High Browse Dropoff'
        when round((coalesce(pf.add_to_cart_sessions, 0) - coalesce(pf.purchase_sessions, 0))::numeric / nullif(coalesce(pf.add_to_cart_sessions, 0), 0) * 100, 2) > 80 then 'High Cart Abandonment'
        else 'Healthy Funnel'
    end as funnel_health

from products p
left join product_funnel pf on p.product_id = pf.product_id
left join actual_purchases ap on p.product_id = ap.product_id
