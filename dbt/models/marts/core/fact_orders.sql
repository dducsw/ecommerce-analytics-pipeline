{{ config(materialized='incremental', unique_key='order_id') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

order_aggregates as (
    select
        order_id,
        count(distinct order_item_id) as total_items,
        sum(sale_price) as total_sale_amount,
        sum(inventory_cost) as total_cost_amount,
        sum(sale_price - inventory_cost) as total_profit_amount,
        avg(sale_price) as avg_item_price,
        
        -- Lifecycle metrics
        min(order_item_created_at) as first_item_created_at,
        max(order_item_updated_at) as last_item_updated_at,
        min(order_item_shipped_at) as first_item_shipped_at,
        max(order_item_delivered_at) as last_item_delivered_at,
        
        -- Status counts
        count(case when order_item_status = 'Returned' then 1 end) as returned_items_count,
        count(case when order_item_status = 'Cancelled' then 1 end) as cancelled_items_count,
        count(case when order_item_status = 'Complete' then 1 end) as completed_items_count
        
    from order_items
    group by order_id
)

select
    -- Primary Key
    o.order_id,
    
    -- Foreign Keys
    o.user_id,
    
    -- Order Attributes
    o.status as order_status,
    o.gender as customer_gender,
    o.num_of_item as order_line_items,
    
    -- Timestamps
    o.created_at as order_created_at,
    o.updated_at as order_updated_at,
    o.shipped_at as order_shipped_at,
    o.delivered_at as order_delivered_at,
    o.returned_at as order_returned_at,
    
    -- Date Keys for joining with dim_date
    cast(o.created_at as date) as order_date,
    cast(o.shipped_at as date) as shipped_date,
    cast(o.delivered_at as date) as delivered_date,
    
    -- Order Metrics from aggregates
    oa.total_items,
    oa.total_sale_amount,
    oa.total_cost_amount,
    oa.total_profit_amount,
    round(cast((oa.total_profit_amount / nullif(oa.total_sale_amount, 0)) * 100 as numeric), 2) as profit_margin_pct,
    oa.avg_item_price,
    
    -- Item Status
    oa.returned_items_count,
    oa.cancelled_items_count,
    oa.completed_items_count,
    
    -- Processing Time Metrics (in hours)
    round(cast(timestamp_diff(o.shipped_at, o.created_at, second) / 3600 as numeric), 2) as hours_to_ship,
    round(cast(timestamp_diff(o.delivered_at, o.shipped_at, second) / 3600 as numeric), 2) as hours_in_transit,
    round(cast(timestamp_diff(o.delivered_at, o.created_at, second) / 3600 as numeric), 2) as hours_to_deliver,
    
    -- Order Flags
    case when o.status = 'Returned' then true else false end as is_returned,
    case when o.status = 'Cancelled' then true else false end as is_cancelled,
    case when o.status = 'Complete' then true else false end as is_completed,
    case when oa.returned_items_count > 0 then true else false end as has_returned_items,
    
    -- Load timestamp
    current_timestamp as dbt_updated_at

from orders o
left join order_aggregates oa on o.order_id = oa.order_id

{% if is_incremental() %}
    where o.updated_at > (select max(order_updated_at) from {{ this }})
{% endif %}
