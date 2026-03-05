{{ config(materialized='incremental', unique_key='order_item_id') }}

with order_items_enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

-- Use only current SCD2 versions
-- For point-in-time accuracy join on:
--   p.valid_from <= oie.order_item_created_at < coalesce(p.valid_to, 'infinity')
products as (
    select * from {{ ref('dim_products') }}
    where is_current = true
),

users as (
    select * from {{ ref('dim_users') }}
    where is_current = true
)

select
    -- Primary Key (Grain: order item level)
    oie.order_item_id,
    
    -- Foreign Keys
    oie.order_id,
    oie.user_id,
    oie.product_id,
    oie.inventory_item_id,
    
    -- Date Keys for joining with dim_date
    cast(oie.order_item_created_at as date) as sale_date,
    cast(oie.order_item_shipped_at as date) as shipped_date,
    cast(oie.order_item_delivered_at as date) as delivered_date,
    
    -- Timestamps
    oie.order_item_created_at as sale_timestamp,
    oie.order_item_updated_at,
    oie.order_item_shipped_at,
    oie.order_item_delivered_at,
    oie.order_item_returned_at,
    
    -- Status
    oie.order_item_status as sale_status,
    
    -- Financial Metrics
    oie.sale_price,
    oie.inventory_cost as cost_of_goods_sold,
    oie.sale_price - oie.inventory_cost as gross_profit,
    round(cast((oie.sale_price - oie.inventory_cost) / nullif(oie.sale_price, 0) * 100 as numeric), 2) as gross_margin_pct,
    
    -- Product Context
    p.product_name,
    p.brand as product_brand,
    p.category as product_category,
    p.department as product_department,
    p.retail_price as product_retail_price,
    oie.sale_price - p.retail_price as discount_amount,
    case 
        when p.retail_price > 0 
        then round(cast((p.retail_price - oie.sale_price) / p.retail_price * 100 as numeric), 2) 
        else 0 
    end as discount_pct,
    
    -- Customer Context
    u.customer_status,
    u.traffic_source as customer_traffic_source,
    u.state as customer_state,
    u.country as customer_country,
    
    -- Distribution
    oie.product_distribution_center_id as distribution_center_id,
    p.distribution_center_name,
    
    -- Inventory Context
    oie.inventory_created_at,
    oie.inventory_sold_at,
    {{ date_diff('oie.inventory_sold_at', 'oie.inventory_created_at') }} as days_in_inventory,
    
    -- Fulfillment Metrics (in hours)
    round({{ timestamp_diff_hours('oie.order_item_shipped_at', 'oie.order_item_created_at') }}, 2) as hours_to_ship,
    round({{ timestamp_diff_hours('oie.order_item_delivered_at', 'oie.order_item_shipped_at') }}, 2) as hours_in_transit,
    round({{ timestamp_diff_hours('oie.order_item_delivered_at', 'oie.order_item_created_at') }}, 2) as hours_to_deliver,
    
    -- Sale Flags
    case when oie.order_item_status = 'Returned' then true else false end as is_returned,
    case when oie.order_item_status = 'Cancelled' then true else false end as is_cancelled,
    case when oie.order_item_status = 'Complete' then true else false end as is_completed,
    case when oie.sale_price < p.retail_price then true else false end as is_discounted,
    
    -- Load timestamp
    current_timestamp as dbt_updated_at

from order_items_enriched oie
left join products p on oie.product_id = p.product_id
left join users u on oie.user_id = u.user_id

{% if is_incremental() %}
    where oie.order_item_updated_at > (select max(order_item_updated_at) from {{ this }})
{% endif %}
