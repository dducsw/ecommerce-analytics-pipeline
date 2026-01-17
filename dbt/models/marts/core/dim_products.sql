{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
),

inventory as (
    select * from {{ ref('int_inventory_availability') }}
),

distribution_centers as (
    select * from {{ ref('stg_distribution_centers') }}
)

select
    -- Primary Key
    p.product_id,
    
    -- Product Attributes
    p.name as product_name,
    p.sku,
    p.brand,
    p.category,
    p.department,
    
    -- Pricing
    p.cost as product_cost,
    p.retail_price,
    p.retail_price - p.cost as profit_margin,
    round((p.retail_price - p.cost) / nullif(p.retail_price, 0) * 100, 2) as profit_margin_pct,
    
    -- Distribution
    p.distribution_center_id,
    dc.name as distribution_center_name,
    dc.latitude as dc_latitude,
    dc.longitude as dc_longitude,
    
    -- Inventory Metrics
    coalesce(inv.available_quantity, 0) as available_quantity,
    coalesce(inv.sold_quantity, 0) as sold_quantity,
    coalesce(inv.available_quantity, 0) + coalesce(inv.sold_quantity, 0) as total_inventory,

    -- Performance Indicators
    case 
        when coalesce(inv.sold_quantity, 0) = 0 then 0
        else round(coalesce(inv.sold_quantity, 0)::numeric / 
                  nullif(coalesce(inv.available_quantity, 0) + coalesce(inv.sold_quantity, 0), 0) * 100, 2)
    end as sell_through_rate_pct

from products p
left join inventory inv on p.product_id = inv.product_id
left join distribution_centers dc on p.distribution_center_id = dc.distribution_center_id
