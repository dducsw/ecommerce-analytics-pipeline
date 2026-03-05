{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('fact_sales') }}
    where is_completed = true  -- Only completed sales
),

-- Filter to current SCD2 version for product attributes
products as (
    select * from {{ ref('dim_products') }}
    where is_current = true
),

product_metrics as (
    select
        product_id,
        
        -- Volume Metrics
        count(distinct order_id) as total_orders,
        count(distinct order_item_id) as total_units_sold,
        count(distinct user_id) as unique_customers,
        
        -- Revenue Metrics
        sum(sale_price) as total_revenue,
        sum(cost_of_goods_sold) as total_cogs,
        sum(gross_profit) as total_gross_profit,
        
        -- Discount Metrics
        count(case when is_discounted then 1 end) as discounted_sales_count,
        sum(case when is_discounted then discount_amount else 0 end) as total_discount_amount,
        
        -- Returns
        count(case when is_returned then 1 end) as returned_units,
        round(cast(count(case when is_returned then 1 end) as numeric) / 
              nullif(count(*), 0) * 100, 2) as return_rate_pct

    from sales
    group by product_id
)

select
    -- Product Info
    p.product_id,
    p.product_name,
    p.sku,
    p.brand,
    p.category,
    p.department,
    
    -- Pricing
    p.product_cost,
    p.retail_price,
    
    -- Current Inventory
    p.available_quantity as current_stock,
    p.sold_quantity as lifetime_sold_quantity,
    p.stock_status,
    
    -- Sales Performance
    coalesce(pm.total_orders, 0) as total_orders,
    coalesce(pm.total_units_sold, 0) as total_units_sold,
    coalesce(pm.unique_customers, 0) as unique_customers,
    coalesce(pm.total_revenue, 0) as total_revenue,
    coalesce(pm.total_cogs, 0) as total_cogs,
    coalesce(pm.total_gross_profit, 0) as total_gross_profit,
    
    -- Discount Analysis
    coalesce(pm.discounted_sales_count, 0) as discounted_sales_count,
    coalesce(pm.total_discount_amount, 0) as total_discount_amount,
    
    -- Returns Analysis
    coalesce(pm.returned_units, 0) as returned_units,
    coalesce(pm.return_rate_pct, 0) as return_rate_pct,
    
    
    -- Distribution
    p.distribution_center_id,
    p.distribution_center_name

from products p
left join product_metrics pm on p.product_id = pm.product_id
