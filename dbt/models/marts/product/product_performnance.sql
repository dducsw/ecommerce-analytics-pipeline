{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('fact_sales') }}
    where is_completed = true  -- Only completed sales
),

products as (
    select * from {{ ref('dim_products') }}
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
        avg(sale_price) as avg_sale_price,
        avg(gross_profit) as avg_profit_per_unit,
        avg(gross_margin_pct) as avg_margin_pct,
        
        -- Discount Metrics
        count(case when is_discounted then 1 end) as discounted_sales_count,
        avg(case when is_discounted then discount_pct end) as avg_discount_pct,
        sum(case when is_discounted then discount_amount else 0 end) as total_discount_amount,
        
        -- Returns
        count(case when is_returned then 1 end) as returned_units,
        round(count(case when is_returned then 1 end)::numeric / 
              nullif(count(*), 0) * 100, 2) as return_rate_pct,
        
        -- Fulfillment
        avg(hours_to_deliver) as avg_hours_to_deliver,
        avg(days_in_inventory) as avg_days_in_inventory,
        
        -- Time Metrics
        min(sale_timestamp) as first_sale_at,
        max(sale_timestamp) as last_sale_at,
        count(distinct sale_date) as days_with_sales
        
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
    p.profit_margin as theoretical_profit_margin,
    p.profit_margin_pct as theoretical_margin_pct,
    
    -- Current Inventory
    p.available_quantity as current_stock,
    p.sold_quantity as lifetime_sold_quantity,
    p.stock_status,
    p.sell_through_rate_pct,
    
    -- Sales Performance
    coalesce(pm.total_orders, 0) as total_orders,
    coalesce(pm.total_units_sold, 0) as total_units_sold,
    coalesce(pm.unique_customers, 0) as unique_customers,
    coalesce(pm.total_revenue, 0) as total_revenue,
    coalesce(pm.total_cogs, 0) as total_cogs,
    coalesce(pm.total_gross_profit, 0) as total_gross_profit,
    coalesce(pm.avg_sale_price, 0) as avg_sale_price,
    coalesce(pm.avg_profit_per_unit, 0) as avg_profit_per_unit,
    coalesce(pm.avg_margin_pct, 0) as avg_margin_pct,
    
    -- Discount Analysis
    coalesce(pm.discounted_sales_count, 0) as discounted_sales_count,
    pm.avg_discount_pct,
    coalesce(pm.total_discount_amount, 0) as total_discount_amount,
    round(coalesce(pm.discounted_sales_count, 0)::numeric / 
          nullif(coalesce(pm.total_units_sold, 0), 0) * 100, 2) as discount_frequency_pct,
    
    -- Returns Analysis
    coalesce(pm.returned_units, 0) as returned_units,
    coalesce(pm.return_rate_pct, 0) as return_rate_pct,
    
    -- Operational Metrics
    pm.avg_hours_to_deliver,
    pm.avg_days_in_inventory,
    pm.first_sale_at,
    pm.last_sale_at,
    coalesce(pm.days_with_sales, 0) as days_with_sales,
    
    -- Performance Indicators
    case 
        when coalesce(pm.total_units_sold, 0) = 0 then 'No Sales'
        when coalesce(pm.total_units_sold, 0) < 10 then 'Low Performer'
        when coalesce(pm.total_units_sold, 0) < 100 then 'Medium Performer'
        when coalesce(pm.total_units_sold, 0) < 500 then 'High Performer'
        else 'Star Product'
    end as performance_tier,
    
    case 
        when coalesce(pm.return_rate_pct, 0) > 20 then 'High Return Risk'
        when coalesce(pm.return_rate_pct, 0) > 10 then 'Medium Return Risk'
        else 'Low Return Risk'
    end as return_risk_category,
    
    -- Distribution
    p.distribution_center_id,
    p.distribution_center_name

from products p
left join product_metrics pm on p.product_id = pm.product_id
