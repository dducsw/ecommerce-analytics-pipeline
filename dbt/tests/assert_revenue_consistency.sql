-- Test: Verify revenue consistency between fact_sales and fact_orders
-- This test will fail if aggregated sales revenue doesn't match order-level revenue

with sales_revenue as (
    select 
        order_id,
        sum(sale_price) as sales_total_revenue
    from {{ ref('fact_sales') }}
    group by order_id
),
order_revenue as (
    select 
        order_id,
        total_sale_amount
    from {{ ref('fact_orders') }}
)
select 
    sr.order_id,
    sr.sales_total_revenue,
    or_.total_sale_amount,
    abs(sr.sales_total_revenue - or_.total_sale_amount) as revenue_diff
from sales_revenue sr
join order_revenue or_ 
    on sr.order_id = or_.order_id
where abs(sr.sales_total_revenue - or_.total_sale_amount) > 0.01  -- tolerance for floating point
