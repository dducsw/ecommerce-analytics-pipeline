-- Monthly Sales Growth Analysis
-- This query calculates Month-over-Month (MoM) growth for Revenue and Orders.

with monthly_sales as (
    select
        date_trunc(order_date, month) as sale_month,
        sum(total_sale_amount) as revenue,
        count(distinct order_id) as orders
    from {{ ref('fact_orders') }}
    group by 1
),

growth_metrics as (
    select
        sale_month,
        revenue,
        lag(revenue) over (order by sale_month) as prev_month_revenue,
        orders,
        lag(orders) over (order by sale_month) as prev_month_orders
    from monthly_sales
)

select
    sale_month,
    revenue,
    round((revenue - prev_month_revenue) / nullif(prev_month_revenue, 0) * 100, 2) as revenue_growth_pct,
    orders,
    round((orders - prev_month_orders) / nullif(prev_month_orders, 0) * 100, 2) as orders_growth_pct
from growth_metrics
order by 1 desc
