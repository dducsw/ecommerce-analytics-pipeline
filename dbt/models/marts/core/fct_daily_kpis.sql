with daily_orders as (
    select
        order_date as date,
        count(distinct order_id) as total_orders,
        count(distinct user_id) as unique_customers,
        sum(total_sale_amount) as total_revenue,
        sum(total_cost_amount) as total_cost,
        sum(total_profit_amount) as total_profit,
        sum(returned_items_count) as total_returned_items
    from {{ ref('fact_orders') }}
    group by 1
),

daily_units as (
    select
        sale_date as date,
        count(*) as total_units_sold
    from {{ ref('fact_sales') }}
    group by 1
)

select
    o.date,
    o.total_orders,
    o.unique_customers,
    o.total_revenue,
    o.total_cost,
    o.total_profit,
    o.total_returned_items,
    u.total_units_sold,
    round(o.total_revenue / nullif(o.total_orders, 0), 2) as avg_order_value,
    round(o.total_profit / nullif(o.total_revenue, 0) * 100, 2) as profit_margin_pct
from daily_orders o
left join daily_units u on o.date = u.date
order by 1 desc
