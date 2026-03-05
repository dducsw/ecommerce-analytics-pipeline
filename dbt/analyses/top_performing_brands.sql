-- Top Performing Brands Analysis
-- Identifies top 10 brands by revenue, along with their profit margins and unit volume.

select
    product_brand,
    sum(sale_price) as total_revenue,
    sum(gross_profit) as total_profit,
    count(*) as units_sold,
    round(sum(gross_profit) / nullif(sum(sale_price), 0) * 100, 2) as profit_margin_pct
from {{ ref('fact_sales') }}
where is_completed = true
group by 1
order by total_revenue desc
limit 10
