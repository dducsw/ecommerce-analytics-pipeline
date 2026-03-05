SELECT
    d.year_month,
    SUM(f.total_sale_amount) AS total_revenue,
    SUM(f.total_profit_amount) AS total_profit
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_date') }} d 
    ON f.order_date = d.date_day
GROUP BY 1
ORDER BY 1 ASC;

SELECT
    f.order_id,
    f.order_date,
    d.year_month,
    d.year_quarter,
    f.total_sale_amount,
    f.total_profit_amount,
    f.hours_to_deliver,
    u.country AS customer_country,
    u.traffic_source,
    f.order_status
FROM marts.fact_orders f
LEFT JOIN marts.dim_date d 
    ON f.order_date = d.date_day
LEFT JOIN marts.dim_users u 
    ON f.user_id = u.user_id;