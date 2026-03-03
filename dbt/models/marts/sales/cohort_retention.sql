/*
  cohort_retention
  ---------------------------------------------------------------
  Grain  : one row per (cohort_month, period_number)
  Purpose: Monthly cohort retention analysis.
           • period_number = 0  -> the cohort's first month (acquisition)
           • period_number = 1  -> one month after acquisition
           • period_number = N  -> N months after acquisition

  Key metrics:
    cohort_size         – total users who placed their first order in this cohort month
    retained_users      – users from this cohort active in this period
    retention_rate_pct  – retained_users / cohort_size * 100
    total_revenue       – gross revenue from retained users in this period
    revenue_per_user    – avg revenue per retained user in this period

  Usage examples:
    -- Current month retention heat-map
    SELECT cohort_label, period_number, retention_rate_pct
    FROM {{ this }}
    ORDER BY cohort_month, period_number;

    -- 3-month retention for recent cohorts
    SELECT cohort_label, cohort_size,
           MAX(CASE WHEN period_number = 3 THEN retention_rate_pct END) AS m3_retention
    FROM {{ this }}
    GROUP BY 1, 2
    ORDER BY cohort_month DESC;
*/
{{ config(materialized='table') }}

with cohort_users as (
    select * from {{ ref('int_cohort_users') }}
),

-- All completed/non-cancelled orders with their month bucket
orders_monthly as (
    select
        o.user_id,
        date_trunc('month', o.order_created_at)::date   as order_month,
        sum(o.total_sale_amount)                         as period_revenue,
        count(distinct o.order_id)                       as period_orders
    from {{ ref('fact_orders') }} o
    where o.is_cancelled = false
    group by o.user_id, date_trunc('month', o.order_created_at)::date
),

-- Cohort size per cohort month
cohort_sizes as (
    select
        cohort_month,
        cohort_label,
        cohort_year,
        cohort_month_num,
        count(distinct user_id) as cohort_size
    from cohort_users
    group by cohort_month, cohort_label, cohort_year, cohort_month_num
),

-- Cross-join cohort users with their activity per month (>= cohort month)
cohort_activity as (
    select
        cu.cohort_month,
        om.order_month,

        -- Period number: months since cohort acquisition
        (
            extract(year  from age(om.order_month, cu.cohort_month)) * 12 +
            extract(month from age(om.order_month, cu.cohort_month))
        )::int                                           as period_number,

        count(distinct cu.user_id)                       as retained_users,
        sum(om.period_revenue)                           as total_revenue,
        sum(om.period_orders)                            as total_orders

    from cohort_users cu
    inner join orders_monthly om
           on  cu.user_id     = om.user_id
           and om.order_month >= cu.cohort_month
    group by
        cu.cohort_month,
        om.order_month
)

select
    --  Identifiers 
    cs.cohort_month,
    cs.cohort_label,
    cs.cohort_year,
    cs.cohort_month_num,
    ca.order_month          as activity_month,
    ca.period_number,

    --  Cohort Size 
    cs.cohort_size,

    --  Retention 
    ca.retained_users,
    round(
        ca.retained_users::numeric / nullif(cs.cohort_size, 0) * 100, 2
    )       as retention_rate_pct,

    --  Revenue 
    round(coalesce(ca.total_revenue, 0)::numeric, 2)    as total_revenue,
    round(
        coalesce(ca.total_revenue, 0)::numeric /
        nullif(ca.retained_users, 0), 2
    )       as revenue_per_retained_user,
    round(
        coalesce(ca.total_revenue, 0)::numeric /
        nullif(cs.cohort_size, 0), 2
    )       as revenue_per_cohort_user,

    --  Volume
    ca.total_orders,
    round(
        ca.total_orders::numeric / nullif(ca.retained_users, 0), 2
    )       as orders_per_retained_user

from cohort_activity ca
inner join cohort_sizes cs on ca.cohort_month = cs.cohort_month
order by cs.cohort_month, ca.period_number
