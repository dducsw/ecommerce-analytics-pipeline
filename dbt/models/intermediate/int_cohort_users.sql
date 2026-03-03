/*
  int_cohort_users
  ----------------------------
  Grain  : one row per user
  Purpose: Assign each user to their acquisition cohort month
           (the calendar month of their very first order).

  Used by: cohort_retention mart for monthly retention analysis.
*/
{{ config(materialized='table') }}

with orders as (
    select
        user_id,
        order_created_at
    from {{ ref('fact_orders') }}
    where is_cancelled = false
),

first_order_per_user as (
    select
        user_id,
        min(order_created_at)                           as first_order_at,
        date_trunc('month', min(order_created_at))::date as cohort_month
    from orders
    group by user_id
)

select
    user_id,
    first_order_at,
    cohort_month,

    -- Convenience labels
    to_char(cohort_month, 'YYYY-MM')                    as cohort_label,
    extract(year  from cohort_month)::int               as cohort_year,
    extract(month from cohort_month)::int               as cohort_month_num

from first_order_per_user
