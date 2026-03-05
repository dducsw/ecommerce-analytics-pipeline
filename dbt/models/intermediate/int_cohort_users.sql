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
        cast(date_trunc(min(order_created_at), month) as date) as cohort_month
    from orders
    group by user_id
)

select
    user_id,
    first_order_at,
    cohort_month,

    -- Convenience labels
    format_date('%Y-%m', cohort_month)                  as cohort_label,
    cast(extract(year from cohort_month) as int64)      as cohort_year,
    cast(extract(month from cohort_month) as int64)     as cohort_month_num

from first_order_per_user
