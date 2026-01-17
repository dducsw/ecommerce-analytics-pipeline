{{ config(materialized='table') }}

select
    user_id,
    count(distinct order_id) as total_orders,
    sum(num_of_item) as total_items,
    min(created_at) as first_order_at,
    max(created_at) as last_order_at
from {{ ref('stg_orders') }}
group by user_id
