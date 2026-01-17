{{ config(materialized='incremental') }}

with orders as (
    select
        order_id,
        status as status_order,
        created_at::timestamp,
        shipped_at::timestamp,
        delivered_at::timestamp,
        returned_at::timestamp
    from {{ ref('stg_orders') }}
)

select
    order_id,
    status_order,
    'created' as status,
    created_at as status_at,
    1 as event_seq
from orders
where created_at is not null

union all
select
    order_id,
    status_order,
    'shipped',
    shipped_at,
    2
from orders
where shipped_at is not null

union all
select
    order_id,
    status_order,
    'delivered',
    delivered_at,
    3
from orders
where delivered_at is not null

union all
select
    order_id,
    status_order,
    'returned',
    returned_at,
    4
from orders
where returned_at is not null
