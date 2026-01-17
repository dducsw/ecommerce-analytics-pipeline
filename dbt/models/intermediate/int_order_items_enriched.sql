with order_items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
inventory_items as (
    select * from {{ ref('stg_inventory_items') }}
)

select
    -- Grain
    oi.order_item_id,

    -- Keys
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.inventory_item_id,

    -- Order item lifecycle
    oi.status as order_item_status,
    oi.created_at as order_item_created_at,
    oi.updated_at as order_item_updated_at,
    oi.shipped_at as order_item_shipped_at,
    oi.delivered_at as order_item_delivered_at,
    oi.returned_at as order_item_returned_at,
    oi.sale_price,

    -- Inventory context
    ii.cost as inventory_cost,
    ii.created_at as inventory_created_at,
    ii.sold_at as inventory_sold_at,
    ii.product_distribution_center_id

from order_items oi
left join orders o
  on oi.order_id = o.order_id
left join inventory_items ii
  on oi.inventory_item_id = ii.inventory_item_id
