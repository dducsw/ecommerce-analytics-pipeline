with source as (
    select * from {{ source('thelook_ecommerce', 'order_items') }}
),
renamed as (
    select
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        shipped_at::timestamp as shipped_at,
        delivered_at::timestamp as delivered_at,
        returned_at::timestamp as returned_at,
        sale_price
    from source
    where id is not null
)
select * from renamed
