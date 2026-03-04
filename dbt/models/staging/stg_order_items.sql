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
        {{ cast_timestamp('created_at') }} as created_at,
        {{ cast_timestamp('updated_at') }} as updated_at,
        {{ cast_timestamp('shipped_at') }} as shipped_at,
        {{ cast_timestamp('delivered_at') }} as delivered_at,
        {{ cast_timestamp('returned_at') }} as returned_at,
        sale_price
    from source
    where id is not null
)
select * from renamed
