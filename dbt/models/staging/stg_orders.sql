with source as (
    select * from {{ source('thelook_ecommerce', 'orders') }}
),
renamed as (
    select
        order_id,
        user_id,
        status,
        gender,
        {{ cast_timestamp('created_at') }} as created_at,
        {{ cast_timestamp('updated_at') }} as updated_at,
        {{ cast_timestamp('returned_at') }} as returned_at,
        {{ cast_timestamp('shipped_at') }} as shipped_at,
        {{ cast_timestamp('delivered_at') }} as delivered_at,
        num_of_item
    from source
    where order_id is not null
)
select * from renamed
