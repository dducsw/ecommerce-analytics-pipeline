with source as (
    select * from {{ source('thelook_ecommerce', 'inventory_items') }}
),
renamed as (
    select
        id as inventory_item_id,
        product_id,
        {{ cast_timestamp('created_at') }} as created_at,
        {{ cast_timestamp('sold_at') }} as sold_at,
        cost,
        product_category,
        product_name,
        product_brand,
        product_retail_price,
        product_department,
        product_sku,
        product_distribution_center_id
    from source
    where id is not null
)
select * from renamed
