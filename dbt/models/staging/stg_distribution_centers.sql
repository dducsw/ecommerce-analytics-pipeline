with source as (
    select * from {{ source('thelook_ecommerce', 'distribution_centers') }}
),
renamed as (
    select
        id as distribution_center_id,
        name,
        latitude,
        longitude
    from source
    where id is not null
)
select * from renamed
