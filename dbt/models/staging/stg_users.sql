with source as (
    select * from {{ source('thelook_ecommerce', 'users') }}
),

renamed as (
    select
        id as user_id,
        first_name,
        last_name,
        email,
        gender,
        age,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at
    from source
    where id is not null
)

select * from renamed