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
        {{ cast_timestamp('created_at') }} as created_at,
        {{ cast_timestamp('updated_at') }} as updated_at
    from source
    where id is not null
)

select * from renamed