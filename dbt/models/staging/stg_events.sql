with source as (
    select * from {{ source('thelook_ecommerce', 'events') }}
),
renamed as (
    select
        id as event_id,
        user_id,
        sequence_number,
        session_id,
        {{ cast_timestamp('created_at') }} as created_at,
        ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri,
        event_type
    from source
    where id is not null
)
select * from renamed
