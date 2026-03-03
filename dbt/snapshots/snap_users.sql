{% snapshot snap_users %}

{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    id,
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
from {{ source('thelook_ecommerce', 'users') }}
where id is not null

{% endsnapshot %}
