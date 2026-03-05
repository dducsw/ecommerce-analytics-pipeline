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
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('thelook_ecommerce', 'users') }}
where id is not null

{% endsnapshot %}
