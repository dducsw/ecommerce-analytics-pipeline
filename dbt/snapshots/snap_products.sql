{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='id',
        check_cols=[
            'cost',
            'retail_price',
            'name',
            'brand',
            'category',
            'department',
            'sku',
            'distribution_center_id'
        ],
        invalidate_hard_deletes=True
    )
}}

select
    id,
    cost,
    category,
    name,
    brand,
    retail_price,
    department,
    sku,
    distribution_center_id
from {{ source('thelook_ecommerce', 'products') }}
where id is not null

{% endsnapshot %}
