{{ config(materialized='table') }}

select
    product_id,
    countif(sold_at is null) as available_quantity,
    countif(sold_at is not null) as sold_quantity
from {{ ref('stg_inventory_items') }}
group by product_id
