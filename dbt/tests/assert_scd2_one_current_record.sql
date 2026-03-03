-- Test: Ensure each user/product has exactly one current record in SCD2 dimensions
-- This test will fail if any entity has 0 or multiple is_current = true records

with user_current_counts as (
    select 
        user_id,
        count(*) as current_record_count
    from {{ ref('dim_users') }}
    where is_current = true
    group by user_id
    having count(*) != 1
),
product_current_counts as (
    select 
        product_id,
        count(*) as current_record_count
    from {{ ref('dim_products') }}
    where is_current = true
    group by product_id
    having count(*) != 1
)
select 
    'dim_users' as dimension_table,
    user_id::text as entity_id,
    current_record_count
from user_current_counts

union all

select 
    'dim_products' as dimension_table,
    product_id::text as entity_id,
    current_record_count
from product_current_counts
