{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_users') }}
),

user_stats as (
    select * from {{ ref('int_user_order_stats') }}
),

user_dc as (
    select * from {{ ref('int_user_closest_dc') }}
)

select
    -- Primary Key
    u.user_id,
    
    -- User Demographics
    u.first_name,
    u.last_name,
    u.email,
    u.gender,
    u.age,
    
    -- Location
    u.country,
    u.state,
    u.city,
    u.postal_code,
    u.street_address,
    u.latitude,
    u.longitude,
    
    -- Acquisition
    u.traffic_source,
    u.created_at as user_created_at,
    
    -- Closest Distribution Center
    coalesce(dc.distribution_center_id, -1) as closest_distribution_center_id,
    coalesce(dc.dc_name, 'Unknown') as closest_dc_name,
    dc.distance_in_kms as distance_to_closest_dc_kms,
    
    -- Customer Metrics
    coalesce(us.total_orders, 0) as lifetime_orders,
    coalesce(us.total_items, 0) as lifetime_items,
    us.first_order_at,
    us.last_order_at,
    
    -- Calculated Metrics
    case
        when us.first_order_at is null then 'Never Purchased'
        when us.first_order_at = us.last_order_at then 'New'
        else 'Returning'
    end as customer_status,
    
    datediff(day, us.first_order_at, us.last_order_at) as days_as_customer,
    
    -- Metadata
    u.update_at as updated_at

from users u
left join user_stats us on u.user_id = us.user_id
left join user_dc dc on u.user_id = dc.user_id
