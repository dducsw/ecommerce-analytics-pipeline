/*
  dim_users - SCD Type 2 Dimension
  ----------------------------------------
  Grain  : one row per user version (current + historical)
  Source : snap_users snapshot (dbt SCD2)
  Filter : is_current = true  → current user attributes
           is_current = false → historical versions

  SCD2 columns added by dbt snapshot:
    dbt_scd_id      – surrogate key for this specific version
    dbt_valid_from  – when this version became active
    dbt_valid_to    – when this version was superseded (NULL = still active)
    dbt_updated_at  – source updated_at at the time of capture
*/
{{ config(materialized='table') }}

with users_snapshot as (
    select * from {{ ref('snap_users') }}
),

user_stats as (
    select * from {{ ref('int_user_order_stats') }}
),

user_dc as (
    select * from {{ ref('int_user_closest_dc') }}
),

users_with_scd2 as (
    select
        -- Surrogate / Natural Keys
        dbt_scd_id                          as user_scd_id,   -- surrogate key
        id                                  as user_id,           -- natural key

        -- SCD2 Validity Window
        dbt_valid_from                      as valid_from,
        dbt_valid_to                        as valid_to,
        (dbt_valid_to is null)              as is_current,

        -- User Demographics─
        first_name,
        last_name,
        email,
        gender,
        age,

        -- Location
        country,
        state,
        city,
        postal_code,
        street_address,
        latitude,
        longitude,

        -- Acquisition─
        traffic_source,
        created_at                          as user_created_at,
        updated_at                          as user_updated_at

    from users_snapshot
)

select
    -- Keys
    u.user_scd_id,
    u.user_id,

    -- SCD2 Metadata
    u.valid_from,
    u.valid_to,
    u.is_current,

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
    u.user_created_at,
    u.user_updated_at,

    -- Closest Distribution Center (current snapshot only)
    coalesce(dc.distribution_center_id, -1) as closest_distribution_center_id,
    coalesce(dc.dc_name, 'Unknown')         as closest_dc_name,
    dc.distance_in_kms                      as distance_to_closest_dc_kms,

    -- Customer Order Metrics (as of latest known state)
    coalesce(us.total_orders, 0)            as lifetime_orders,
    coalesce(us.total_items, 0)             as lifetime_items,
    us.first_order_at,
    us.last_order_at,

    -- Derived Customer Status
    case
        when us.first_order_at = us.last_order_at and us.first_order_at is not null
            then 'New'
        when us.first_order_at is not null
            then 'Returning'
        else 'Never Purchased'
    end as customer_status,

    {{ date_diff('us.last_order_at', 'us.first_order_at') }} as days_as_customer

from users_with_scd2 u
left join user_stats us on u.user_id = us.user_id
left join user_dc    dc on u.user_id = dc.user_id
