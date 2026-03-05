/*
  dim_products - SCD Type 2 Dimension
  --------------------------------------
  Grain  : one row per product version (current + historical)
  Source : snap_products snapshot (dbt SCD2, check strategy)
  Filter : is_current = true -> current product attributes
           is_current = false -> historical attribute versions

  SCD2 columns added by dbt snapshot:
    dbt_scd_id      – surrogate key for this specific version
    dbt_valid_from  – when this version became active
    dbt_valid_to    – when this version was superseded (NULL = still active)
    dbt_updated_at  – timestamp this record was captured

  Tracked SCD2 columns (check strategy):
    cost, retail_price, name, brand, category, department, sku, distribution_center_id
*/
{{ config(materialized='table') }}

with products_snapshot as (
    select * from {{ ref('snap_products') }}
),

inventory as (
    select * from {{ ref('int_inventory_availability') }}
),

distribution_centers as (
    select * from {{ ref('stg_distribution_centers') }}
),

products_with_scd2 as (
    select
        -- Surrogate / Natural Keys
        dbt_scd_id                          as product_scd_id,  -- surrogate key
        id                                  as product_id,          -- natural key

        -- SCD2 Validity Window
        dbt_valid_from                      as valid_from,
        dbt_valid_to                        as valid_to,
        (dbt_valid_to is null)              as is_current,

        -- Product Attributes (tracked for SCD2)
        name                                as product_name,
        sku,
        brand,
        category,
        department,
        cost,
        retail_price,
        distribution_center_id

    from products_snapshot
)

select
    -- Keys 
    p.product_scd_id,
    p.product_id,

    -- SCD2 Metadata
    p.valid_from,
    p.valid_to,
    p.is_current,

    -- Product Attributes
    p.product_name,
    p.sku,
    p.brand,
    p.category,
    p.department,

    -- Pricing (versioned: captured at time of change)
    p.cost                      as product_cost,
    p.retail_price,
    p.retail_price - p.cost     as profit_margin,
    round(cast((p.retail_price - p.cost) / nullif(p.retail_price, 0) * 100 as numeric), 2)    as profit_margin_pct,

    -- Distribution
    p.distribution_center_id,
    dc.name                     as distribution_center_name,
    dc.latitude                 as dc_latitude,
    dc.longitude                as dc_longitude,

    -- Inventory Metrics (current snapshot, joined on natural key)
    coalesce(inv.available_quantity, 0)     as available_quantity,
    coalesce(inv.sold_quantity, 0)          as sold_quantity,
    coalesce(inv.available_quantity, 0) + coalesce(inv.sold_quantity, 0)    as total_inventory,

    -- Inventory Status
    case
        when coalesce(inv.available_quantity, 0) = 0  then 'Out of Stock'
        when coalesce(inv.available_quantity, 0) < 10 then 'Low Stock'
        else                                               'In Stock'
    end                         as stock_status

from products_with_scd2 p
left join inventory         inv on p.product_id          = inv.product_id
left join distribution_centers dc on p.distribution_center_id = dc.distribution_center_id
