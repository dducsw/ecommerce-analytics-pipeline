with distribution_centers as (
    select 
        distribution_center_id,
        name,
        latitude,
        longitude
    from {{ ref('stg_distribution_centers') }}
),

users as (
    select 
        user_id,
        latitude,
        longitude
    from {{ ref('stg_users') }}
    where latitude is not null 
      and longitude is not null
),

user_dc_geo_distance as (
    select 
        u.user_id,
        dc.distribution_center_id,
        dc.name as dc_name,
        {{ calculate_distance_km('u.longitude', 'u.latitude', 'dc.longitude', 'dc.latitude') }} as distance_in_kms
    from users as u
    cross join distribution_centers as dc
)
select user_id
	, distribution_center_id
	, dc_name
	, distance_in_kms
from user_dc_geo_distance qualify row_number() over (
		partition by user_id order by distance_in_kms asc) = 1