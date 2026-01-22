-- Dimension table for monitoring sites
-- Contains all site attributes for analysis

with sites as (
    select * from {{ ref('stg_sites') }}
)

select
    site_id,
    station_name,
    latitude,
    longitude,
    drainage_area_sq_mi,
    altitude_ft,
    huc_code,
    huc_region,
    -- Computed fields
    case
        when drainage_area_sq_mi < 10 then 'small'
        when drainage_area_sq_mi < 100 then 'medium'
        when drainage_area_sq_mi < 1000 then 'large'
        else 'very_large'
    end as basin_size_category
from sites
