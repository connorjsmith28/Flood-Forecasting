-- Site dimension from USGS site metadata

select
    site_id,
    station_name,
    latitude,
    longitude,
    drainage_area_sq_mi,
    huc_code
from {{ ref('stg_site_metadata') }}
where latitude is not null
  and longitude is not null
