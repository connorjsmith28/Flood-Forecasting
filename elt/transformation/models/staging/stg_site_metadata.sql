select
    site_id,
    station_name,
    latitude,
    longitude,
    drainage_area_sq_mi,
    huc_code
from {{ source('raw', 'site_metadata') }}
