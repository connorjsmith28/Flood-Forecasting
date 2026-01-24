select
    site_id,
    station_name,
    latitude,
    longitude,
    huc_code,
    altitude as altitude_ft
from {{ source('raw', 'sites_missouri_basin') }}
