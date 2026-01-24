-- Site dimension combining Missouri Basin sites with metadata

with missouri_basin as (
    select
        site_id,
        station_name,
        latitude,
        longitude,
        huc_code,
        altitude_ft
    from {{ ref('stg_sites_missouri_basin') }}
),

metadata as (
    select
        site_id,
        station_name,
        latitude,
        longitude,
        drainage_area_sq_mi,
        huc_code
    from {{ ref('stg_site_metadata') }}
),

combined as (
    select
        coalesce(m.site_id, mb.site_id) as site_id,
        coalesce(m.station_name, mb.station_name) as station_name,
        coalesce(m.latitude, mb.latitude) as latitude,
        coalesce(m.longitude, mb.longitude) as longitude,
        m.drainage_area_sq_mi,
        coalesce(m.huc_code, mb.huc_code) as huc_code,
        mb.altitude_ft
    from missouri_basin mb
    left join metadata m on mb.site_id = m.site_id
    where mb.latitude is not null
      and mb.longitude is not null
)

select
    site_id,
    station_name,
    latitude,
    longitude,
    drainage_area_sq_mi,
    huc_code,
    altitude_ft
from combined
