-- Staging model for site metadata
-- Combines Missouri Basin sites with detailed metadata

with sites as (
    select
        site_id,
        station_name,
        latitude,
        longitude,
        huc_code,
        altitude
    from {{ source('raw', 'sites_missouri_basin') }}
),

metadata as (
    select
        site_id,
        station_name,
        latitude,
        longitude,
        drainage_area_sq_mi,
        huc_code
    from {{ source('raw', 'site_metadata') }}
),

combined as (
    select
        coalesce(m.site_id, s.site_id) as site_id,
        coalesce(m.station_name, s.station_name) as station_name,
        coalesce(m.latitude, s.latitude) as latitude,
        coalesce(m.longitude, s.longitude) as longitude,
        m.drainage_area_sq_mi,
        coalesce(m.huc_code, s.huc_code) as huc_code,
        s.altitude as altitude_ft,
        -- Extract 2-digit HUC region
        cast(left(cast(coalesce(m.huc_code, s.huc_code) as varchar), 2) as varchar) as huc_region
    from sites s
    left join metadata m on s.site_id = m.site_id
)

select * from combined
where latitude is not null
  and longitude is not null
