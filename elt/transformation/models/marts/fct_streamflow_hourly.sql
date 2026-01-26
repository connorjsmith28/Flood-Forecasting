-- Hourly streamflow joined with weather
-- Joins to dim_sites to get lat/long coordinates for weather matching

with streamflow as (
    select
        site_id,
        observed_at,
        streamflow_cfs,
        gage_height_ft
    from {{ ref('stg_streamflow') }}
),

sites as (
    select
        site_id,
        latitude,
        longitude
    from {{ ref('dim_sites') }}
),

weather as (
    select
        latitude,
        longitude,
        observed_at,
        precipitation_mm,
        temperature_c,
        wind_speed_ms,
        humidity_pct
    from {{ ref('stg_weather') }}
),

streamflow_hourly as (
    select
        site_id,
        date_trunc('hour', observed_at) as observation_hour,
        avg(streamflow_cfs) as streamflow_cfs_mean,
        max(streamflow_cfs) as streamflow_cfs_max,
        min(streamflow_cfs) as streamflow_cfs_min,
        avg(gage_height_ft) as gage_height_ft_mean,
        max(gage_height_ft) as gage_height_ft_max,
        min(gage_height_ft) as gage_height_ft_min,
        count(*) as observation_count
    from streamflow
    group by site_id, date_trunc('hour', observed_at)
),

-- Join with dim_sites to get coordinates for weather matching
streamflow_with_coords as (
    select
        sf.site_id,
        sf.observation_hour,
        s.latitude,
        s.longitude,
        sf.streamflow_cfs_mean,
        sf.streamflow_cfs_max,
        sf.streamflow_cfs_min,
        sf.gage_height_ft_mean,
        sf.gage_height_ft_max,
        sf.gage_height_ft_min,
        sf.observation_count
    from streamflow_hourly as sf
    inner join sites as s on sf.site_id = s.site_id
),

final as (
    select
        sws.site_id,
        sws.observation_hour,
        sws.latitude,
        sws.longitude,
        sws.streamflow_cfs_mean,
        sws.streamflow_cfs_max,
        sws.streamflow_cfs_min,
        sws.gage_height_ft_mean,
        sws.gage_height_ft_max,
        sws.gage_height_ft_min,
        sws.observation_count,
        w.precipitation_mm,
        w.temperature_c,
        w.wind_speed_ms,
        w.humidity_pct
    from streamflow_with_coords as sws
    left join weather as w
        on
            round(sws.longitude, 2) = round(w.longitude, 2)
            and round(sws.latitude, 2) = round(w.latitude, 2)
            and sws.observation_hour = w.observed_at
)

select
    site_id,
    observation_hour,
    latitude,
    longitude,
    streamflow_cfs_mean,
    streamflow_cfs_max,
    streamflow_cfs_min,
    gage_height_ft_mean,
    gage_height_ft_max,
    gage_height_ft_min,
    observation_count,
    precipitation_mm,
    temperature_c,
    wind_speed_ms,
    humidity_pct
from final
