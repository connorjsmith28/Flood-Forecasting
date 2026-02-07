-- Hourly streamflow joined with NLDAS-3 watershed-averaged weather
-- Uses site_id join since NLDAS-3 data is already watershed-averaged

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
        site_id,
        observed_at,
        precipitation_mm,
        temperature_c,
        wind_speed_ms,
        specific_humidity_kgkg,
        surface_pressure_pa,
        shortwave_radiation_wm2,
        longwave_radiation_wm2,
        potential_evaporation_mm,
        cape_jkg,
        convective_precip_fraction
    from {{ ref('stg_nldas3_weather') }}
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

-- Join with dim_sites to get coordinates
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
        -- NLDAS-3 forcing variables (watershed-averaged)
        w.precipitation_mm,
        w.temperature_c,
        w.wind_speed_ms,
        w.specific_humidity_kgkg,
        w.surface_pressure_pa,
        w.shortwave_radiation_wm2,
        w.longwave_radiation_wm2,
        w.potential_evaporation_mm,
        w.cape_jkg,
        w.convective_precip_fraction
    from streamflow_with_coords as sws
    inner join weather as w
        on
            sws.site_id = w.site_id
            and sws.observation_hour::timestamp = w.observed_at::timestamp
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
    -- Weather forcing
    precipitation_mm,
    temperature_c,
    wind_speed_ms,
    specific_humidity_kgkg,
    surface_pressure_pa,
    shortwave_radiation_wm2,
    longwave_radiation_wm2,
    potential_evaporation_mm,
    cape_jkg,
    convective_precip_fraction
from final
