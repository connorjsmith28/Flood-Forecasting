-- Daily streamflow joined with aggregated daily NLDAS-3 weather
-- For sites with only daily data (no 15-min IV data available)
-- Uses site_id join since NLDAS-3 data is already watershed-averaged

with streamflow as (
    select
        site_id,
        observed_date,
        streamflow_cfs_mean,
        gage_height_ft_mean
    from {{ ref('stg_streamflow_daily') }}
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

-- Aggregate hourly NLDAS-3 weather to daily
weather_daily as (
    select
        site_id,
        date_trunc('day', observed_at) as observed_date,
        sum(precipitation_mm) as precipitation_mm,
        avg(temperature_c) as temperature_c_mean,
        max(temperature_c) as temperature_c_max,
        min(temperature_c) as temperature_c_min,
        avg(wind_speed_ms) as wind_speed_ms_mean,
        avg(specific_humidity_kgkg) as specific_humidity_kgkg_mean,
        avg(surface_pressure_pa) as surface_pressure_pa_mean,
        avg(shortwave_radiation_wm2) as shortwave_radiation_wm2_mean,
        avg(longwave_radiation_wm2) as longwave_radiation_wm2_mean,
        sum(potential_evaporation_mm) as potential_evaporation_mm,
        avg(cape_jkg) as cape_jkg_mean,
        avg(convective_precip_fraction) as convective_precip_fraction_mean
    from weather
    group by site_id, date_trunc('day', observed_at)
),

-- Join with dim_sites to get coordinates
streamflow_with_coords as (
    select
        sf.site_id,
        sf.observed_date,
        s.latitude,
        s.longitude,
        sf.streamflow_cfs_mean,
        sf.gage_height_ft_mean
    from streamflow as sf
    inner join sites as s on sf.site_id = s.site_id
),

final as (
    select
        sws.site_id,
        sws.observed_date,
        sws.latitude,
        sws.longitude,
        sws.streamflow_cfs_mean,
        sws.gage_height_ft_mean,
        -- NLDAS-3 forcing variables (aggregated to daily)
        w.precipitation_mm,
        w.temperature_c_mean,
        w.temperature_c_max,
        w.temperature_c_min,
        w.wind_speed_ms_mean,
        w.specific_humidity_kgkg_mean,
        w.surface_pressure_pa_mean,
        w.shortwave_radiation_wm2_mean,
        w.longwave_radiation_wm2_mean,
        w.potential_evaporation_mm,
        w.cape_jkg_mean,
        w.convective_precip_fraction_mean
    from streamflow_with_coords as sws
    left join weather_daily as w
        on
            sws.site_id = w.site_id
            and sws.observed_date = w.observed_date
)

select
    site_id,
    observed_date,
    latitude,
    longitude,
    streamflow_cfs_mean,
    gage_height_ft_mean,
    -- Weather forcing
    precipitation_mm,
    temperature_c_mean,
    temperature_c_max,
    temperature_c_min,
    wind_speed_ms_mean,
    specific_humidity_kgkg_mean,
    surface_pressure_pa_mean,
    shortwave_radiation_wm2_mean,
    longwave_radiation_wm2_mean,
    potential_evaporation_mm,
    cape_jkg_mean,
    convective_precip_fraction_mean
from final
