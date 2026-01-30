-- Daily streamflow joined with aggregated daily weather
-- For sites with only daily data (no 15-min IV data available)

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
        latitude,
        longitude,
        observed_at,
        precipitation_mm,
        temperature_c,
        wind_speed_ms,
        humidity_pct
    from {{ ref('stg_weather') }}
),

-- Aggregate hourly weather to daily
weather_daily as (
    select
        latitude,
        longitude,
        date_trunc('day', observed_at) as observed_date,
        sum(precipitation_mm) as precipitation_mm,
        avg(temperature_c) as temperature_c_mean,
        max(temperature_c) as temperature_c_max,
        min(temperature_c) as temperature_c_min,
        avg(wind_speed_ms) as wind_speed_ms_mean,
        avg(humidity_pct) as humidity_pct_mean
    from weather
    group by latitude, longitude, date_trunc('day', observed_at)
),

-- Join with dim_sites to get coordinates for weather matching
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
        w.precipitation_mm,
        w.temperature_c_mean,
        w.temperature_c_max,
        w.temperature_c_min,
        w.wind_speed_ms_mean,
        w.humidity_pct_mean
    from streamflow_with_coords as sws
    left join weather_daily as w
        on
            sws.longitude = w.longitude
            and sws.latitude = w.latitude
            and sws.observed_date = w.observed_date
)

select
    site_id,
    observed_date,
    latitude,
    longitude,
    streamflow_cfs_mean,
    gage_height_ft_mean,
    precipitation_mm,
    temperature_c_mean,
    temperature_c_max,
    temperature_c_min,
    wind_speed_ms_mean,
    humidity_pct_mean
from final
