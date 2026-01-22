-- Fact table joining streamflow with site metadata and weather
-- Aggregated to hourly granularity for ML modeling

with streamflow as (
    select * from {{ ref('stg_streamflow') }}
),

sites as (
    select * from {{ ref('stg_sites') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

-- Aggregate streamflow to hourly
streamflow_hourly as (
    select
        site_id,
        date_trunc('hour', observed_at) as observation_hour,
        avg(streamflow_cfs) as streamflow_cfs_mean,
        max(streamflow_cfs) as streamflow_cfs_max,
        min(streamflow_cfs) as streamflow_cfs_min,
        avg(streamflow_cms) as streamflow_cms_mean,
        count(*) as observation_count,
        sum(case when is_estimated then 1 else 0 end) as estimated_count
    from streamflow
    group by site_id, date_trunc('hour', observed_at)
),

-- Join streamflow with site info
streamflow_with_sites as (
    select
        sf.site_id,
        sf.observation_hour,
        s.station_name,
        s.latitude,
        s.longitude,
        s.drainage_area_sq_mi,
        s.huc_code,
        s.huc_region,
        sf.streamflow_cfs_mean,
        sf.streamflow_cfs_max,
        sf.streamflow_cfs_min,
        sf.streamflow_cms_mean,
        sf.observation_count,
        sf.estimated_count
    from streamflow_hourly sf
    inner join sites s on sf.site_id = s.site_id
),

-- Join with weather using nearest grid point (by site coordinates)
final as (
    select
        sws.site_id,
        sws.observation_hour,
        sws.station_name,
        sws.latitude,
        sws.longitude,
        sws.drainage_area_sq_mi,
        sws.huc_code,
        sws.huc_region,
        sws.streamflow_cfs_mean,
        sws.streamflow_cfs_max,
        sws.streamflow_cfs_min,
        sws.streamflow_cms_mean,
        sws.observation_count,
        sws.estimated_count,
        w.precipitation_mm,
        w.temperature_c,
        w.wind_speed_ms,
        w.humidity_pct
    from streamflow_with_sites sws
    left join weather w
        on round(sws.longitude, 2) = round(w.longitude, 2)
        and round(sws.latitude, 2) = round(w.latitude, 2)
        and sws.observation_hour = w.observed_at
)

select * from final
