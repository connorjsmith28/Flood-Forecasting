select
    -- Streamflow data (USGS streamflow extractor)
    streamflow.site_id,
    streamflow.observation_hour,
    streamflow.latitude,
    streamflow.longitude,
    streamflow.streamflow_cfs_mean,

    -- Target: 1-hour ahead streamflow (what we're predicting)
    streamflow.streamflow_cfs_max,
    streamflow.streamflow_cfs_min,
    streamflow.gage_height_ft_mean,

    -- Gage height (water level)
    streamflow.gage_height_ft_max,
    streamflow.gage_height_ft_min,
    streamflow.observation_count,

    -- Target: 1-hour ahead gage height
    weather.precipitation_mm,

    weather.temperature_c,

    -- Weather data (Mateo weather extractor)
    weather.wind_speed_ms,
    weather.humidity_pct,
    attributes.station_name,
    attributes.huc_code,

    -- Site attributes (CAMELSH static attributes via dbt seeds)
    attributes.drainage_area_sq_km,
    attributes.is_reference_hcdn2009,
    attributes.elev_mean_m,
    attributes.elev_max_m,
    attributes.elev_min_m,
    attributes.slope_pct,
    attributes.aspect_northness,
    attributes.aspect_eastness,
    attributes.geology_class_reedbush,
    attributes.geology_desc_hunt,
    attributes.p_mean,
    attributes.pet_mean,
    attributes.aridity_index,
    attributes.p_seasonality,
    attributes.frac_snow,
    attributes.high_prec_freq,
    attributes.low_prec_freq,
    attributes.hydroatlas_elev_m,
    attributes.hydroatlas_slope_deg,
    attributes.hydroatlas_temp_mean_c,
    attributes.hydroatlas_precip_mm_yr,
    attributes.hydroatlas_pet_mm_yr,
    attributes.hydroatlas_aridity,
    attributes.hydroatlas_clay_pct,
    attributes.hydroatlas_sand_pct,
    attributes.hydroatlas_forest_pct,
    attributes.hydroatlas_crop_pct,
    attributes.hydroatlas_urban_pct,
    lead(streamflow.streamflow_cfs_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observation_hour
    ) as streamflow_cfs_target_1h,
    lead(streamflow.gage_height_ft_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observation_hour
    ) as gage_height_ft_target_1h
from {{ ref('fct_streamflow_hourly') }} as streamflow
inner join {{ ref('dim_catchment_attributes') }} as attributes
    on streamflow.site_id = attributes.site_id
inner join {{ ref('stg_weather') }} as weather
    on
        attributes.longitude = weather.longitude
        and attributes.latitude = weather.latitude
        and streamflow.observation_hour = weather.observed_at
