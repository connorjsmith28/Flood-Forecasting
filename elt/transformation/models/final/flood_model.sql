select
    -- Streamflow data (USGS streamflow extractor)
    streamflow.site_id,
    streamflow.observation_hour,
    streamflow.latitude,
    streamflow.longitude,
    streamflow.streamflow_cfs_mean,

    -- Target: 1-hour ahead streamflow (what we're predicting)
    lead(streamflow.streamflow_cfs_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observation_hour
    ) as streamflow_cfs_target_1h,
    streamflow.streamflow_cfs_max,
    streamflow.streamflow_cfs_min,

    -- Gage height (water level)
    streamflow.gage_height_ft_mean,
    streamflow.gage_height_ft_max,
    streamflow.gage_height_ft_min,

    -- Target: 1-hour ahead gage height
    lead(streamflow.gage_height_ft_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observation_hour
    ) as gage_height_ft_target_1h,

    streamflow.observation_count,

    -- Weather data (Mateo weather extractor)
    weather.precipitation_mm,
    weather.temperature_c,
    weather.wind_speed_ms,
    weather.humidity_pct,

    -- Site attributes (CAMELSH static attributes via dbt seeds)
    attributes.station_name,
    attributes.huc_code,
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
    attributes.hydroatlas_urban_pct  
from {{ ref('fct_streamflow_hourly') }} streamflow  
inner join {{ ref('dim_catchment_attributes') }} attributes 
    on streamflow.site_id = attributes.site_id
inner join {{ ref('stg_weather') }} weather
    on attributes.longitude = weather.longitude
    and attributes.latitude = weather.latitude
    and streamflow.observation_hour = weather.observed_at