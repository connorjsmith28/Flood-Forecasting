-- ML-ready training data at daily resolution
-- For sites that only have daily data (no 15-min IV data)
-- Provides 1-day ahead prediction targets

select
    -- Streamflow data
    streamflow.site_id,
    streamflow.observed_date,
    streamflow.latitude,
    streamflow.longitude,
    streamflow.streamflow_cfs_mean,
    streamflow.gage_height_ft_mean,

    -- Weather data (aggregated to daily)
    streamflow.precipitation_mm,
    streamflow.temperature_c_mean,
    streamflow.temperature_c_max,
    streamflow.temperature_c_min,
    streamflow.wind_speed_ms_mean,
    streamflow.humidity_pct_mean,

    -- Site attributes
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
    attributes.hydroatlas_urban_pct,

    -- Target: 1-day ahead streamflow (what we're predicting)
    lead(streamflow.streamflow_cfs_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observed_date
    ) as streamflow_cfs_target_1d,
    lead(streamflow.gage_height_ft_mean, 1) over (
        partition by streamflow.site_id
        order by streamflow.observed_date
    ) as gage_height_ft_target_1d
from {{ ref('fct_streamflow_daily') }} as streamflow
inner join {{ ref('dim_catchment_attributes') }} as attributes
    on streamflow.site_id = attributes.site_id
