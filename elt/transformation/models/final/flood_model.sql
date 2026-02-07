{{ config(materialized='table') }}

select
    -- Streamflow data (USGS streamflow extractor)
    streamflow.site_id,
    streamflow.observation_hour,
    streamflow.latitude,
    streamflow.longitude,
    streamflow.streamflow_cfs_mean,
    streamflow.streamflow_cfs_max,
    streamflow.streamflow_cfs_min,
    streamflow.gage_height_ft_mean,
    streamflow.gage_height_ft_max,
    streamflow.gage_height_ft_min,
    streamflow.observation_count,

    -- NLDAS-3 forcing data (watershed-averaged)
    streamflow.precipitation_mm,
    streamflow.temperature_c,
    streamflow.wind_speed_ms,
    streamflow.specific_humidity_kgkg,
    streamflow.surface_pressure_pa,
    streamflow.shortwave_radiation_wm2,
    streamflow.longwave_radiation_wm2,
    streamflow.potential_evaporation_mm,
    streamflow.cape_jkg,
    streamflow.convective_precip_fraction,

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
from {{ ref('fct_streamflow_hourly') }} as streamflow
inner join {{ ref('dim_catchment_attributes') }} as attributes
    on streamflow.site_id = attributes.site_id
