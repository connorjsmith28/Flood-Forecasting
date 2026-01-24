-- Catchment attributes for Missouri Basin sites
-- Joins CAMELS-H static attributes (GAGES-II, HydroATLAS, NLDAS climate)

with sites as (
    select * from {{ ref('stg_site_metadata') }}
),

basin as (
    select * from {{ ref('stg_gagesii_basin') }}
),

topo as (
    select * from {{ ref('stg_gagesii_topo') }}
),

soils as (
    select * from {{ ref('stg_gagesii_soils') }}
),

geology as (
    select * from {{ ref('stg_gagesii_geology') }}
),

climate as (
    select * from {{ ref('stg_nldas_climate') }}
),

hydroatlas as (
    select * from {{ ref('stg_hydroatlas') }}
)

select
    -- Site identification
    s.site_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.huc_code,

    -- Basin characteristics
    b.drainage_area_sq_km,
    b.is_reference_hcdn2009,

    -- Topography
    t.elev_mean_m,
    t.elev_max_m,
    t.elev_min_m,
    t.slope_pct,
    t.aspect_northness,
    t.aspect_eastness,

    -- Soils
    so.soil_water_capacity_avg,
    so.soil_permeability_avg,
    so.clay_pct_avg,
    so.silt_pct_avg,
    so.sand_pct_avg,
    so.rock_depth_avg,

    -- Geology
    g.geology_class_reedbush,
    g.geology_desc_hunt,

    -- Climate indices (NLDAS-derived)
    c.p_mean,
    c.pet_mean,
    c.aridity_index,
    c.p_seasonality,
    c.frac_snow,
    c.high_prec_freq,
    c.low_prec_freq,

    -- HydroATLAS key attributes
    h.ele_mt_sav as hydroatlas_elev_m,
    h.slp_dg_uav as hydroatlas_slope_deg,
    h.tmp_dc_syr as hydroatlas_temp_mean_c,
    h.pre_mm_syr as hydroatlas_precip_mm_yr,
    h.pet_mm_syr as hydroatlas_pet_mm_yr,
    h.ari_ix_uav as hydroatlas_aridity,
    h.cly_pc_uav as hydroatlas_clay_pct,
    h.snd_pc_uav as hydroatlas_sand_pct,
    h.for_pc_use as hydroatlas_forest_pct,
    h.crp_pc_use as hydroatlas_crop_pct,
    h.urb_pc_use as hydroatlas_urban_pct

from sites s
left join basin b on s.site_id = b.site_id
left join topo t on s.site_id = t.site_id
left join soils so on s.site_id = so.site_id
left join geology g on s.site_id = g.site_id
left join climate c on s.site_id = c.site_id
left join hydroatlas h on s.site_id = h.site_id
