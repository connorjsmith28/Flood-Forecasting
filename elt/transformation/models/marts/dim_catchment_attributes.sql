-- Catchment attributes for Missouri Basin sites
-- Joins CAMELS-H static attributes (GAGES-II, HydroATLAS, NLDAS climate)

with sites as (
    select * from {{ ref('stg_site_metadata') }}
),

bas_classif as (
    select * from {{ ref('stg_gagesii_bas_classif') }}
),

bas_morph as (
    select * from {{ ref('stg_gagesii_bas_morph') }}
),

basin as (
    select * from {{ ref('stg_gagesii_basin') }}
),

bound_qa as (
    select * from {{ ref('stg_gagesii_bound_qa') }}
),

geology as (
    select * from {{ ref('stg_gagesii_geology') }}
),

hydro as (
    select * from {{ ref('stg_gagesii_hydro') }}
),

hydromod_dams as (
    select * from {{ ref('stg_gagesii_hydromod_dams') }}
),

hydromod_other as (
    select * from {{ ref('stg_gagesii_hydromod_other') }}
),

landscape_pat as (
    select * from {{ ref('stg_gagesii_landscape_pat') }}
),

lc_crops as (
    select * from {{ ref('stg_gagesii_lc_crops') }}
),

lc06_basin as (
    select * from {{ ref('stg_gagesii_lc06_basin') }}
),

lc06_mains100 as (
    select * from {{ ref('stg_gagesii_lc06_mains100') }}
),

lc06_mains800 as (
    select * from {{ ref('stg_gagesii_lc06_mains800') }}
),

lc06_rip100 as (
    select * from {{ ref('stg_gagesii_lc06_rip100') }}
),

lc06_rip800 as (
    select * from {{ ref('stg_gagesii_lc06_rip800') }}
),

nutrient_app as (
    select * from {{ ref('stg_gagesii_nutrient_app') }}
),

pest_app as (
    select * from {{ ref('stg_gagesii_pest_app') }}
),

pop_infrastr as (
    select * from {{ ref('stg_gagesii_pop_infrastr') }}
),

prot_areas as (
    select * from {{ ref('stg_gagesii_prot_areas') }}
),

regions as (
    select * from {{ ref('stg_gagesii_regions') }}
),

soils as (
    select * from {{ ref('stg_gagesii_soils') }}
),

topo as (
    select * from {{ ref('stg_gagesii_topo') }}
),

hydroatlas as (
    select * from {{ ref('stg_hydroatlas') }}
),

nldas_climate as (
    select * from {{ ref('stg_nldas_climate') }}
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
    b.is_reference_hbn36,

    -- Basin classification
    bc.agg_ecoregion,
    bc.hydro_disturbance_index,

    -- Basin morphology
    bm.basin_compactness,

    -- Boundary QA
    bqa.basin_boundary_confidence,
    bqa.nwis_drainage_area,
    bqa.huc10_check,

    -- Geology
    g.geology_class_reedbush,
    g.geology_pct_reedbush,
    g.geology_site_reedbush,
    g.geology_code_hunt,
    g.geology_pct_hunt,
    g.geology_desc_hunt,


    -- Climate indices (NLDAS-derived)
    c.p_mean,
    c.pet_mean,
    c.aridity_index,
    c.p_seasonality,
    c.frac_snow,
    c.high_prec_freq,
    c.high_prec_dur,
    c.low_prec_freq,
    c.low_prec_dur,

    -- Hydrology
    h.streams_km_per_sq_km,
    h.strahler_max,
    h.mainstem_sinuosity,
    h.reach_code,
    h.artificial_path_pct,
    h.artificial_path_mainstem_pct,
    h.hires_lentic_pct,
    h.bfi_ave,
    h.perdun,
    h.perhor,
    h.topwet,
    h.contact,
    h.runave7100,
    h.wb5100_ann_mm,
    h.pct_1st_order,
    h.pct_2nd_order,
    h.pct_3rd_order,
    h.pct_4th_order,
    h.pct_5th_order,
    h.pct_6th_order_or_more,
    h.pct_no_order,

    -- Hydrology Modification attributes related to dams
    hd.num_dams,
    hd.dam_density,
    hd.storage_nid,
    hd.storage_nor,
    hd.num_major_dams,
    hd.major_dam_density,
    hd.raw_dis_nearest_dam,
    hd.raw_avg_dis_all_dams,
    hd.raw_dis_nearest_maj_dam,
    hd.raw_avg_dis_all_maj_dams,

    -- Hydrology Modification attributes related to other factors
    ho.canals_pct,
    ho.raw_dis_nearest_canal,
    ho.raw_avg_dis_allcanals,
    ho.canals_mainstem_pct,
    ho.npdes_major_density,
    ho.raw_dis_nearest_maj_npdes,
    ho.raw_avg_dis_all_maj_npdes,
    ho.freshwater_withdrawal,
    ho.mining_pct,
    ho.pct_irrig_ag,
    ho.power_num_pts,
    ho.power_sum_mw,

    -- HydroATLAS key attributes
    ha.discharge_avg,
    ha.discharge_min,
    ha.discharge_max,
    ha.runoff,
    ha.inu_pc_smn,
    ha.inu_pc_smx,
    ha.inu_pc_slt,
    ha.lka_pc_use,
    ha.lkv_mc_usu,
    ha.rev_mc_usu,
    ha.dor_pc_pva,
    ha.ria_ha_usu,
    ha.riv_tc_usu,
    ha.gwt_cm_sav,
    ha.ele_mt_sav,
    ha.ele_mt_smn,
    ha.ele_mt_smx,
    ha.slp_dg_uav,
    ha.sgr_dk_sav,
    ha.clz_cl_smj,
    ha.cls_cl_smj,
    ha.tmp_dc_syr,
    ha.tmp_dc_smn,
    ha.tmp_dc_smx,
    ha.tmp_dc_s01,
    ha.tmp_dc_s02,
    ha.tmp_dc_s03,
    ha.tmp_dc_s04,
    ha.tmp_dc_s05,
    ha.tmp_dc_s06,
    ha.tmp_dc_s07,
    ha.tmp_dc_s08,
    ha.tmp_dc_s09,
    ha.tmp_dc_s10,
    ha.tmp_dc_s11,
    ha.tmp_dc_s12,
    ha.pet_mm_syr,
    ha.pet_mm_s01,
    ha.pet_mm_s02,
    ha.pet_mm_s03,
    ha.pet_mm_s04,
    ha.pet_mm_s05,
    ha.pet_mm_s06,
    ha.pet_mm_s07,
    ha.pet_mm_s08,
    ha.pet_mm_s09,
    ha.pet_mm_s10,
    ha.pet_mm_s11,
    ha.pet_mm_s12,
    ha.aet_mm_syr,
    ha.aet_mm_s01,
    ha.aet_mm_s02,
    ha.aet_mm_s03,
    ha.aet_mm_s04,
    ha.aet_mm_s05,
    ha.aet_mm_s06,
    ha.aet_mm_s07,
    ha.aet_mm_s08,
    ha.aet_mm_s09,
    ha.aet_mm_s10,
    ha.aet_mm_s11,
    ha.aet_mm_s12,
    ha.ari_ix_uav,
    ha.cmi_ix_syr,
    ha.cmi_ix_s01,
    ha.cmi_ix_s02,
    ha.cmi_ix_s03,
    ha.cmi_ix_s04,
    ha.cmi_ix_s05,
    ha.cmi_ix_s06,
    ha.cmi_ix_s07,
    ha.cmi_ix_s08,
    ha.cmi_ix_s09,
    ha.cmi_ix_s10,
    ha.cmi_ix_s11,
    ha.cmi_ix_s12,
    ha.snw_pc_syr,
    ha.snw_pc_smx,
    ha.snw_pc_s01,
    ha.snw_pc_s02,
    ha.snw_pc_s03,
    ha.snw_pc_s04,
    ha.snw_pc_s05,
    ha.snw_pc_s06,
    ha.snw_pc_s07,
    ha.snw_pc_s08,
    ha.snw_pc_s09,
    ha.snw_pc_s10,
    ha.snw_pc_s11,
    ha.snw_pc_s12,
    ha.glc_cl_smj,
    ha.glc_pc_u01,
    ha.glc_pc_u02,
    ha.glc_pc_u03,
    ha.glc_pc_u04,
    ha.glc_pc_u05,
    ha.glc_pc_u06,
    ha.glc_pc_u07,
    ha.glc_pc_u08,
    ha.glc_pc_u09,
    ha.glc_pc_u10,
    ha.glc_pc_u11,
    ha.glc_pc_u12,
    ha.glc_pc_u13,
    ha.glc_pc_u14,
    ha.glc_pc_u15,
    ha.glc_pc_u16,
    ha.glc_pc_u17,
    ha.glc_pc_u18,
    ha.glc_pc_u19,
    ha.glc_pc_u20,
    ha.glc_pc_u21,
    ha.glc_pc_u22,
    ha.pnv_cl_smj,
    ha.pnv_pc_u01,
    ha.pnv_pc_u02,
    ha.pnv_pc_u03,
    ha.pnv_pc_u04,
    ha.pnv_pc_u05,
    ha.pnv_pc_u06,
    ha.pnv_pc_u07,
    ha.pnv_pc_u08,
    ha.pnv_pc_u09,
    ha.pnv_pc_u10,
    ha.pnv_pc_u11,
    ha.pnv_pc_u12,
    ha.pnv_pc_u13,
    ha.pnv_pc_u14,
    ha.pnv_pc_u15,
    ha.wet_cl_smj,
    ha.wet_pc_ug1,
    ha.wet_pc_ug2,
    ha.wet_pc_u01,
    ha.wet_pc_u02,
    ha.wet_pc_u03,
    ha.wet_pc_u04,
    ha.wet_pc_u05,
    ha.wet_pc_u06,
    ha.wet_pc_u07,
    ha.wet_pc_u08,
    ha.wet_pc_u09,
    ha.for_pc_use,
    ha.crp_pc_use,
    ha.pst_pc_use,
    ha.ire_pc_use,
    ha.gla_pc_use,
    ha.prm_pc_use,
    ha.pac_pc_use,
    ha.tbi_cl_smj,
    ha.tec_cl_smj,
    ha.fmh_cl_smj,
    ha.fec_cl_smj,
    ha.cly_pc_uav,
    ha.slt_pc_uav,
    ha.snd_pc_uav,
    ha.soc_th_uav,
    ha.swc_pc_syr,
    ha.swc_pc_s01,
    ha.swc_pc_s02,
    ha.swc_pc_s03,
    ha.swc_pc_s04,
    ha.swc_pc_s05,
    ha.swc_pc_s06,
    ha.swc_pc_s07,
    ha.swc_pc_s08,
    ha.swc_pc_s09,
    ha.swc_pc_s10,
    ha.swc_pc_s11,
    ha.swc_pc_s12,
    ha.lit_cl_smj,
    ha.kar_pc_use,
    ha.ero_kh_uav,
    ha.pop_ct_usu,
    ha.ppd_pk_uav,
    ha.urb_pc_use,
    ha.nli_ix_uav,
    ha.rdd_mk_uav,
    ha.hft_ix_u93,
    ha.hft_ix_u09,
    ha.gad_id_smj,
    ha.gdp_ud_usu,
    ha.hdi_ix_sav,

    -- Landscape pattern attributes
    lp.fragun_basin,
    lp.hires_lentic_num,
    lp.hires_lentic_dens,
    lp.hires_lentic_meansiz,

    -- Landscape crop attributes
    lc.cdl_corn,
    lc.cdl_cotton,
    lc.cdl_rice,
    lc.cdl_sorghum,
    lc.cdl_soybeans,
    lc.cdl_sunflowers,
    lc.cdl_peanuts,
    lc.cdl_barley,
    lc.cdl_durum_wheat,
    lc.cdl_spring_wheat,
    lc.cdl_winter_wheat,
    lc.cdl_wwht_soy_dbl_crop,
    lc.cdl_oats,
    lc.cdl_alfalfa,
    lc.cdl_other_hays,
    lc.cdl_dry_beans,
    lc.cdl_potatoes,
    lc.cdl_fallow_idle,
    lc.cdl_pasture_grass,
    lc.cdl_oranges,
    lc.cdl_other_crops,
    lc.cdl_all_other_land,

    -- Landscape Basin
    lb.dev_nlcd06,
    lb.forest_nlcd06,
    lb.plant_nlcd06,
    lb.water_nlcd06,
    lb.snow_ice_nlcd06,
    lb.dev_open_nlcd06,
    lb.dev_low_nlcd06,
    lb.dev_med_nlcd06,
    lb.dev_high_nlcd06,
    lb.barren_nlcd06,
    lb.deciduous_nlcd06,
    lb.evergreen_nlcd06,
    lb.mixed_forest_nlcd06,
    lb.shrub_nlcd06,
    lb.grass_nlcd06,
    lb.pasture_nlcd06,
    lb.crops_nlcd06,
    lb.woody_wet_nlcd06,
    lb.emergent_wet_nlcd06,

    -- Landscape Mainstem attributes with 100m buffer, 2006 era
    lm1.mains100_dev,
    lm1.mains100_forest,
    lm1.mains100_plant,
    lm1.mains100_11,
    lm1.mains100_12,
    lm1.mains100_21,
    lm1.mains100_22,
    lm1.mains100_23,
    lm1.mains100_24,
    lm1.mains100_31,
    lm1.mains100_41,
    lm1.mains100_42,
    lm1.mains100_43,
    lm1.mains100_52,
    lm1.mains100_71,
    lm1.mains100_81,
    lm1.mains100_82,
    lm1.mains100_90,
    lm1.mains100_95,

    -- Landscape Mainstem attributes with 800m buffer, 2006 era
    lm8.mains800_dev,
    lm8.mains800_forest,
    lm8.mains800_plant,
    lm8.mains800_11,
    lm8.mains800_12,
    lm8.mains800_21,
    lm8.mains800_22,
    lm8.mains800_23,
    lm8.mains800_24,
    lm8.mains800_31,
    lm8.mains800_41,
    lm8.mains800_42,
    lm8.mains800_43,
    lm8.mains800_52,
    lm8.mains800_71,
    lm8.mains800_81,
    lm8.mains800_82,
    lm8.mains800_90,
    lm8.mains800_95,

    -- Landscape Riparian attributes with 100m buffer, 2006 era
    lr1.rip100_dev,
    lr1.rip100_forest,
    lr1.rip100_plant,
    lr1.rip100_11,
    lr1.rip100_12,
    lr1.rip100_21,
    lr1.rip100_22,
    lr1.rip100_23,
    lr1.rip100_24,
    lr1.rip100_31,
    lr1.rip100_41,
    lr1.rip100_42,
    lr1.rip100_43,
    lr1.rip100_52,
    lr1.rip100_71,
    lr1.rip100_81,
    lr1.rip100_82,
    lr1.rip100_90,
    lr1.rip100_95,

    -- Landscape Riparian attributes with 800m buffer, 2006 era
    lr8.rip800_dev,
    lr8.rip800_forest,
    lr8.rip800_plant,
    lr8.rip800_11,
    lr8.rip800_12,
    lr8.rip800_21,
    lr8.rip800_22,
    lr8.rip800_23,
    lr8.rip800_24,
    lr8.rip800_31,
    lr8.rip800_41,
    lr8.rip800_42,
    lr8.rip800_43,
    lr8.rip800_52,
    lr8.rip800_71,
    lr8.rip800_81,
    lr8.rip800_82,
    lr8.rip800_90,
    lr8.rip800_95,

    -- Nutrient Application
    n.nitr_app_kg_sqkm,
    n.phos_app_kg_sqkm,

    -- Pesticides Application
    pa.pestapp_kg_sqkm,

    -- Population and infrastructure attributes
    pi.pden_2000_block,
    pi.pden_day_landscan_2007,
    pi.pden_night_landscan_2007,
    pi.roads_km_sq_km,
    pi.rd_str_inters,
    pi.impnlcd06,
    pi.nlcd01_06_dev,

    -- Protected Areas
    par.padcat1_pct_basin,
    par.padcat2_pct_basin,
    par.padcat3_pct_basin,

    -- Regions
    r.eco3_site,
    r.hlr100m_site,
    r.huc8_site,
    r.nutr_eco_site,
    r.usda_lrr_site,
    r.eco2_bas_dom,
    r.eco3_bas_dom,
    r.eco3_bas_pct,
    r.hlr_bas_dom_100m,
    r.hlr_bas_pct_100m,
    r.nutr_bas_dom,
    r.nutr_bas_pct,
    r.pnv_bas_dom,
    r.pnv_bas_pct,

    -- Soils
    so.hga,
    so.hgb,
    so.hgad,
    so.hgc,
    so.hgd,
    so.hgac,
    so.hgb_d,
    so.hgcd,
    so.hgbc,
    so.hgvar,
    so.soil_water_capacity_avg,
    so.soil_permeability_avg,
    so.bulk_density_avg,
    so.organic_matter_avg,
    so.water_table_depth_avg,
    so.rock_depth_avg,
    so.no4_ave,
    so.no200_ave,
    so.no10_ave,
    so.clay_pct_avg,
    so.silt_pct_avg,
    so.sand_pct_avg,
    so.erodibility_factor,
    so.runoff_factor,

    -- Topography
    t.elev_mean_m,
    t.elev_max_m,
    t.elev_min_m,
    t.elev_median_m,
    t.elev_std_m,
    t.elev_site_m,
    t.relief_ratio_mean,
    t.relief_ratio_median,
    t.slope_pct,
    t.aspect_deg,
    t.aspect_northness,
    t.aspect_eastness,

from sites as s
left join bas_classif as bc on s.site_id = bc.site_id
left join bas_morph as bm on s.site_id = bm.site_id
left join basin as b on s.site_id = b.site_id
left join bound_qa as bqa on s.site_id = bqa.site_id
left join geology as g on s.site_id = g.site_id
left join hydro as h on s.site_id = h.site_id
left join hydromod_dams as hd on s.site_id = hd.site_id
left join hydromod_other as ho on s.site_id = ho.site_id
left join landscape_pat as lp on s.site_id = lp.site_id
left join lc_crops as lc on s.site_id = lc.site_id
left join lc06_basin as lb on s.site_id = lb.site_id
left join lc06_mains100 as lm1 on s.site_id = lm1.site_id
left join lc06_mains800 as lm8 on s.site_id = lm8.site_id
left join lc06_rip100 as lr1 on s.site_id = lr1.site_id
left join lc06_rip800 as lr8 on s.site_id = lr8.site_id
left join nutrient_app as n on s.site_id = n.site_id
left join pest_app as pa on s.site_id = pa.site_id
left join pop_infrastr as pi on s.site_id = pi.site_id
left join prot_areas as par on s.site_id = par.site_id
left join regions as r on s.site_id = r.site_id
left join soils as so on s.site_id = so.site_id
left join topo as t on s.site_id = t.site_id
left join hydroatlas as ha on s.site_id = ha.site_id
left join nldas_climate as c on s.site_id = c.site_id