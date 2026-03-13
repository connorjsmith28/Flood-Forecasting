-- Core GAGES-II hydrologic modification attributes related to other factors
select
    staid as site_id,
    CANALS_PCT as canals_pct,
    RAW_DIS_NEAREST_CANAL as raw_dis_nearest_canal,
    RAW_AVG_DIS_ALLCANALS as raw_avg_dis_allcanals,
    CANALS_MAINSTEM_PCT as canals_mainstem_pct,
    NPDES_MAJ_DENS as npdes_major_density,
    RAW_DIS_NEAREST_MAJ_NPDES as raw_dis_nearest_maj_npdes,
    RAW_AVG_DIS_ALL_MAJ_NPDES as raw_avg_dis_all_maj_npdes,
    FRESHW_WITHDRAWAL as freshwater_withdrawal,
    MINING92_PCT as mining_pct,
    PCT_IRRIG_AG as pct_irrig_ag,
    POWER_NUM_PTS as power_num_pts,
    POWER_SUM_MW as power_sum_mw
from {{ ref('attributes_gageii_HydroMod_Other') }}
