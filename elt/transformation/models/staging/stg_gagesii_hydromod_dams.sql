-- Core GAGES-II hydrologic modification attributes related to dams
select
    staid as site_id,
    NDAMS_2009 as num_dams,
    DDENS_2009 as dam_density,
    STOR_NID_2009 as storage_nid,
    STOR_NOR_2009 as storage_nor,
    MAJ_NDAMS_2009 as num_major_dams,
    MAJ_DDENS_2009 as major_dam_density,
    RAW_DIS_NEAREST_DAM as raw_dis_nearest_dam,
    RAW_AVG_DIS_ALLDAMS as raw_avg_dis_all_dams,
    RAW_DIS_NEAREST_MAJ_DAM as raw_dis_nearest_maj_dam,
    RAW_AVG_DIS_ALL_MAJ_DAMS as raw_avg_dis_all_maj_dams
from {{ ref('attributes_gageii_HydroMod_Dams') }}
