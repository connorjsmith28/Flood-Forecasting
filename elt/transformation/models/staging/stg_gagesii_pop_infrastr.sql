-- Core GAGES-II Population and infrastructure attributes
select
    staid as site_id,
    PDEN_2000_BLOCK as pden_2000_block,
    PDEN_DAY_LANDSCAN_2007 as pden_day_landscan_2007,
    PDEN_NIGHT_LANDSCAN_2007 as pden_night_landscan_2007,
    ROADS_KM_SQ_KM as roads_km_sq_km,
    RD_STR_INTERS as rd_str_inters,
    IMPNLCD06 as impnlcd06,
    NLCD01_06_DEV as nlcd01_06_dev
from {{ ref('attributes_gageii_Pop_Infrastr') }}

