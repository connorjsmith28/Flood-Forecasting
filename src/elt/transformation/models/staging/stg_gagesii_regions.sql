-- Core GAGES-II Regions attributes
select
    staid as site_id,
    ECO3_SITE as eco3_site,
    HLR100M_SITE as hlr100m_site,
    HUC8_SITE as huc8_site,
    NUTR_ECO_SITE as nutr_eco_site,
    USDA_LRR_SITE as usda_lrr_site,
    ECO2_BAS_DOM as eco2_bas_dom,
    ECO3_BAS_DOM as eco3_bas_dom,
    ECO3_BAS_PCT as eco3_bas_pct,
    HLR_BAS_DOM_100M as hlr_bas_dom_100m,
    HLR_BAS_PCT_100M as hlr_bas_pct_100m,
    NUTR_BAS_DOM as nutr_bas_dom,
    NUTR_BAS_PCT as nutr_bas_pct,
    PNV_BAS_DOM as pnv_bas_dom,
    PNV_BAS_PCT as pnv_bas_pct
from {{ ref('attributes_gageii_Regions') }}

