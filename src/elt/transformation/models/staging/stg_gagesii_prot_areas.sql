-- Core GAGES-II Protected Areas attributes
select
    staid as site_id,
    PADCAT1_PCT_BASIN as padcat1_pct_basin,
    PADCAT2_PCT_BASIN as padcat2_pct_basin,
    PADCAT3_PCT_BASIN as padcat3_pct_basin
from {{ ref('attributes_gageii_Prot_Areas') }}

