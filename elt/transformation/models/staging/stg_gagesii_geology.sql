-- GAGES-II geology attributes
select
    STAID as site_id,
    GEOL_REEDBUSH_DOM as geology_class_reedbush,
    GEOL_REEDBUSH_DOM_PCT as geology_pct_reedbush,
    GEOL_HUNT_DOM_CODE as geology_code_hunt,
    GEOL_HUNT_DOM_PCT as geology_pct_hunt,
    GEOL_HUNT_DOM_DESC as geology_desc_hunt
from {{ ref('attributes_gageii_Geology') }}
