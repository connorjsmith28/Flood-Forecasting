-- GAGES-II geology attributes
select
    staid as site_id,
    geol_reedbush_dom as geology_class_reedbush,
    geol_reedbush_dom_pct as geology_pct_reedbush,
    geol_hunt_dom_code as geology_code_hunt,
    geol_hunt_dom_pct as geology_pct_hunt,
    geol_hunt_dom_desc as geology_desc_hunt
from {{ ref('attributes_gageii_Geology') }}
