-- Core GAGES-II landscape pattern attributes
select
    staid as site_id,
    FRAGUN_BASIN as fragun_basin,
    HIRES_LENTIC_NUM as hires_lentic_num,
    HIRES_LENTIC_DENS as hires_lentic_dens,
    HIRES_LENTIC_MEANSIZ as hires_lentic_meansiz
from {{ ref('attributes_gageii_Landscape_Pat') }}