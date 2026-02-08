-- Core GAGES-II basin identification and location
select
    staid as site_id,
    drain_sqkm as drainage_area_sq_km,
    hcdn_2009 as is_reference_hcdn2009,
    hbn36 as is_reference_hbn36
from {{ ref('attributes_gageii_BasinID') }}