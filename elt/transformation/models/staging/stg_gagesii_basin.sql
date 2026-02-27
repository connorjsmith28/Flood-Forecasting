-- Core GAGES-II basin identification and location
select
    staid as site_id,
    staname,
    HUC02,
    lat_gage,
    lng_gage,
    "state" as state_gage,
    fips_site,
    countyname_site,
    drain_sqkm,
    COALESCE(hcdn_2009 = 'yes', false) as is_reference_hcdn2009,
    COALESCE(hbn36 = 'yes', false) as is_reference_hbn36
from {{ ref('attributes_gageii_BasinID') }}