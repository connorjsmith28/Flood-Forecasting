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
    hcdn_2009 = 'yes' as is_reference_hcdn2009,
    hbn36 = 'yes' as is_reference_hbn36
from {{ ref('attributes_gageii_BasinID') }}