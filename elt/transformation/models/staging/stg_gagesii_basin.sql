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
    drain_sqkm
from {{ ref('attributes_gageii_BasinID') }}