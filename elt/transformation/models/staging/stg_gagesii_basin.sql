-- Core GAGES-II basin identification and location
select
    staid as site_id,
    staname as station_name,
    drain_sqkm as drainage_area_sq_km,
    huc02,
    lat_gage as latitude,
    lng_gage as longitude,
    state,
    hcdn_2009 as is_reference_hcdn2009,
    hbn36 as is_reference_hbn36
from {{ ref('attributes_gageii_BasinID') }}
