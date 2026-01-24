-- Core GAGES-II basin identification and location
select
    STAID as site_id,
    STANAME as station_name,
    DRAIN_SQKM as drainage_area_sq_km,
    HUC02 as huc02,
    LAT_GAGE as latitude,
    LNG_GAGE as longitude,
    STATE as state,
    HCDN_2009 as is_reference_hcdn2009,
    HBN36 as is_reference_hbn36
from {{ ref('attributes_gageii_BasinID') }}
