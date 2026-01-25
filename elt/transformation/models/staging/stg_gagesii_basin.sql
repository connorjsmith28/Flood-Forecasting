-- Core GAGES-II basin identification and location
select
    STAID as SITE_ID,
    STANAME as STATION_NAME,
    DRAIN_SQKM as DRAINAGE_AREA_SQ_KM,
    HUC02,
    LAT_GAGE as LATITUDE,
    LNG_GAGE as LONGITUDE,
    STATE,
    HCDN_2009 as IS_REFERENCE_HCDN2009,
    HBN36 as IS_REFERENCE_HBN36
from {{ ref('attributes_gageii_BasinID') }}
