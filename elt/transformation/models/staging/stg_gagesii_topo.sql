-- GAGES-II topographic attributes
select
    STAID as SITE_ID,
    ELEV_MEAN_M_BASIN as ELEV_MEAN_M,
    ELEV_MAX_M_BASIN as ELEV_MAX_M,
    ELEV_MIN_M_BASIN as ELEV_MIN_M,
    ELEV_MEDIAN_M_BASIN as ELEV_MEDIAN_M,
    ELEV_STD_M_BASIN as ELEV_STD_M,
    ELEV_SITE_M,
    SLOPE_PCT,
    ASPECT_DEGREES as ASPECT_DEG,
    ASPECT_NORTHNESS,
    ASPECT_EASTNESS
from {{ ref('attributes_gageii_Topo') }}
