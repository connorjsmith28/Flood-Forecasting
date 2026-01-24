-- GAGES-II topographic attributes
select
    STAID as site_id,
    ELEV_MEAN_M_BASIN as elev_mean_m,
    ELEV_MAX_M_BASIN as elev_max_m,
    ELEV_MIN_M_BASIN as elev_min_m,
    ELEV_MEDIAN_M_BASIN as elev_median_m,
    ELEV_STD_M_BASIN as elev_std_m,
    ELEV_SITE_M as elev_site_m,
    SLOPE_PCT as slope_pct,
    ASPECT_DEGREES as aspect_deg,
    ASPECT_NORTHNESS as aspect_northness,
    ASPECT_EASTNESS as aspect_eastness
from {{ ref('attributes_gageii_Topo') }}
