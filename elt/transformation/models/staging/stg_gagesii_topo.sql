-- GAGES-II topographic attributes
select
    staid as site_id,
    elev_mean_m_basin as elev_mean_m,
    elev_max_m_basin as elev_max_m,
    elev_min_m_basin as elev_min_m,
    elev_median_m_basin as elev_median_m,
    elev_std_m_basin as elev_std_m,
    elev_site_m,
    slope_pct,
    aspect_degrees as aspect_deg,
    aspect_northness,
    aspect_eastness
from {{ ref('attributes_gageii_Topo') }}
