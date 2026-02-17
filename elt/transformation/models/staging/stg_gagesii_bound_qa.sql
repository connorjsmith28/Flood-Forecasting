-- Core GAGES-II Boundary question and answer attributes
select
    staid as site_id,
    BASIN_BOUNDARY_CONFIDENCE as basin_boundary_confidence,
    NWIS_DRAIN_SQKM as nwis_drainage_area,
    HUC10_CHECK as huc10_check,
from {{ ref('attributes_gageii_Bound_QA') }}