-- HydroATLAS catchment attributes (195 attributes)
-- Includes hydrology, physiography, climate, soils, land cover, and anthropogenic factors
select
    * exclude (STAID),
    STAID as SITE_ID
from {{ ref('attributes_hydroATLAS') }}
