-- HydroATLAS catchment attributes (195 attributes)
-- Includes hydrology, physiography, climate, soils, land cover, and anthropogenic factors
select
    STAID as site_id,
    * exclude (STAID)
from {{ ref('attributes_hydroATLAS') }}
