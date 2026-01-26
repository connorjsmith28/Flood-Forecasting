-- HydroATLAS catchment attributes (195 attributes)
-- Includes hydrology, physiography, climate, soils, land cover,
-- and anthropogenic factors
select
    * exclude (staid),
    staid as site_id
from {{ ref('attributes_hydroATLAS') }}
