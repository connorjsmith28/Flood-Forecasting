select
    site_id,
    -- Include all characteristic columns dynamically
    -- NLDI characteristics vary by site, so we select all available columns
    * exclude (site_id)
from {{ source('raw', 'nldi_basin_attributes') }}
