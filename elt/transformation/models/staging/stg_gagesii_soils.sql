-- GAGES-II soil attributes
select
    STAID as site_id,
    AWCAVE as soil_water_capacity_avg,
    PERMAVE as soil_permeability_avg,
    BDAVE as bulk_density_avg,
    OMAVE as organic_matter_avg,
    WTDEPAVE as water_table_depth_avg,
    ROCKDEPAVE as rock_depth_avg,
    CLAYAVE as clay_pct_avg,
    SILTAVE as silt_pct_avg,
    SANDAVE as sand_pct_avg,
    KFACT_UP as erodibility_factor
from {{ ref('attributes_gageii_Soils') }}
