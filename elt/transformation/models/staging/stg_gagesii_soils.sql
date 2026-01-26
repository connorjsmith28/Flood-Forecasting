-- GAGES-II soil attributes
select
    staid as site_id,
    awcave as soil_water_capacity_avg,
    permave as soil_permeability_avg,
    bdave as bulk_density_avg,
    omave as organic_matter_avg,
    wtdepave as water_table_depth_avg,
    rockdepave as rock_depth_avg,
    clayave as clay_pct_avg,
    siltave as silt_pct_avg,
    sandave as sand_pct_avg,
    kfact_up as erodibility_factor
from {{ ref('attributes_gageii_Soils') }}
