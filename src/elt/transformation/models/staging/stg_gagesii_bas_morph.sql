-- Core GAGES-II basin morphology attributes
select
    staid as site_id,
    BAS_COMPACTNESS as basin_compactness
from {{ ref('attributes_gageii_Bas_Morph') }}
