-- Core GAGES-II basin classification attributes
select
    staid as site_id,
    AGGECOREGION as agg_ecoregion,
    HYDRO_DISTURB_INDX as hydro_disturbance_index
from {{ ref('attributes_gageii_Bas_Classif') }}