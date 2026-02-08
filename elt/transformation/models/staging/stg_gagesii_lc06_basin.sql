-- Core GAGES-II landscape basin attributes from 2006
select
    staid as site_id,
    DEVNLCD06 as dev_nlcd06,
    FORESTNLCD06 as forest_nlcd06,
    PLANTNLCD06 as plant_nlcd06,
    WATERNLCD06 as water_nlcd06,
    SNOWICENLCD06 as snow_ice_nlcd06,
    DEVOPENNLCD06 as dev_open_nlcd06,
    DEVLOWNLCD06 as dev_low_nlcd06,
    DEVMEDNLCD06 as dev_med_nlcd06,
    DEVHINLCD06 as dev_high_nlcd06,
    BARRENNLCD06 as barren_nlcd06,
    DECIDNLCD06 as deciduous_nlcd06,
    EVERGRNLCD06 as evergreen_nlcd06,
    MIXEDFORNLCD06 as mixed_forest_nlcd06,
    SHRUBNLCD06 as shrub_nlcd06,
    GRASSNLCD06 as grass_nlcd06,
    PASTURENLCD06 as pasture_nlcd06,
    CROPSNLCD06 as crops_nlcd06,
    WOODYWETNLCD06 as woody_wet_nlcd06,
    EMERGWETNLCD06 as emergent_wet_nlcd06
from {{ ref('attributes_gageii_LC06_Basin') }}

