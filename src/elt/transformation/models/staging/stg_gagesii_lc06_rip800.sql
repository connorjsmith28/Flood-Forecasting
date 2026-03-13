-- Core GAGES-II Riparian attributes with 800m buffer, 2006 era
select
    staid as site_id,
    RIP800_DEV as rip800_dev,
    RIP800_FOREST as rip800_forest,
    RIP800_PLANT as rip800_plant,
    RIP800_11 as rip800_11,
    RIP800_12 as rip800_12,
    RIP800_21 as rip800_21,
    RIP800_22 as rip800_22,
    RIP800_23 as rip800_23,
    RIP800_24 as rip800_24,
    RIP800_31 as rip800_31,
    RIP800_41 as rip800_41,
    RIP800_42 as rip800_42,
    RIP800_43 as rip800_43,
    RIP800_52 as rip800_52,
    RIP800_71 as rip800_71,
    RIP800_81 as rip800_81,
    RIP800_82 as rip800_82,
    RIP800_90 as rip800_90,
    RIP800_95 as rip800_95
from {{ ref('attributes_gageii_LC06_Rip800') }}

