-- Core GAGES-II Riparian attributes with 100m buffer, 2006 era
select
    staid as site_id,
    RIP100_DEV as rip100_dev,
    RIP100_FOREST as rip100_forest,
    RIP100_PLANT as rip100_plant,
    RIP100_11 as rip100_11,
    RIP100_12 as rip100_12,
    RIP100_21 as rip100_21,
    RIP100_22 as rip100_22,
    RIP100_23 as rip100_23,
    RIP100_24 as rip100_24,
    RIP100_31 as rip100_31,
    RIP100_41 as rip100_41,
    RIP100_42 as rip100_42,
    RIP100_43 as rip100_43,
    RIP100_52 as rip100_52,
    RIP100_71 as rip100_71,
    RIP100_81 as rip100_81,
    RIP100_82 as rip100_82,
    RIP100_90 as rip100_90,
    RIP100_95 as rip100_95
from {{ ref('attributes_gageii_LC06_Rip100') }}

