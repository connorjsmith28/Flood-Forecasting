-- Core GAGES-II landscape mainstem attributes with 800m buffer, 2006 era
select
    staid as site_id,
    MAINS800_DEV as mains800_dev,
    MAINS800_FOREST as mains800_forest,
    MAINS800_PLANT as mains800_plant,
    MAINS800_11 as mains800_11,
    MAINS800_12 as mains800_12,
    MAINS800_21 as mains800_21,
    MAINS800_22 as mains800_22,
    MAINS800_23 as mains800_23,
    MAINS800_24 as mains800_24,
    MAINS800_31 as mains800_31,
    MAINS800_41 as mains800_41,
    MAINS800_42 as mains800_42,
    MAINS800_43 as mains800_43,
    MAINS800_52 as mains800_52,
    MAINS800_71 as mains800_71,
    MAINS800_81 as mains800_81,
    MAINS800_82 as mains800_82,
    MAINS800_90 as mains800_90,
    MAINS800_95 as mains800_95
from {{ ref('attributes_gageii_LC06_Mains800') }}

