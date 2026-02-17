-- Core GAGES-II landscape mainstem attributes with 100m buffer, 2006 era
select
    staid as site_id,
    MAINS100_DEV as mains100_dev,
    MAINS100_FOREST as mains100_forest,
    MAINS100_PLANT as mains100_plant,
    MAINS100_11 as mains100_11,
    MAINS100_12 as mains100_12,
    MAINS100_21 as mains100_21,
    MAINS100_22 as mains100_22,
    MAINS100_23 as mains100_23,
    MAINS100_24 as mains100_24,
    MAINS100_31 as mains100_31,
    MAINS100_41 as mains100_41,
    MAINS100_42 as mains100_42,
    MAINS100_43 as mains100_43,
    MAINS100_52 as mains100_52,
    MAINS100_71 as mains100_71,
    MAINS100_81 as mains100_81,
    MAINS100_82 as mains100_82,
    MAINS100_90 as mains100_90,
    MAINS100_95 as mains100_95
from {{ ref('attributes_gageii_LC06_Mains100') }}

