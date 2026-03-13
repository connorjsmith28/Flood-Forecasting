-- Core GAGES-II Pesticide attapplication from 1997
select
    staid as site_id,
    PESTAPP_KG_SQKM as pestapp_kg_sqkm
from {{ ref('attributes_gageii_Pest_App') }}

