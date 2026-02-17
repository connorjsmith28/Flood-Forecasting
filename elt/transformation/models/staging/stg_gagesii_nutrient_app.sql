-- Core GAGES-II Nutrient application from 1997
select
    staid as site_id,
    NITR_APP_KG_SQKM as nitr_app_kg_sqkm,
    PHOS_APP_KG_SQKM as phos_app_kg_sqkm
from {{ ref('attributes_gageii_Nutrient_App') }}

