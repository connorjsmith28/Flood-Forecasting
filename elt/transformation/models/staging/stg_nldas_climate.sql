-- NLDAS-2 derived climate attributes (CAMELS-style indices)
select
    STAID as SITE_ID,
    P_MEAN,
    PET_MEAN,
    ARIDITY_INDEX,
    P_SEASONALITY,
    FRAC_SNOW,
    HIGH_PREC_FREQ,
    HIGH_PREC_DUR,
    LOW_PREC_FREQ,
    LOW_PREC_DUR
from {{ ref('attributes_nldas2_climate') }}
