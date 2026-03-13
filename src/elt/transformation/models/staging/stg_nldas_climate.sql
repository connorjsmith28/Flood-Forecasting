-- NLDAS-2 derived climate attributes (CAMELS-style indices)
select
    staid as site_id,
    p_mean,
    pet_mean,
    aridity_index,
    p_seasonality,
    frac_snow,
    high_prec_freq,
    high_prec_dur,
    low_prec_freq,
    low_prec_dur
from {{ ref('attributes_nldas2_climate') }}
