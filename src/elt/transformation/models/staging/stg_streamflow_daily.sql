select
    site_id,
    date as observed_date,
    streamflow_cfs_mean,
    gage_height_ft_mean,
    qualifiers,
    extracted_at
from {{ source('raw', 'streamflow_daily') }}
