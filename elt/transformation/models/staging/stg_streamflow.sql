select
    site_id,
    datetime as observed_at,
    streamflow_cfs,
    qualifiers,
    extracted_at
from {{ source('raw', 'streamflow_raw') }}
