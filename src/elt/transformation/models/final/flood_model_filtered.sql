-- Filtered ML-ready training data: high flood risk sites with good data quality
-- Only includes sites with above-median streamflow CV and <20% null rates
-- See data_exploration/site_filtering.ipynb for the analysis

{{ config(materialized='table') }}

select
    flood.*
from {{ ref('flood_model') }} as flood
inner join {{ ref('filtered_site_ids') }} as filtered
    on flood.site_id = filtered.site_id
