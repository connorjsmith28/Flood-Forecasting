-- Sites based off Log-Pearson Type 3 distribution filtered sites

select
    site_id
from {{ ref('lp3_results') }}
where
    flood_severity_score >= 1.25
