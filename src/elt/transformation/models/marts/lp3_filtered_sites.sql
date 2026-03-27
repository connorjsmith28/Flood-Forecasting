-- Sites based off Log-Pearson Type 3 distribution filtered sites

select
    site_id,
    Q2_cfs,
    Q5_cfs,
    Q10_cfs,
    Q25_cfs,
    Q50_cfs,
    Q100_cfs,
    flood_severity_score
from {{ ref('lp3_results') }}
where
    flood_severity_score >= 1.25
