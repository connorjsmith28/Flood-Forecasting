-- Daily max streamflow and gage height from hourly (IV) sites only
-- Aggregates the hourly flood model to daily by taking the peak values per site per day

select
    site_id,
    observation_hour::date as observed_date,
    max(streamflow_cfs_max) as streamflow_cfs_max,
    max(gage_height_ft_max) as gage_height_ft_max
from {{ ref('flood_model') }}
group by site_id, observation_hour::date
