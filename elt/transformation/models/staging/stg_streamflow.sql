-- Staging model for streamflow observations
-- Cleans and standardizes raw USGS streamflow data

with raw_streamflow as (
    select
        site_id,
        datetime as observed_at,
        streamflow_cfs,
        qualifiers,
        extracted_at
    from {{ source('raw', 'streamflow_raw') }}
),

cleaned as (
    select
        site_id,
        observed_at,
        streamflow_cfs,
        -- Convert to metric (cubic meters per second)
        streamflow_cfs * 0.0283168 as streamflow_cms,
        -- Check if value is estimated based on qualifiers
        case
            when qualifiers like '%e%' or qualifiers like '%E%' then true
            else false
        end as is_estimated,
        qualifiers,
        extracted_at
    from raw_streamflow
    where streamflow_cfs is not null
      and streamflow_cfs >= 0  -- Remove negative/invalid values
)

select * from cleaned
