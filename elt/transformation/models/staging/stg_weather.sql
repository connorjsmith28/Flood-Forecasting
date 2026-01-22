-- Staging model for NLDAS-2 meteorological data
-- Converts units and standardizes column names

with raw_weather as (
    select
        longitude,
        latitude,
        datetime as observed_at,
        prcp,      -- kg/m^2 (equivalent to mm for water)
        temp,      -- Kelvin
        humidity,  -- kg/kg specific humidity
        wind_u,    -- m/s
        wind_v,    -- m/s
        extracted_at
    from {{ source('raw', 'nldas_forcing') }}
),

converted as (
    select
        longitude,
        latitude,
        observed_at,
        -- Precipitation is already in mm equivalent (kg/m^2)
        prcp as precipitation_mm,
        -- Convert Kelvin to Celsius
        temp - 273.15 as temperature_c,
        -- Approximate relative humidity from specific humidity
        -- This is a simplification; proper conversion needs pressure
        humidity * 100000 as humidity_pct,  -- Very rough approximation
        -- Calculate wind speed from components
        sqrt(wind_u * wind_u + wind_v * wind_v) as wind_speed_ms,
        wind_u,
        wind_v,
        extracted_at
    from raw_weather
    where prcp is not null
)

select * from converted
