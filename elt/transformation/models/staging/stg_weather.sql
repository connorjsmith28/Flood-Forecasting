-- Staging model for meteorological data from Open-Meteo
-- Standardizes column names for downstream models

with raw_weather as (
    select
        longitude,
        latitude,
        datetime as observed_at,
        prcp,           -- mm
        temp,           -- Celsius
        humidity,       -- Relative humidity (%)
        wind_speed,     -- m/s
        wind_direction, -- degrees
        extracted_at
    from {{ source('raw', 'weather_forcing') }}
),

standardized as (
    select
        longitude,
        latitude,
        observed_at,
        prcp as precipitation_mm,
        temp as temperature_c,
        humidity as humidity_pct,
        wind_speed as wind_speed_ms,
        wind_direction as wind_direction_deg,
        extracted_at
    from raw_weather
    where prcp is not null
)

select * from standardized
