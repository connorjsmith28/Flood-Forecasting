select
    longitude,
    latitude,
    datetime as observed_at,
    prcp as precipitation_mm,
    temp as temperature_c,
    humidity as humidity_pct,
    wind_speed as wind_speed_ms,
    wind_direction as wind_direction_deg,
    extracted_at
from {{ source('raw', 'weather_forcing') }}
