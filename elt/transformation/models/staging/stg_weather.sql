select
    longitude,
    latitude,
    datetime as observed_at,
    prcp as precipitation_mm,
    temp as temperature_c,
    humidity as humidity_pct,
    wind_speed as wind_speed_ms,
    wind_direction as wind_direction_deg,
    rsds as shortwave_radiation_wm2,
    pet as pet_mm,  -- FAO Penman-Monteith reference ET from Open-Meteo
    extracted_at
from {{ source('raw', 'weather_forcing') }}
