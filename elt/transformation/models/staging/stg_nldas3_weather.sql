-- NLDAS-3 watershed-averaged forcing data
-- Matches CAMELS-H methodology: area-weighted averages over watershed boundaries
-- Joins on site_id (not coordinates) since values are already watershed-averaged

select
    site_id,
    datetime as observed_at,
    air_temp_c as temperature_c,
    precipitation_mm,
    specific_humidity_kgkg,
    surface_pressure_pa,
    wind_u_ms,
    wind_v_ms,
    -- Compute wind speed from u/v components
    shortwave_radiation_wm2,
    longwave_radiation_wm2,
    potential_evaporation_mm,
    cape_jkg,
    convective_precip_fraction,
    extracted_at,
    sqrt(power(wind_u_ms, 2) + power(wind_v_ms, 2)) as wind_speed_ms
from {{ source('raw', 'nldas3_forcing') }}
