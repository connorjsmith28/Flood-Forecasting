-- Climate attributes derived from Open-Meteo weather forcing data
-- Computes CAMELS-style climate indices for each site location

with daily_weather as (
    select
        latitude,
        longitude,
        date_trunc('day', observed_at) as observation_date,
        extract(month from observed_at) as month,
        sum(precipitation_mm) as daily_precip_mm,
        avg(temperature_c) as daily_temp_c,
        min(temperature_c) as daily_temp_min_c,
        max(temperature_c) as daily_temp_max_c,
        avg(humidity_pct) as daily_humidity_pct,
        avg(shortwave_radiation_wm2) as daily_radiation_wm2,
        sum(pet_mm) as daily_pet_mm  -- FAO Penman-Monteith from Open-Meteo
    from {{ ref('stg_weather') }}
    group by 1, 2, 3, 4
),

-- Calculate site-level mean precip for threshold calculations
site_means as (
    select
        latitude,
        longitude,
        avg(daily_precip_mm) as mean_daily_precip_mm
    from daily_weather
    group by 1, 2
),

-- Monthly aggregates for seasonality
monthly_stats as (
    select
        latitude,
        longitude,
        month,
        avg(daily_precip_mm) as monthly_mean_precip_mm,
        avg(daily_pet_mm) as monthly_mean_pet_mm
    from daily_weather
    group by 1, 2, 3
),

-- Seasonality indices using coefficient of variation
seasonality as (
    select
        latitude,
        longitude,
        -- Precipitation seasonality (CV of monthly means)
        stddev(monthly_mean_precip_mm) / nullif(avg(monthly_mean_precip_mm), 0) as p_seasonality,
        -- PET seasonality
        stddev(monthly_mean_pet_mm) / nullif(avg(monthly_mean_pet_mm), 0) as pet_seasonality
    from monthly_stats
    group by 1, 2
),

site_stats as (
    select
        d.latitude,
        d.longitude,

        -- Precipitation statistics
        avg(d.daily_precip_mm) as p_mean_mm_day,
        sum(d.daily_precip_mm) / (count(*) / 365.25) as p_mean_mm_year,

        -- Precipitation frequency indices
        avg(case when d.daily_precip_mm > 0 then 1.0 else 0.0 end) as frac_wet_days,
        avg(case when d.daily_precip_mm < 1 then 1.0 else 0.0 end) as low_prec_freq,
        avg(case when d.daily_precip_mm > 5 * m.mean_daily_precip_mm then 1.0 else 0.0 end) as high_prec_freq,

        -- Snow fraction (precip when temp < 0°C)
        sum(case when d.daily_temp_c < 0 then d.daily_precip_mm else 0 end)
            / nullif(sum(d.daily_precip_mm), 0) as snow_frac,

        -- Temperature statistics
        avg(d.daily_temp_c) as t_mean_c,
        avg(d.daily_temp_max_c) as t_max_mean_c,
        avg(d.daily_temp_min_c) as t_min_mean_c,

        -- Radiation
        avg(d.daily_radiation_wm2) as radiation_mean_wm2,

        -- PET from Open-Meteo (FAO Penman-Monteith)
        avg(d.daily_pet_mm) as pet_mean_mm_day,

        -- Record period
        min(d.observation_date) as period_start,
        max(d.observation_date) as period_end,
        count(*) as n_days

    from daily_weather d
    inner join site_means m on d.latitude = m.latitude and d.longitude = m.longitude
    group by 1, 2
),

final as (
    select
        s.latitude,
        s.longitude,

        -- Precipitation
        s.p_mean_mm_day,
        s.p_mean_mm_year,
        s.frac_wet_days,
        s.low_prec_freq,
        s.high_prec_freq,
        s.snow_frac,
        seas.p_seasonality,

        -- Temperature
        s.t_mean_c,
        s.t_max_mean_c,
        s.t_min_mean_c,

        -- Radiation
        s.radiation_mean_wm2,

        -- PET and aridity (using Open-Meteo FAO Penman-Monteith)
        s.pet_mean_mm_day,
        s.pet_mean_mm_day * 365.25 as pet_mean_mm_year,
        seas.pet_seasonality,
        s.pet_mean_mm_day / nullif(s.p_mean_mm_day, 0) as aridity_index,

        -- Metadata
        s.period_start,
        s.period_end,
        s.n_days

    from site_stats s
    left join seasonality seas on s.latitude = seas.latitude and s.longitude = seas.longitude
)

select * from final
