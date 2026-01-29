"""Weather data extraction using Open-Meteo API. https://open-meteo.com/en/docs/historical-weather-api"""

import time
import numpy as np
import openmeteo_requests
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
BATCH_SIZE = 50  # coordinates per request (balance between fewer calls vs timeout risk)

# Variable mapping: our name -> Open-Meteo API name
WEATHER_VARS = {
    "prcp": "precipitation",
    "temp": "temperature_2m",
    "humidity": "relative_humidity_2m",
    "wind_speed": "wind_speed_10m",
    "wind_direction": "wind_direction_10m",
    "rsds": "shortwave_radiation",
    "rlds": "terrestrial_radiation",
    "psurf": "surface_pressure",
    "pet": "et0_fao_evapotranspiration",
}


def _is_rate_limit_error(exc: BaseException) -> bool:
    err = str(exc).lower()
    return any(t in err for t in ["rate limit", "limit exceeded", "too many requests", "try again"])


_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=30, min=30, max=120),  # Wait 30s-2min between retries
    retry=retry_if_exception(_is_rate_limit_error),
)


def _parse_response(response, lon, lat, variables) -> pl.DataFrame:
    """Parse Open-Meteo response into a Polars DataFrame."""
    hourly = response.Hourly()
    if hourly is None:
        return pl.DataFrame()

    var_data = {
        var: hourly.Variables(i).ValuesAsNumpy()
        for i, var in enumerate(variables)
        if hourly.Variables(i) is not None
    }

    return pl.DataFrame({
        "longitude": lon,
        "latitude": lat,
        "datetime": np.arange(hourly.Time(), hourly.TimeEnd(), hourly.Interval()) * 1000,
        **var_data,
    }).cast({"datetime": pl.Datetime("ms", "UTC")})


def fetch_weather_forcing(coordinates, start_date, end_date, variables=None) -> pl.DataFrame:
    """Fetch hourly weather forcing data from Open-Meteo."""
    variables = list(variables or WEATHER_VARS.keys())
    hourly_vars = [WEATHER_VARS.get(v, v) for v in variables]

    client = openmeteo_requests.Client()

    @_retry
    def fetch_batch(coords):
        lons, lats = zip(*coords)
        responses = client.weather_api(
            ARCHIVE_URL,
            params={
                "latitude": lats,
                "longitude": lons,
                "start_date": str(start_date)[:10],
                "end_date": str(end_date)[:10],
                "hourly": hourly_vars,
                "timezone": "UTC",
                "wind_speed_unit": "ms",
            },
            timeout=120,
        )
        return [_parse_response(r, lons[i], lats[i], variables) for i, r in enumerate(responses)]

    all_dfs = []
    chunks = [coordinates[i:i + BATCH_SIZE] for i in range(0, len(coordinates), BATCH_SIZE)]

    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(2)  # Small delay between batches to avoid rate limits
        dfs = fetch_batch(chunk)
        all_dfs.extend([df for df in dfs if not df.is_empty()])

    return pl.concat(all_dfs) if all_dfs else pl.DataFrame()
