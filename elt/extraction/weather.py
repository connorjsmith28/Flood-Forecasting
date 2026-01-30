"""Weather data extraction using Open-Meteo API. https://open-meteo.com/en/docs/historical-weather-api"""

import logging
import time
from collections.abc import Callable

import numpy as np
import openmeteo_requests
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = logging.getLogger(__name__)

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
    """Return True if the exception looks like an Open-Meteo rate limit error."""
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

    return pl.DataFrame(
        {
            "longitude": lon,
            "latitude": lat,
            "datetime": np.arange(hourly.Time(), hourly.TimeEnd(), hourly.Interval()) * 1000,
            **var_data,
        }
    ).cast({"datetime": pl.Datetime("ms", "UTC")})


def fetch_weather_forcing(
    coordinates,
    start_date,
    end_date,
    variables=None,
    log: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """Fetch hourly weather forcing data from Open-Meteo.

    Adds per-batch logging so Dagster shows progress while long-running
    API calls are in flight.
    """
    variables = list(variables or WEATHER_VARS.keys())
    hourly_vars = [WEATHER_VARS.get(v, v) for v in variables]

    client = openmeteo_requests.Client()

    def _log(msg: str) -> None:
        # Prefer caller-provided logger (e.g. Dagster `context.log.info`) so messages
        # show up as Dagster events even if Python logging isn't configured at INFO.
        if log is not None:
            log(msg)
        else:
            logger.info("%s", msg)

    @_retry
    def fetch_batch(coords):
        lons, lats = zip(*coords)
        _log(
            f"Requesting Open-Meteo archive for {len(coords)} coordinates "
            f"from {str(start_date)[:10]} to {str(end_date)[:10]}"
        )
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

    all_dfs: list[pl.DataFrame] = []
    chunks = [coordinates[i : i + BATCH_SIZE] for i in range(0, len(coordinates), BATCH_SIZE)]

    _log(
        "Starting Open-Meteo fetch: "
        f"{len(coordinates)} coordinates, {len(chunks)} batches, "
        f"date range {str(start_date)[:10]} → {str(end_date)[:10]}"
    )

    for idx, chunk in enumerate(chunks, start=1):
        if idx > 1:
            # Small delay between batches to avoid rate limits
            time.sleep(2)

        _log(f"Fetching batch {idx}/{len(chunks)} ({len(chunk)} coordinates) from Open-Meteo")
        dfs = fetch_batch(chunk)
        non_empty = [df for df in dfs if not df.is_empty()]
        all_dfs.extend(non_empty)
        _log(
            f"Finished batch {idx}/{len(chunks)}: {len(non_empty)} non-empty responses "
            f"(frames={len(all_dfs)})"
        )

    if not all_dfs:
        if log is not None:
            log("Open-Meteo fetch returned no data for any coordinate")
        else:
            logger.warning("Open-Meteo fetch returned no data for any coordinate")
        return pl.DataFrame()

    result = pl.concat(all_dfs)
    _log(f"Open-Meteo fetch complete: {result.height} rows")
    return result
