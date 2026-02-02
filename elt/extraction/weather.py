"""Weather data extraction using Open-Meteo API. https://open-meteo.com/en/docs/historical-weather-api"""

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def _is_retryable_error(exc: BaseException) -> bool:
    """Return True if the exception is retryable (rate limits, timeouts, transient errors)."""
    # Retry any OpenMeteoRequestsError - these are all API-side issues
    if isinstance(exc, openmeteo_requests.OpenMeteoRequestsError):
        return True
    # Also check for common transient error keywords
    err = str(exc).lower()
    return any(
        t in err
        for t in ["rate limit", "limit exceeded", "too many requests", "try again", "timeout", "connection"]
    )


_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=30, min=30, max=120),  # Wait 30s-2min between retries
    retry=retry_if_exception(_is_retryable_error),
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


# Rate limiting for parallel fetches
_fetch_semaphore = threading.Semaphore(5)


def fetch_weather_parallel(
    coordinates: list[tuple[float, float]],
    start_date,
    end_date,
    variables: list[str] | None = None,
    max_workers: int = 1,
    log: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """Fetch weather data with parallel coordinate batches.

    Uses ThreadPoolExecutor to fetch multiple coordinate batches concurrently,
    with rate limiting via semaphore to respect API limits.

    Args:
        coordinates: List of (longitude, latitude) tuples
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        variables: Weather variables to fetch (defaults to all)
        max_workers: Maximum concurrent API requests (default: 5)
        log: Optional logging function (e.g., context.log.info)

    Returns:
        Polars DataFrame with weather data for all coordinates
    """
    variables = list(variables or WEATHER_VARS.keys())
    hourly_vars = [WEATHER_VARS.get(v, v) for v in variables]

    def _log(msg: str) -> None:
        if log is not None:
            log(msg)
        else:
            logger.info("%s", msg)

    # Split coordinates into batches
    chunks = [coordinates[i : i + BATCH_SIZE] for i in range(0, len(coordinates), BATCH_SIZE)]
    _log(
        f"Starting parallel Open-Meteo fetch: {len(coordinates)} coordinates, "
        f"{len(chunks)} batches, {max_workers} workers, "
        f"date range {str(start_date)[:10]} → {str(end_date)[:10]}"
    )

    @_retry
    def fetch_batch_with_rate_limit(chunk_idx: int, coords: list[tuple[float, float]]) -> list[pl.DataFrame]:
        """Fetch a batch with concurrency limited by semaphore."""
        with _fetch_semaphore:
            lons, lats = zip(*coords)
            client = openmeteo_requests.Client()
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
            # Small delay after each request to avoid rate limits
            time.sleep(2)
            return [_parse_response(r, lons[i], lats[i], variables) for i, r in enumerate(responses)]

    all_results: list[pl.DataFrame] = []
    failed_batches = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_batch_with_rate_limit, i, chunk): i
            for i, chunk in enumerate(chunks)
        }

        for future in as_completed(futures):
            chunk_idx = futures[future]
            try:
                dfs = future.result()
                non_empty = [df for df in dfs if not df.is_empty()]
                all_results.extend(non_empty)
                _log(f"Completed batch {chunk_idx + 1}/{len(chunks)}: {len(non_empty)} responses")
            except Exception as e:
                failed_batches += 1
                # Log the full error chain to understand what Open-Meteo is returning
                cause = e.__cause__ if hasattr(e, '__cause__') else None
                _log(f"Batch {chunk_idx + 1}/{len(chunks)} failed: {e}")
                if cause:
                    _log(f"  Caused by: {type(cause).__name__}: {cause}")

    if failed_batches > 0:
        _log(f"Warning: {failed_batches}/{len(chunks)} batches failed")

    if not all_results:
        _log("Parallel Open-Meteo fetch returned no data")
        return pl.DataFrame()

    result = pl.concat(all_results)
    _log(f"Parallel Open-Meteo fetch complete: {result.height} rows")
    return result
