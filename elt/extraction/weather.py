"""Weather data extraction using Open-Meteo API.

Fetches hourly meteorological forcing data for hydrological modeling.
https://open-meteo.com/en/docs/historical-weather-api

Free tier limits:
- 600 calls/minute
- 5,000 calls/hour
- 10,000 calls/day
- 300,000 calls/month
"""

import logging
import time
from datetime import datetime

import openmeteo_requests
import pandas as pd

logger = logging.getLogger(__name__)

# Request configuration
REQUEST_TIMEOUT = 120  # seconds
BATCH_SIZE = 25  # max coordinates per request to avoid timeouts

# Rate limiting for free tier (600 calls/min = 10/sec, we use 8/sec for safety)
DEFAULT_CALLS_PER_MINUTE = 500  # Conservative limit under 600
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 60  # seconds to wait on rate limit error

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variable mapping: our name -> Open-Meteo name
OPENMETEO_VARIABLES = {
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


def _parse_response(response, longitude, latitude, variables):
    """Parse Open-Meteo response into a pandas DataFrame."""
    hourly = response.Hourly()
    if hourly is None:
        return pd.DataFrame()

    # Build datetime range from response metadata
    datetime_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    data = {
        "longitude": longitude,
        "latitude": latitude,
        "datetime": datetime_range,
    }

    # Add each variable, mapping back to our names
    for i, var in enumerate(variables):
        values = hourly.Variables(i)
        if values is not None:
            data[var] = values.ValuesAsNumpy()

    return pd.DataFrame(data)


def _fetch_batch_with_retry(
    client,
    coordinates,
    start_date,
    end_date,
    hourly_vars,
    variables,
    retry_attempts=DEFAULT_RETRY_ATTEMPTS,
    retry_backoff=DEFAULT_RETRY_BACKOFF,
):
    """Fetch weather data for a batch of coordinates with retry logic."""
    lons = [coord[0] for coord in coordinates]
    lats = [coord[1] for coord in coordinates]

    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_vars,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }

    last_error = None
    for attempt in range(retry_attempts):
        try:
            responses = client.weather_api(
                ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT
            )

            batch_dfs = []
            for i, response in enumerate(responses):
                df = _parse_response(response, lons[i], lats[i], variables)
                if not df.empty:
                    batch_dfs.append(df)

            return batch_dfs

        except Exception as e:
            last_error = e
            error_str = str(e).lower()

            # Check for rate limit errors (429 or "too many requests")
            is_rate_limit = "429" in error_str or "too many" in error_str

            if is_rate_limit:
                wait_time = retry_backoff * (2**attempt)  # Exponential backoff
                logger.warning(
                    f"Rate limit hit, waiting {wait_time}s before retry "
                    f"(attempt {attempt + 1}/{retry_attempts})"
                )
                time.sleep(wait_time)
            elif attempt < retry_attempts - 1:
                # For other errors, shorter retry delay
                wait_time = 5 * (attempt + 1)
                logger.warning(
                    f"Request failed: {e}. Retrying in {wait_time}s "
                    f"(attempt {attempt + 1}/{retry_attempts})"
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retry_attempts} attempts: {e}")

    raise last_error


def fetch_weather_forcing(
    coordinates,
    start_date,
    end_date,
    variables=None,
    calls_per_minute=DEFAULT_CALLS_PER_MINUTE,
    retry_attempts=DEFAULT_RETRY_ATTEMPTS,
    retry_backoff=DEFAULT_RETRY_BACKOFF,
):
    """Fetch hourly weather forcing data from Open-Meteo.

    Args:
        coordinates: List of (longitude, latitude) tuples
        start_date: Start date (datetime or string)
        end_date: End date (datetime or string)
        variables: List of variable names to fetch (default: prcp, temp, humidity, wind_speed, wind_direction)
        calls_per_minute: Max API calls per minute (default: 500, free tier limit is 600)
        retry_attempts: Number of retry attempts on failure (default: 3)
        retry_backoff: Base backoff time in seconds for rate limit errors (default: 60)
    """
    if not variables:
        variables = [
            "prcp",
            "temp",
            "humidity",
            "wind_speed",
            "wind_direction",
            "rsds",  # Shortwave radiation for PET calculation
            "pet",  # FAO Penman-Monteith reference ET from Open-Meteo
        ]
    variables = list(variables)

    # Format dates
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    else:
        start_date = start_date[:10]
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")
    else:
        end_date = end_date[:10]

    # Map variable names to Open-Meteo names
    hourly_vars = [OPENMETEO_VARIABLES.get(v, v) for v in variables]

    client = openmeteo_requests.Client()

    all_dfs = []
    total_batches = (len(coordinates) + BATCH_SIZE - 1) // BATCH_SIZE

    # Calculate delay between batches to respect rate limit
    # Each batch is 1 API call, so delay = 60 / calls_per_minute
    min_delay_between_batches = 60.0 / calls_per_minute if calls_per_minute > 0 else 0

    logger.info(
        f"Fetching weather for {len(coordinates)} locations in {total_batches} batches "
        f"(rate limit: {calls_per_minute} calls/min)"
    )

    # Process in batches with rate limiting
    for batch_num, i in enumerate(range(0, len(coordinates), BATCH_SIZE)):
        batch_coords = coordinates[i : i + BATCH_SIZE]
        batch_start_time = time.time()

        if batch_num > 0 and batch_num % 10 == 0:
            logger.info(f"Progress: {batch_num}/{total_batches} batches completed")

        batch_dfs = _fetch_batch_with_retry(
            client,
            batch_coords,
            start_date,
            end_date,
            hourly_vars,
            variables,
            retry_attempts=retry_attempts,
            retry_backoff=retry_backoff,
        )
        all_dfs.extend(batch_dfs)

        # Rate limiting: ensure we don't exceed calls_per_minute
        elapsed = time.time() - batch_start_time
        if elapsed < min_delay_between_batches and batch_num < total_batches - 1:
            sleep_time = min_delay_between_batches - elapsed
            time.sleep(sleep_time)

    logger.info(f"Completed fetching weather data: {total_batches} batches processed")

    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)
