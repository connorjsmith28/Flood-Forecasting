"""Weather data extraction using Open-Meteo API.

Fetches hourly meteorological forcing data for hydrological modeling.
https://open-meteo.com/en/docs/historical-weather-api
"""

from datetime import datetime

import openmeteo_requests
import pandas as pd

# Request configuration
REQUEST_TIMEOUT = 120  # seconds
BATCH_SIZE = 25  # max coordinates per request to avoid timeouts

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


def _fetch_batch(client, coordinates, start_date, end_date, hourly_vars, variables):
    """Fetch weather data for a batch of coordinates."""
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

    responses = client.weather_api(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT)

    batch_dfs = []
    for i, response in enumerate(responses):
        df = _parse_response(response, lons[i], lats[i], variables)
        if not df.empty:
            batch_dfs.append(df)

    return batch_dfs


def fetch_weather_forcing(coordinates, start_date, end_date, variables=None):
    """Fetch hourly weather forcing data from Open-Meteo.

    Args:
        coordinates: List of (longitude, latitude) tuples
        start_date: Start date (datetime or string)
        end_date: End date (datetime or string)
        variables: List of variable names to fetch (default: prcp, temp, humidity, wind_speed, wind_direction)
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

    # Process in batches to avoid timeouts on large requests
    for i in range(0, len(coordinates), BATCH_SIZE):
        batch_coords = coordinates[i : i + BATCH_SIZE]

        batch_dfs = _fetch_batch(
            client, batch_coords, start_date, end_date, hourly_vars, variables
        )
        all_dfs.extend(batch_dfs)

    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)
