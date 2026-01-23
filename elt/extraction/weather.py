"""Weather data extraction using Open-Meteo API.

Fetches hourly meteorological forcing data for hydrological modeling.
https://open-meteo.com/en/docs/historical-weather-api

Replaces the discontinued NASA NLDAS-2 Data Rods service.
"""

from datetime import datetime
from typing import Sequence

import httpx
import polars as pl

# Open-Meteo API configuration
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variable mapping: our name -> Open-Meteo name
OPENMETEO_VARIABLES = {
    "prcp": "precipitation",
    "temp": "temperature_2m",
    "humidity": "relative_humidity_2m",
    "wind_u": "wind_speed_10m",  # Open-Meteo gives speed, not components
    "wind_v": "wind_direction_10m",  # We'll store direction instead
    "wind_speed": "wind_speed_10m",
    "wind_direction": "wind_direction_10m",
    "rsds": "shortwave_radiation",
    "rlds": "terrestrial_radiation",
    "psurf": "surface_pressure",
    "pet": "et0_fao_evapotranspiration",
}

# Reverse mapping for response parsing
REVERSE_MAPPING = {v: k for k, v in OPENMETEO_VARIABLES.items()}


def _format_date(dt: datetime | str) -> str:
    """Format date for Open-Meteo API."""
    if isinstance(dt, str):
        return dt[:10]  # Take just the date part
    return dt.strftime("%Y-%m-%d")


def _build_request_params(
    latitudes: list[float],
    longitudes: list[float],
    start_date: str,
    end_date: str,
    variables: list[str],
) -> dict:
    """Build request parameters for Open-Meteo API."""
    # Map our variable names to Open-Meteo names
    hourly_vars = []
    for var in variables:
        if var in OPENMETEO_VARIABLES:
            hourly_vars.append(OPENMETEO_VARIABLES[var])
        elif var in REVERSE_MAPPING:
            hourly_vars.append(var)

    return {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
        "timezone": "UTC",
        "wind_speed_unit": "ms",  # Match NLDAS units (m/s)
    }


def _parse_response(
    response: dict,
    longitude: float,
    latitude: float,
    variables: list[str],
) -> pl.DataFrame:
    """Parse Open-Meteo response into a Polars DataFrame."""
    hourly = response.get("hourly", {})

    if not hourly or "time" not in hourly:
        return pl.DataFrame()

    # Build data dict
    data = {
        "longitude": [longitude] * len(hourly["time"]),
        "latitude": [latitude] * len(hourly["time"]),
        "datetime": hourly["time"],
    }

    # Add each variable, mapping back to our names
    for var in variables:
        om_var = OPENMETEO_VARIABLES.get(var, var)
        if om_var in hourly:
            data[var] = hourly[om_var]

    df = pl.DataFrame(data)

    # Convert datetime string to proper datetime type
    df = df.with_columns(pl.col("datetime").str.to_datetime("%Y-%m-%dT%H:%M"))

    return df


def fetch_weather_forcing(
    coordinates: Sequence[tuple[float, float]],
    start_date: str | datetime,
    end_date: str | datetime,
    variables: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Fetch hourly weather forcing data from Open-Meteo.

    Args:
        coordinates: List of (longitude, latitude) tuples
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        variables: List of variables to fetch. If None, fetches default set.
            Options: prcp, temp, humidity, wind_speed, wind_direction,
                     rsds, rlds, psurf, pet

    Returns:
        Polars DataFrame with columns:
            - longitude, latitude: Coordinates
            - datetime: Timestamp (UTC)
            - One column per requested variable
    """
    if variables is None:
        variables = ["prcp", "temp", "humidity", "wind_speed", "wind_direction"]

    variables = list(variables)
    start_str = _format_date(start_date)
    end_str = _format_date(end_date)

    # Open-Meteo supports batch requests with multiple coordinates
    lons = [coord[0] for coord in coordinates]
    lats = [coord[1] for coord in coordinates]

    params = _build_request_params(lats, lons, start_str, end_str, variables)

    try:
        with httpx.Client(timeout=120.0) as client:
            response = client.get(ARCHIVE_URL, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as e:
        print(f"Warning: Failed to fetch weather data: {e}")
        # Return empty DataFrame with expected schema
        schema = {
            "longitude": pl.Float64,
            "latitude": pl.Float64,
            "datetime": pl.Datetime("us"),
        }
        for var in variables:
            schema[var] = pl.Float64
        return pl.DataFrame(schema=schema)

    # Handle single vs multiple coordinate responses
    if isinstance(data, list):
        # Multiple coordinates - list of responses
        all_dfs = []
        for i, resp in enumerate(data):
            df = _parse_response(resp, lons[i], lats[i], variables)
            if not df.is_empty():
                all_dfs.append(df)
        if not all_dfs:
            schema = {
                "longitude": pl.Float64,
                "latitude": pl.Float64,
                "datetime": pl.Datetime("us"),
            }
            for var in variables:
                schema[var] = pl.Float64
            return pl.DataFrame(schema=schema)
        result = pl.concat(all_dfs, how="diagonal")
    else:
        # Single coordinate - direct response
        result = _parse_response(data, lons[0], lats[0], variables)

    return result


def fetch_weather_for_basins(
    basin_centroids: pl.DataFrame,
    start_date: str | datetime,
    end_date: str | datetime,
    variables: Sequence[str] | None = None,
    id_column: str = "site_id",
    lon_column: str = "longitude",
    lat_column: str = "latitude",
) -> pl.DataFrame:
    """Fetch weather data for basin centroids, preserving basin IDs.

    Args:
        basin_centroids: DataFrame with basin IDs and centroid coordinates
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        variables: List of weather variables to fetch
        id_column: Name of the basin ID column
        lon_column: Name of the longitude column
        lat_column: Name of the latitude column

    Returns:
        Polars DataFrame with site_id, datetime, and meteorological variables
    """
    if variables is None:
        variables = ["prcp", "temp", "humidity", "wind_speed", "wind_direction"]

    variables = list(variables)

    # Build coordinate list with IDs
    coords_with_ids = [
        (row[lon_column], row[lat_column], row[id_column])
        for row in basin_centroids.iter_rows(named=True)
    ]

    coordinates = [(lon, lat) for lon, lat, _ in coords_with_ids]
    id_lookup = {(lon, lat): site_id for lon, lat, site_id in coords_with_ids}

    # Fetch data
    df = fetch_weather_forcing(coordinates, start_date, end_date, variables)

    if df.is_empty():
        schema = {"site_id": pl.Utf8, "datetime": pl.Datetime("us")}
        for var in variables:
            schema[var] = pl.Float64
        return pl.DataFrame(schema=schema)

    # Map coordinates back to site IDs
    df = df.with_columns(
        pl.struct(["longitude", "latitude"])
        .map_elements(
            lambda s: id_lookup.get((s["longitude"], s["latitude"]), None),
            return_dtype=pl.Utf8,
        )
        .alias("site_id")
    )

    # Reorder columns
    keep_cols = ["site_id", "datetime"] + [v for v in variables if v in df.columns]
    return df.select([c for c in keep_cols if c in df.columns])
