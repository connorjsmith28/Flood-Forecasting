"""NLDAS-2 meteorological forcing data extraction.

Uses pynldas2 from the HyRiver stack to fetch hourly meteorological data.
https://ldas.gsfc.nasa.gov/nldas/v2/forcing
"""

from datetime import datetime
from typing import Sequence

import polars as pl

try:
    import pynldas2 as nldas
except ImportError:
    nldas = None


# NLDAS-2 variable names and descriptions
NLDAS_VARIABLES = {
    "prcp": "Precipitation hourly total (kg/m^2)",
    "temp": "2-m above ground temperature (K)",
    "wind_u": "10-m above ground zonal wind (m/s)",
    "wind_v": "10-m above ground meridional wind (m/s)",
    "rlds": "Surface downward longwave radiation (W/m^2)",
    "rsds": "Surface downward shortwave radiation (W/m^2)",
    "humidity": "2-m above ground specific humidity (kg/kg)",
    "psurf": "Surface pressure (Pa)",
    "pet": "Potential evaporation (kg/m^2)",
    "convfrac": "Fraction of total precipitation that is convective",
    "cape": "Convective available potential energy (J/kg)",
}


def fetch_nldas_forcing(
    coordinates: Sequence[tuple[float, float]],
    start_date: str | datetime,
    end_date: str | datetime,
    variables: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Fetch NLDAS-2 hourly forcing data for given coordinates.

    Args:
        coordinates: List of (longitude, latitude) tuples
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        variables: List of variables to fetch. If None, fetches all available.
            Options: prcp, temp, wind_u, wind_v, rlds, rsds, humidity, psurf, pet

    Returns:
        Polars DataFrame with columns:
            - longitude, latitude: Coordinates
            - datetime: Timestamp
            - One column per requested variable
    """
    if nldas is None:
        raise ImportError(
            "pynldas2 is required for NLDAS data extraction. "
            "Install with: pip install pynldas2"
        )

    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    if variables is None:
        variables = ["prcp", "temp", "humidity", "wind_u", "wind_v", "rsds", "rlds"]

    all_data = []

    for lon, lat in coordinates:
        try:
            # pynldas2 returns xarray Dataset
            ds = nldas.get_bycoords(
                (lon, lat),
                start_date,
                end_date,
                variables=list(variables),
            )

            # Convert xarray to pandas then to polars
            df_pandas = ds.to_dataframe().reset_index()
            df = pl.from_pandas(df_pandas)

            # Add coordinate columns
            df = df.with_columns(
                pl.lit(lon).alias("longitude"),
                pl.lit(lat).alias("latitude"),
            )

            all_data.append(df)

        except Exception as e:
            # Log warning but continue with other coordinates
            print(f"Warning: Failed to fetch NLDAS data for ({lon}, {lat}): {e}")
            continue

    if not all_data:
        # Return empty DataFrame with expected schema
        schema = {
            "longitude": pl.Float64,
            "latitude": pl.Float64,
            "time": pl.Datetime("us", "UTC"),
        }
        for var in variables:
            schema[var] = pl.Float64
        return pl.DataFrame(schema=schema)

    result = pl.concat(all_data, how="diagonal")

    # Standardize datetime column name
    if "time" in result.columns:
        result = result.rename({"time": "datetime"})

    # Reorder columns
    ordered_cols = ["longitude", "latitude", "datetime"] + [
        c for c in result.columns if c not in ["longitude", "latitude", "datetime"]
    ]
    return result.select([c for c in ordered_cols if c in result.columns])


def fetch_nldas_for_basins(
    basin_centroids: pl.DataFrame,
    start_date: str | datetime,
    end_date: str | datetime,
    variables: Sequence[str] | None = None,
    id_column: str = "site_id",
    lon_column: str = "longitude",
    lat_column: str = "latitude",
) -> pl.DataFrame:
    """Fetch NLDAS-2 data for basin centroids, preserving basin IDs.

    Args:
        basin_centroids: DataFrame with basin IDs and centroid coordinates
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        variables: List of NLDAS variables to fetch
        id_column: Name of the basin ID column
        lon_column: Name of the longitude column
        lat_column: Name of the latitude column

    Returns:
        Polars DataFrame with basin_id, datetime, and meteorological variables
    """
    if nldas is None:
        raise ImportError(
            "pynldas2 is required for NLDAS data extraction. "
            "Install with: pip install pynldas2"
        )

    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    if variables is None:
        variables = ["prcp", "temp", "humidity", "wind_u", "wind_v", "rsds", "rlds"]

    all_data = []

    for row in basin_centroids.iter_rows(named=True):
        basin_id = row[id_column]
        lon = row[lon_column]
        lat = row[lat_column]

        try:
            ds = nldas.get_bycoords(
                (lon, lat),
                start_date,
                end_date,
                variables=list(variables),
            )

            df_pandas = ds.to_dataframe().reset_index()
            df = pl.from_pandas(df_pandas)

            df = df.with_columns(pl.lit(basin_id).alias("site_id"))
            all_data.append(df)

        except Exception as e:
            print(f"Warning: Failed to fetch NLDAS data for basin {basin_id}: {e}")
            continue

    if not all_data:
        schema = {"site_id": pl.Utf8, "time": pl.Datetime("us", "UTC")}
        for var in variables:
            schema[var] = pl.Float64
        return pl.DataFrame(schema=schema)

    result = pl.concat(all_data, how="diagonal")

    if "time" in result.columns:
        result = result.rename({"time": "datetime"})

    ordered_cols = ["site_id", "datetime"] + [
        c for c in result.columns if c not in ["site_id", "datetime"]
    ]
    return result.select([c for c in ordered_cols if c in result.columns])
