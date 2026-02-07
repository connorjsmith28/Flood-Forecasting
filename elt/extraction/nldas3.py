"""NLDAS-2 V2.0 forcing data extraction via NASA earthaccess.

Downloads hourly NLDAS-2 forcing files from GES DISC and computes
watershed-averaged values using pre-computed grid weights. Matches
CAMELS-H methodology for forcing data.

Data source: NLDAS_FORA0125_H V2.0 (GES DISC, 1979-present)
Auth: Earthdata Login + .netrc + GES DISC EULA
"""

import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import earthaccess
import numpy as np
import pandas as pd
import polars as pl
import xarray as xr

from elt.extraction.watershed_mapping import MISSOURI_BASIN_BBOX

logger = logging.getLogger(__name__)

# Variable name mapping: NLDAS-2 internal -> output column name
VARIABLE_MAPPING = {
    "Tair": "air_temp_k",
    "Qair": "specific_humidity_kgkg",
    "PSurf": "surface_pressure_pa",
    "Wind_E": "wind_u_ms",
    "Wind_N": "wind_v_ms",
    "SWdown": "shortwave_radiation_wm2",
    "LWdown": "longwave_radiation_wm2",
    "Rainf": "precipitation_mm",
    "CRainf_frac": "convective_precip_fraction",
    "CAPE": "cape_jkg",
    "PotEvap": "potential_evaporation_mm",
}


def open_nldas2_file(
    file_path: str | Path,
    variables: list[str] | None = None,
    log: Callable[[str], None] | None = None,
) -> xr.Dataset | None:
    """Open a local NLDAS-2 NetCDF file and clip to Missouri Basin.

    Args:
        file_path: Path to a downloaded NLDAS-2 NetCDF file
        variables: Variables to load (None = all CAMELS-H variables)
        log: Optional logging function

    Returns:
        xarray Dataset clipped to Missouri Basin, or None on error
    """
    def _log(msg: str) -> None:
        if log is not None:
            log(msg)
        else:
            logger.info("%s", msg)

    file_path = Path(file_path)

    try:
        ds = xr.open_dataset(file_path, engine="netcdf4")
    except Exception as e:
        _log(f"Error reading {file_path}: {e}")
        return None

    # Select only requested variables
    if variables is not None:
        available_vars = [v for v in variables if v in ds.data_vars]
        if not available_vars:
            _log(f"No requested variables found in {file_path}")
            ds.close()
            return None
        ds = ds[available_vars]

    # Clip to Missouri Basin bounding box
    bbox = MISSOURI_BASIN_BBOX
    ds = ds.sel(
        lon=slice(bbox["min_lon"], bbox["max_lon"]),
        lat=slice(bbox["min_lat"], bbox["max_lat"]),
    )

    return ds


def aggregate_to_watersheds(
    ds: xr.Dataset,
    watershed_mapping: pd.DataFrame,
    variables: list[str] | None = None,
) -> pd.DataFrame:
    """Compute watershed-averaged values from gridded NLDAS data.

    Uses pre-computed area weights to compute weighted averages for each watershed.

    Args:
        ds: xarray Dataset with NLDAS data (clipped to Missouri Basin)
        watershed_mapping: DataFrame with columns: site_id, grid_row, grid_col, area_weight
        variables: Variables to aggregate (None = all in dataset)

    Returns:
        DataFrame with columns: site_id, datetime, and one column per variable
    """
    if variables is None:
        variables = list(ds.data_vars)

    # Get the time coordinate (use .item() to extract scalar from 0-d or 1-element array)
    time_val = pd.Timestamp(ds.time.values.item())

    # Get lat/lon arrays from dataset
    ds_lats = ds.lat.values
    ds_lons = ds.lon.values

    # Build lookup for grid cell indices to dataset indices
    lat_to_idx = {lat: i for i, lat in enumerate(ds_lats)}
    lon_to_idx = {lon: i for i, lon in enumerate(ds_lons)}

    # Pre-extract variable data as numpy arrays
    var_data = {}
    for var in variables:
        if var in ds.data_vars:
            # Squeeze out time dimension if present
            data = ds[var].values
            if data.ndim == 3:
                data = data[0]  # Remove time dimension
            var_data[var] = data

    if not var_data:
        return pd.DataFrame()

    # Compute weighted averages per site
    results = []
    for site_id, group in watershed_mapping.groupby("site_id"):
        site_values = {"site_id": site_id, "datetime": time_val}

        for var, data in var_data.items():
            weighted_sum = 0.0
            total_weight = 0.0

            for _, row in group.iterrows():
                from elt.extraction.watershed_mapping import (
                    NLDAS3_ORIGIN_LAT,
                    NLDAS3_ORIGIN_LON,
                    NLDAS3_RESOLUTION,
                )
                cell_lat = NLDAS3_ORIGIN_LAT + row["grid_row"] * NLDAS3_RESOLUTION
                cell_lon = NLDAS3_ORIGIN_LON + row["grid_col"] * NLDAS3_RESOLUTION

                # Find closest indices in the clipped dataset
                lat_idx = lat_to_idx.get(cell_lat)
                lon_idx = lon_to_idx.get(cell_lon)

                if lat_idx is None or lon_idx is None:
                    continue

                try:
                    value = data[lat_idx, lon_idx]
                    if not np.isnan(value):
                        weighted_sum += value * row["area_weight"]
                        total_weight += row["area_weight"]
                except IndexError:
                    continue

            # Normalize by total weight (in case some cells were outside region)
            if total_weight > 0:
                site_values[VARIABLE_MAPPING.get(var, var)] = weighted_sum / total_weight
            else:
                site_values[VARIABLE_MAPPING.get(var, var)] = np.nan

        results.append(site_values)

    return pd.DataFrame(results)


def fetch_nldas3_forcing(
    start_date: datetime,
    end_date: datetime,
    watershed_mapping: pd.DataFrame,
    cache_dir: str | Path,
    variables: list[str] | None = None,
    max_workers: int = 5,
    log: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """Fetch NLDAS-2 forcing data for a date range and compute watershed averages.

    Uses earthaccess to search and download NLDAS-2 V2.0 granules from GES DISC,
    then aggregates to watershed-averaged values using pre-computed grid weights.

    Args:
        start_date: Start of date range
        end_date: End of date range
        watershed_mapping: DataFrame with grid cell weights per watershed
        cache_dir: Local cache directory for downloaded files
        variables: NLDAS variable names to fetch
        max_workers: Unused (kept for API compatibility)
        log: Optional logging function

    Returns:
        Polars DataFrame with watershed-averaged forcing data
    """
    def _log(msg: str) -> None:
        if log is not None:
            log(msg)
        else:
            logger.info("%s", msg)

    variables = variables or list(VARIABLE_MAPPING.keys())
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Login via .netrc
    earthaccess.login(strategy="netrc")

    # Search for NLDAS-2 V2.0 granules in the date range
    _log(f"Searching NLDAS-2 granules: {start_date.date()} to {end_date.date()}")
    results = earthaccess.search_data(
        short_name="NLDAS_FORA0125_H",
        version="2.0",
        temporal=(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )
    _log(f"Found {len(results)} granules")

    if not results:
        _log("No NLDAS-2 granules found")
        return pl.DataFrame()

    # Download granules (earthaccess skips files already in cache)
    _log(f"Downloading to {cache_dir} (skips existing files)")
    downloaded_files = earthaccess.download(results, str(cache_dir))
    _log(f"Downloaded/cached {len(downloaded_files)} files")

    # Process each file
    all_results = []
    completed = 0
    for file_path in downloaded_files:
        ds = open_nldas2_file(
            file_path=file_path,
            variables=variables,
            log=log,
        )

        if ds is None:
            continue

        try:
            result = aggregate_to_watersheds(
                ds=ds,
                watershed_mapping=watershed_mapping,
                variables=variables,
            )
            if not result.empty:
                all_results.append(result)
        finally:
            ds.close()

        completed += 1
        if completed % 24 == 0:
            _log(f"Processed {completed}/{len(downloaded_files)} files")

    if not all_results:
        _log("No NLDAS-2 data retrieved")
        return pl.DataFrame()

    # Combine results
    combined = pd.concat(all_results, ignore_index=True)
    _log(f"Retrieved {len(combined)} watershed-hour records")

    # Convert to Polars
    return pl.from_pandas(combined)


def convert_units(df: pl.DataFrame) -> pl.DataFrame:
    """Convert NLDAS-2 native units to standard units.

    Conversions:
    - Temperature: K -> C

    NLDAS-2 V2.0 Rainf is already in kg/m2 (mm total per timestep) and
    PotEvap is already in kg/m2 (mm), so no rate conversion is needed.

    Args:
        df: DataFrame with raw NLDAS-2 values

    Returns:
        DataFrame with converted units
    """
    conversions = {}

    # Temperature: K -> C
    if "air_temp_k" in df.columns:
        conversions["air_temp_c"] = pl.col("air_temp_k") - 273.15

    if conversions:
        df = df.with_columns(**conversions)

        # Drop original columns that were converted
        drop_cols = []
        if "air_temp_k" in df.columns and "air_temp_c" in df.columns:
            drop_cols.append("air_temp_k")

        if drop_cols:
            df = df.drop(drop_cols)

    return df
