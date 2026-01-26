"""USGS NWIS streamflow data extraction.

Uses the dataretrieval package to fetch streamflow data from USGS Water Services API.
https://waterservices.usgs.gov/
"""

import time
from datetime import datetime
from functools import wraps

import pandas as pd
from dataretrieval import nwis


def retry_on_network_error(max_retries: int = 3, base_delay: float = 2.0):
    """Decorator to retry functions on network/SSL errors with exponential backoff."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    is_retryable = any(
                        term in error_str
                        for term in ["ssl", "connection", "timeout", "max retries"]
                    )
                    if not is_retryable or attempt == max_retries - 1:
                        raise
                    last_exception = e
                    delay = base_delay * (2**attempt)
                    time.sleep(delay)
            raise last_exception

        return wrapper

    return decorator


@retry_on_network_error(max_retries=3, base_delay=2.0)
def get_sites_by_huc(huc_code: str, site_type: str = "ST") -> pd.DataFrame:
    """Get all USGS sites within a HUC region.

    Args:
        huc_code: HUC code to filter by (e.g., "10" for Missouri River Basin)
        site_type: USGS site type code (default "ST" for streams)

    Returns:
        DataFrame with site_id, station_name, latitude, longitude, huc_code
    """
    gdf, _ = nwis.what_sites(huc=huc_code, siteType=site_type)

    if gdf is None or gdf.empty:
        return pd.DataFrame()

    df = gdf.reset_index(drop=True)

    rename_map = {
        "site_no": "site_id",
        "station_nm": "station_name",
        "dec_lat_va": "latitude",
        "dec_long_va": "longitude",
        "huc_cd": "huc_code",
    }

    cols_to_keep = [k for k in rename_map.keys() if k in df.columns]
    df = df[cols_to_keep]
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    return df


# USGS parameter codes
PARAM_STREAMFLOW = "00060"  # Discharge (cfs)
PARAM_GAGE_HEIGHT = "00065"  # Gage height (ft)


@retry_on_network_error(max_retries=3, base_delay=2.0)
def fetch_usgs_streamflow(
    site_ids, start_date, end_date, parameter_codes=None, include_gage_height=True
):
    """Fetch streamflow and gage height data from USGS NWIS.

    Args:
        site_ids: List of USGS site IDs
        start_date: Start date (datetime or string)
        end_date: End date (datetime or string)
        parameter_codes: List of parameter codes to fetch. If None, uses defaults.
        include_gage_height: If True and parameter_codes is None, fetch both
            streamflow (00060) and gage height (00065). Set False for legacy behavior.

    Returns:
        DataFrame with site_id, datetime, streamflow_cfs, gage_height_ft, qualifiers
    """
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    # Determine which parameters to fetch
    if parameter_codes is None:
        if include_gage_height:
            parameter_codes = [PARAM_STREAMFLOW, PARAM_GAGE_HEIGHT]
        else:
            parameter_codes = [PARAM_STREAMFLOW]

    # Fetch instantaneous values (15-minute intervals)
    df, _ = nwis.get_iv(
        sites=list(site_ids),
        parameterCd=parameter_codes,
        start=start_date,
        end=end_date,
    )

    output_cols = ["site_id", "datetime", "streamflow_cfs", "gage_height_ft", "qualifiers"]

    if df.empty:
        return pd.DataFrame(columns=output_cols)

    df = df.reset_index()

    # NWIS returns columns like '00060' for the value and '00060_cd' for qualifiers
    rename_map = {"datetime": "datetime", "site_no": "site_id"}

    # Map parameter codes to friendly column names
    param_col_map = {
        PARAM_STREAMFLOW: "streamflow_cfs",
        PARAM_GAGE_HEIGHT: "gage_height_ft",
    }

    for param_code, col_name in param_col_map.items():
        if param_code in df.columns:
            rename_map[param_code] = col_name
        if f"{param_code}_cd" in df.columns:
            rename_map[f"{param_code}_cd"] = f"{col_name}_qualifier"

    df = df[[col for col in rename_map.keys() if col in df.columns]]
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Combine qualifier columns into single column if both exist
    qualifier_cols = [c for c in df.columns if c.endswith("_qualifier")]
    if qualifier_cols:
        df["qualifiers"] = df[qualifier_cols].apply(
            lambda row: "|".join(str(v) for v in row if pd.notna(v)), axis=1
        )
        df = df.drop(columns=qualifier_cols)
    else:
        df["qualifiers"] = None

    # Ensure all output columns exist
    for col in output_cols:
        if col not in df.columns:
            df[col] = None

    return df[output_cols]


@retry_on_network_error(max_retries=3, base_delay=2.0)
def get_site_info(site_ids):
    """Get metadata for USGS sites."""
    gdf, _ = nwis.get_info(sites=list(site_ids))

    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT string
    if "geometry" in gdf.columns and gdf.geometry is not None:
        gdf = gdf.copy()
        gdf["geometry_wkt"] = gdf.geometry.to_wkt()

    # Drop the native geometry column
    df = gdf.drop(columns=["geometry"], errors="ignore")

    # Select key columns if they exist
    key_columns = [
        "site_no", "station_nm", "dec_lat_va", "dec_long_va",
        "drain_area_va", "huc_cd", "state_cd", "county_cd", "geometry_wkt",
    ]
    df = df[[c for c in key_columns if c in df.columns]]

    rename_map = {
        "site_no": "site_id",
        "station_nm": "station_name",
        "dec_lat_va": "latitude",
        "dec_long_va": "longitude",
        "drain_area_va": "drainage_area_sq_mi",
        "huc_cd": "huc_code",
        "state_cd": "state_code",
        "county_cd": "county_code",
        "geometry_wkt": "geometry",
    }

    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
