"""USGS NWIS streamflow data extraction.

Uses the dataretrieval package to fetch streamflow data from USGS Water Services API.
https://waterservices.usgs.gov/
"""

from datetime import datetime

import pandas as pd
from dataretrieval import nwis


def fetch_usgs_streamflow(site_ids, start_date, end_date, parameter_code="00060"):
    """Fetch streamflow data from USGS NWIS."""
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    # Fetch instantaneous values (15-minute intervals)
    df, _ = nwis.get_iv(
        sites=list(site_ids),
        parameterCd=parameter_code,
        start=start_date,
        end=end_date,
    )

    if df.empty:
        return pd.DataFrame(columns=["site_id", "datetime", "streamflow_cfs", "qualifiers"])

    df = df.reset_index()

    # NWIS returns columns like '00060' for the value and '00060_cd' for qualifiers
    rename_map = {"datetime": "datetime", "site_no": "site_id"}
    if parameter_code in df.columns:
        rename_map[parameter_code] = "streamflow_cfs"
    if f"{parameter_code}_cd" in df.columns:
        rename_map[f"{parameter_code}_cd"] = "qualifiers"

    df = df[[col for col in rename_map.keys() if col in df.columns]]
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "qualifiers" not in df.columns:
        df["qualifiers"] = None

    return df[["site_id", "datetime", "streamflow_cfs", "qualifiers"]]


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
