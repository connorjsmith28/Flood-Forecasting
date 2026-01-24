"""GAGES-II catchment attributes extraction.

Uses dataretrieval for site queries and pygeohydro for CAMELS attributes.
"""

import pandas as pd
import pygeohydro as gh
from dataretrieval import nwis


def get_sites_in_huc(huc_code, site_type="ST"):
    """Get all USGS sites within a HUC region."""
    gdf, _ = nwis.what_sites(huc=huc_code, siteType=site_type)

    if gdf is None or gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT string for DuckDB storage
    if "geometry" in gdf.columns and gdf.geometry is not None:
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.to_wkt()

    df = gdf.reset_index(drop=True)

    # Standardize column names
    rename_map = {
        "site_no": "site_id",
        "station_nm": "station_name",
        "dec_lat_va": "latitude",
        "dec_long_va": "longitude",
        "drain_area_va": "drainage_area_sq_mi",
        "huc_cd": "huc_code",
        "state_cd": "state_code",
        "county_cd": "county_code",
        "alt_va": "altitude",
    }

    # Select and rename available columns, keeping geometry
    cols_to_keep = [k for k in rename_map.keys() if k in df.columns]
    if "geometry" in df.columns:
        cols_to_keep.append("geometry")
    df = df[cols_to_keep]
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    return df


def fetch_gages_attributes(site_ids=None):
    """Fetch GAGES-II/CAMELS attributes for sites."""
    # pygeohydro.get_camels() returns (GeoDataFrame, xarray.Dataset)
    gages_gdf, _ = gh.get_camels()

    if gages_gdf is None or gages_gdf.empty:
        return pd.DataFrame()

    # Convert geometry to WKT string for DuckDB storage
    if "geometry" in gages_gdf.columns and gages_gdf.geometry is not None:
        gages_gdf = gages_gdf.copy()
        gages_gdf["geometry"] = gages_gdf.geometry.to_wkt()

    df = gages_gdf.reset_index()

    # Filter by site_ids if provided
    if site_ids is not None:
        for col in ["site_no", "site_id", "STAID", "gage_id", "gauge_id", "index"]:
            if col in df.columns:
                df = df[df[col].isin(list(site_ids))]
                break

    return df
