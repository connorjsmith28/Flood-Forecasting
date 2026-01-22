"""GAGES-II catchment attributes extraction.

Uses dataretrieval for site queries and pygeohydro for CAMELS attributes.
"""

from typing import Sequence

import polars as pl
from dataretrieval import nwis


def get_sites_in_huc(huc_code: str, site_type: str = "ST") -> pl.DataFrame:
    """Get all USGS sites within a HUC region.

    Args:
        huc_code: HUC code (2, 4, 6, or 8 digits)
        site_type: Site type code. Default "ST" for stream sites.

    Returns:
        DataFrame with site IDs and basic metadata for sites in the HUC
    """
    try:
        gdf, _ = nwis.what_sites(huc=huc_code, siteType=site_type)

        if gdf is None or gdf.empty:
            return pl.DataFrame()

        # Convert GeoDataFrame to regular DataFrame, dropping geometry
        df_pandas = gdf.drop(columns=["geometry"], errors="ignore").reset_index(drop=True)
        df = pl.from_pandas(df_pandas)

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

        # Select and rename available columns
        available_renames = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.select(list(available_renames.keys()))
        df = df.rename(available_renames)

        return df

    except Exception as e:
        print(f"Warning: Failed to get sites for HUC {huc_code}: {e}")
        return pl.DataFrame()


def get_missouri_basin_sites() -> pl.DataFrame:
    """Get USGS sites specifically in the Missouri River Basin (HUC 10).

    The Missouri River Basin is the largest tributary of the Mississippi,
    spanning over 500,000 square miles across ten U.S. states.

    Returns:
        DataFrame with USGS sites in the Missouri River Basin
    """
    return get_sites_in_huc("10")


def get_mississippi_basin_sites() -> pl.DataFrame:
    """Get USGS sites in the Mississippi River Basin.

    The Mississippi River Basin encompasses HUC regions:
    - 05: Ohio River Basin
    - 06: Tennessee River Basin
    - 07: Upper Mississippi
    - 08: Lower Mississippi
    - 10: Missouri River Basin
    - 11: Arkansas-White-Red

    Returns:
        DataFrame with all USGS sites in the Mississippi Basin
    """
    mississippi_hucs = ["05", "06", "07", "08", "10", "11"]

    all_sites = []
    for huc in mississippi_hucs:
        print(f"Fetching sites for HUC {huc}...")
        sites = get_sites_in_huc(huc)
        if not sites.is_empty():
            all_sites.append(sites)

    if not all_sites:
        return pl.DataFrame()

    return pl.concat(all_sites, how="diagonal").unique(subset=["site_id"])


def fetch_gages_attributes(
    site_ids: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Fetch GAGES-II/CAMELS attributes for sites.

    This function retrieves watershed characteristics from CAMELS via pygeohydro.
    CAMELS (Catchment Attributes and Meteorology for Large-sample Studies) provides
    671 basins with attributes covering climate, geology, hydrology, land cover,
    soils, and topography.

    Args:
        site_ids: List of USGS site IDs to fetch attributes for. If None, returns
            all available CAMELS basins.

    Returns:
        Polars DataFrame with catchment attributes for each site

    Raises:
        ImportError: If required dependencies (pygeohydro, h5py) are missing
    """
    try:
        import pygeohydro as gh
    except ImportError as e:
        raise ImportError(
            f"pygeohydro import failed: {e}. "
            "Make sure pygeohydro and h5py are installed."
        ) from e

    try:
        # pygeohydro.get_camels() returns (GeoDataFrame, xarray.Dataset)
        gages_gdf, _ = gh.get_camels()
    except ModuleNotFoundError as e:
        # h5py is required to read CAMELS HDF5 data
        raise ImportError(
            f"Missing dependency for CAMELS data: {e}. "
            "Install h5py with: pip install h5py"
        ) from e

    if gages_gdf is None or gages_gdf.empty:
        print("Warning: CAMELS data not available, using NWIS fallback")
        return _fetch_from_nwis(site_ids)

    # Drop geometry column and convert to polars
    df_pandas = gages_gdf.drop(columns=["geometry"], errors="ignore").reset_index()
    df = pl.from_pandas(df_pandas)

    # Filter by site_ids if provided
    if site_ids is not None:
        site_id_col = _find_site_id_column(df)
        if site_id_col:
            df = df.filter(pl.col(site_id_col).is_in(list(site_ids)))

    return df


def _find_site_id_column(df: pl.DataFrame) -> str | None:
    """Find the site ID column in the DataFrame."""
    candidates = ["site_no", "site_id", "STAID", "gage_id", "gauge_id", "index"]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _fetch_from_nwis(site_ids: Sequence[str] | None) -> pl.DataFrame:
    """Fallback method using NWIS site info."""
    if site_ids is None or len(site_ids) == 0:
        return pl.DataFrame()

    try:
        df_pandas, _ = nwis.get_info(sites=list(site_ids))

        if df_pandas is None or df_pandas.empty:
            return pl.DataFrame()

        return pl.from_pandas(df_pandas.reset_index())

    except Exception as e:
        print(f"Warning: NWIS fallback also failed: {e}")
        return pl.DataFrame()
