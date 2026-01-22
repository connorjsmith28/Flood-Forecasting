"""USGS NWIS streamflow data extraction.

Uses the dataretrieval package to fetch streamflow data from USGS Water Services API.
https://waterservices.usgs.gov/
"""

from datetime import datetime
from typing import Sequence

import polars as pl
from dataretrieval import nwis


def fetch_usgs_streamflow(
    site_ids: Sequence[str],
    start_date: str | datetime,
    end_date: str | datetime,
    parameter_code: str = "00060",  # Discharge in cubic feet per second
) -> pl.DataFrame:
    """Fetch streamflow data from USGS NWIS.

    Args:
        site_ids: List of USGS site IDs (e.g., ["01646500", "01638500"])
        start_date: Start date for data retrieval (YYYY-MM-DD or datetime)
        end_date: End date for data retrieval (YYYY-MM-DD or datetime)
        parameter_code: USGS parameter code. Default "00060" is discharge (cfs).
            Other common codes:
            - "00065": Gage height (feet)
            - "00010": Water temperature (Celsius)

    Returns:
        Polars DataFrame with columns:
            - site_id: USGS site identifier
            - datetime: Timestamp of observation
            - streamflow_cfs: Discharge in cubic feet per second
            - qualifiers: Data quality flags
    """
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    # Fetch instantaneous values (15-minute intervals)
    df_pandas, metadata = nwis.get_iv(
        sites=list(site_ids),
        parameterCd=parameter_code,
        start=start_date,
        end=end_date,
    )

    if df_pandas.empty:
        return pl.DataFrame(
            schema={
                "site_id": pl.Utf8,
                "datetime": pl.Datetime("us", "UTC"),
                "streamflow_cfs": pl.Float64,
                "qualifiers": pl.Utf8,
            }
        )

    # Reset index to get datetime as column
    df_pandas = df_pandas.reset_index()

    # Build column mapping based on what's returned
    # NWIS returns columns like '00060' for the value and '00060_cd' for qualifiers
    value_col = f"{parameter_code}"
    qualifier_col = f"{parameter_code}_cd"

    # Convert to Polars
    df = pl.from_pandas(df_pandas)

    # Standardize column names
    rename_map = {
        "datetime": "datetime",
        "site_no": "site_id",
    }

    if value_col in df.columns:
        rename_map[value_col] = "streamflow_cfs"
    if qualifier_col in df.columns:
        rename_map[qualifier_col] = "qualifiers"

    df = df.select([col for col in rename_map.keys() if col in df.columns])
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure qualifiers column exists
    if "qualifiers" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("qualifiers"))

    return df.select(["site_id", "datetime", "streamflow_cfs", "qualifiers"])


def fetch_usgs_daily_streamflow(
    site_ids: Sequence[str],
    start_date: str | datetime,
    end_date: str | datetime,
    parameter_code: str = "00060",
    stat_code: str = "00003",  # Mean
) -> pl.DataFrame:
    """Fetch daily mean streamflow data from USGS NWIS.

    Args:
        site_ids: List of USGS site IDs
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        parameter_code: USGS parameter code (default: discharge)
        stat_code: Statistic code. Default "00003" is daily mean.
            - "00001": Maximum
            - "00002": Minimum
            - "00003": Mean

    Returns:
        Polars DataFrame with daily streamflow values
    """
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    df_pandas, metadata = nwis.get_dv(
        sites=list(site_ids),
        parameterCd=parameter_code,
        statCd=stat_code,
        start=start_date,
        end=end_date,
    )

    if df_pandas.empty:
        return pl.DataFrame(
            schema={
                "site_id": pl.Utf8,
                "date": pl.Date,
                "streamflow_cfs": pl.Float64,
                "qualifiers": pl.Utf8,
            }
        )

    df_pandas = df_pandas.reset_index()
    df = pl.from_pandas(df_pandas)

    # Column naming varies - find the discharge column
    value_cols = [c for c in df.columns if parameter_code in c and "_cd" not in c]
    qualifier_cols = [c for c in df.columns if f"{parameter_code}_cd" in c]

    columns_to_select = ["datetime", "site_no"]
    rename_map = {"datetime": "date", "site_no": "site_id"}

    if value_cols:
        columns_to_select.append(value_cols[0])
        rename_map[value_cols[0]] = "streamflow_cfs"

    if qualifier_cols:
        columns_to_select.append(qualifier_cols[0])
        rename_map[qualifier_cols[0]] = "qualifiers"

    df = df.select([c for c in columns_to_select if c in df.columns])
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    # Convert datetime to date
    if "date" in df.columns:
        df = df.with_columns(pl.col("date").cast(pl.Date))

    if "qualifiers" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("qualifiers"))

    return df.select(["site_id", "date", "streamflow_cfs", "qualifiers"])


def get_site_info(site_ids: Sequence[str]) -> pl.DataFrame:
    """Get metadata for USGS sites.

    Args:
        site_ids: List of USGS site IDs

    Returns:
        Polars DataFrame with site metadata including location, drainage area, etc.
        Geometry is stored as WKT (Well-Known Text) string for compatibility.
    """
    gdf, _ = nwis.get_info(sites=list(site_ids))

    if gdf is None or gdf.empty:
        return pl.DataFrame()

    # Convert geometry to WKT string before converting to polars
    # This preserves spatial data while being compatible with polars
    if "geometry" in gdf.columns and gdf.geometry is not None:
        gdf = gdf.copy()
        gdf["geometry_wkt"] = gdf.geometry.to_wkt()

    # Drop the native geometry column (polars can't handle it)
    df_pandas = gdf.drop(columns=["geometry"], errors="ignore")
    df = pl.from_pandas(df_pandas)

    # Select key columns if they exist
    key_columns = [
        "site_no",
        "station_nm",
        "dec_lat_va",
        "dec_long_va",
        "drain_area_va",
        "huc_cd",
        "state_cd",
        "county_cd",
        "geometry_wkt",
    ]

    available_cols = [c for c in key_columns if c in df.columns]
    df = df.select(available_cols)

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

    return df.rename({k: v for k, v in rename_map.items() if k in df.columns})
