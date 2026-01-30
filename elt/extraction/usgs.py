"""USGS NWIS streamflow data extraction. Checkout the official USGS repo for examples: https://github.com/DOI-USGS/dataretrieval-python/blob/main/dataretrieval/nwis.py"""

import pandas as pd
from dataretrieval import nwis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# Column rename mappings (USGS names → our names)
SITE_COLUMNS = {
    "site_no": "site_id",
    "station_nm": "station_name",
    "dec_lat_va": "latitude",
    "dec_long_va": "longitude",
    "huc_cd": "huc_code",
    "drain_area_va": "drainage_area_sq_mi",
    "state_cd": "state_code",
    "county_cd": "county_code",
}


def _is_network_error(exc: BaseException) -> bool:
    err = str(exc).lower()
    return any(t in err for t in ["ssl", "connection", "timeout", "max retries"])


_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=8),
    retry=retry_if_exception(_is_network_error),
)


def get_site_metadata(
    huc_code: str,
    max_sites: int | None = None,
    parameter_codes: list[str] | None = None,
    data_type: str | None = None,
) -> pd.DataFrame:
    """Discover USGS sites in a HUC region and fetch their metadata.

    Args:
        huc_code: HUC region code (e.g., "10" for Missouri Basin)
        max_sites: Maximum number of sites to return (for sampling)
        parameter_codes: Filter for sites with specific parameters (e.g., ["00060"] for discharge)
        data_type: Filter for data type availability ("iv" for instantaneous, "dv" for daily)
    """
    # Build query parameters (use siteTypeCd not siteType - API requirement)
    query_params = {"huc": huc_code, "siteTypeCd": "ST"}
    if parameter_codes:
        query_params["parameterCd"] = ",".join(parameter_codes)
    if data_type:
        query_params["hasDataTypeCd"] = data_type

    # Discover sites in the HUC region
    df, _ = nwis.what_sites(**query_params)
    if df is None or df.empty:
        return pd.DataFrame()

    site_ids = df["site_no"].tolist()
    if max_sites:
        site_ids = site_ids[:max_sites]

    # Fetch metadata in batches (API has URL length limit). This is the code sample USGS recommended using for big data pulls.
    @_retry
    def fetch_batch(ids):
        df, _ = nwis.get_info(sites=ids)
        return df

    chunks = [site_ids[i:i + 100] for i in range(0, len(site_ids), 100)]
    dfs = [fetch_batch(chunk) for chunk in chunks if chunk]
    dfs = [d for d in dfs if d is not None and not d.empty]

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True).drop(columns=["geometry"], errors="ignore")
    cols = [c for c in SITE_COLUMNS if c in df.columns]
    return df[cols].rename(columns=SITE_COLUMNS)


@_retry
def fetch_usgs_streamflow(site_ids, start_date, end_date) -> pd.DataFrame:
    """Fetch streamflow and gage height data (15-min intervals) from USGS NWIS."""
    # Convert dates to strings - dataretrieval requires YYYY-MM-DD format
    start_str = start_date.strftime("%Y-%m-%d") if hasattr(start_date, "strftime") else str(start_date)
    end_str = end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else str(end_date)

    df, _ = nwis.get_iv(
        sites=list(site_ids),
        parameterCd=["00060", "00065"],  # Discharge (cfs), Gage height (ft)
        start=start_str,
        end=end_str,
    )
    out_cols = ["site_id", "datetime", "streamflow_cfs", "gage_height_ft", "qualifiers"]
    if df.empty:
        return pd.DataFrame(columns=out_cols)

    df = df.reset_index().rename(columns={
        "site_no": "site_id",
        "00060": "streamflow_cfs",
        "00065": "gage_height_ft",
    })
    # Combine qualifier columns
    qual_cols = [c for c in df.columns if c.endswith("_cd")]
    df["qualifiers"] = df[qual_cols].apply(
        lambda r: "|".join(str(v) for v in r if pd.notna(v)), axis=1
    ) if qual_cols else None

    return df[[c for c in out_cols if c in df.columns]].reindex(columns=out_cols)


@_retry
def fetch_usgs_daily(site_ids, start_date, end_date) -> pd.DataFrame:
    """Fetch daily streamflow values from USGS NWIS.

    Returns daily mean discharge and gage height statistics.
    """
    # Convert dates to strings - dataretrieval requires YYYY-MM-DD format
    start_str = start_date.strftime("%Y-%m-%d") if hasattr(start_date, "strftime") else str(start_date)
    end_str = end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else str(end_date)

    df, _ = nwis.get_dv(
        sites=list(site_ids),
        parameterCd=["00060", "00065"],  # Discharge (cfs), Gage height (ft)
        start=start_str,
        end=end_str,
    )
    out_cols = ["site_id", "date", "streamflow_cfs_mean", "gage_height_ft_mean", "qualifiers"]
    if df.empty:
        return pd.DataFrame(columns=out_cols)

    df = df.reset_index().rename(columns={
        "site_no": "site_id",
        "datetime": "date",
        "00060_Mean": "streamflow_cfs_mean",
        "00065_Mean": "gage_height_ft_mean",
    })
    # Combine qualifier columns
    qual_cols = [c for c in df.columns if c.endswith("_cd")]
    df["qualifiers"] = df[qual_cols].apply(
        lambda r: "|".join(str(v) for v in r if pd.notna(v)), axis=1
    ) if qual_cols else None

    return df[[c for c in out_cols if c in df.columns]].reindex(columns=out_cols)


