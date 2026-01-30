"""USGS site metadata extraction asset."""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from orchestration.configs import SiteConfig
from orchestration.resources import DuckDBResource

# Schema and table names
RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"


@asset(
    group_name="extraction",
    description="USGS site metadata for sites in a HUC region",
    compute_kind="python",
)
def usgs_site_metadata(
    context: AssetExecutionContext,
    config: SiteConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract USGS site metadata for a HUC region.

    Includes has_iv and has_daily flags indicating data availability.
    """
    from elt.extraction.usgs import get_site_metadata

    # Ensure schema exists
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    max_sites = config.max_sites if config.sample_mode else None
    context.log.info(f"Fetching sites in HUC {config.huc_code}..." + (f" (limit {max_sites})" if max_sites else ""))

    # Get sites with discharge data (00060) - all sites with IV and/or daily
    context.log.info("Fetching all sites with discharge data...")
    df = get_site_metadata(
        config.huc_code,
        max_sites=max_sites,
        parameter_codes=["00060"],
    )

    if df.empty:
        raise RuntimeError(f"No sites found in HUC {config.huc_code}")

    # Get sites with IV data to create has_iv flag
    context.log.info("Identifying sites with IV data...")
    df_iv = get_site_metadata(
        config.huc_code,
        parameter_codes=["00060"],
        data_type="iv",
    )
    iv_sites = set(df_iv["site_id"]) if not df_iv.empty else set()

    # Get sites with daily data to create has_daily flag
    context.log.info("Identifying sites with daily data...")
    df_dv = get_site_metadata(
        config.huc_code,
        parameter_codes=["00060"],
        data_type="dv",
    )
    dv_sites = set(df_dv["site_id"]) if not df_dv.empty else set()

    # Add flags
    df["has_iv"] = df["site_id"].isin(iv_sites)
    df["has_daily"] = df["site_id"].isin(dv_sites)

    # Store in DuckDB (full replace - this is reference data)
    with duckdb.get_connection() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{TBL_SITE_METADATA}")
        conn.execute(
            f"CREATE TABLE {RAW_SCHEMA}.{TBL_SITE_METADATA} AS SELECT * FROM df"
        )

    num_iv = df["has_iv"].sum()
    num_daily = df["has_daily"].sum()
    context.log.info(f"Stored metadata for {len(df)} sites ({num_iv} IV, {num_daily} daily)")

    return MaterializeResult(
        metadata={
            "num_sites": len(df),
            "num_sites_iv": int(num_iv),
            "num_sites_daily": int(num_daily),
            "huc_code": config.huc_code,
            "sample_mode": config.sample_mode,
            "columns": MetadataValue.json(list(df.columns)),
        },
    )
