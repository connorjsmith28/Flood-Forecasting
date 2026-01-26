"""USGS site metadata extraction asset."""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)
import pandas as pd

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

    Queries USGS NWIS for all stream sites in the configured HUC region,
    then fetches detailed metadata (coordinates, drainage area, etc.).
    """
    from elt.extraction.usgs import get_sites_by_huc, get_site_info

    # Ensure schema exists
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    context.log.info(f"Fetching sites in HUC {config.huc_code}...")

    # Get all sites in the HUC region
    sites_df = get_sites_by_huc(config.huc_code)

    if sites_df.empty:
        raise RuntimeError(f"No sites found in HUC {config.huc_code}")

    site_ids = sites_df["site_id"].tolist()
    context.log.info(f"Found {len(site_ids)} sites in HUC {config.huc_code}")

    # Apply sample mode limit
    if config.sample_mode:
        site_ids = site_ids[: config.max_sites]
        context.log.info(f"Sample mode: limiting to {len(site_ids)} sites")

    # Fetch detailed metadata in batches
    batch_size = 100
    all_metadata = []

    for i in range(0, len(site_ids), batch_size):
        batch = site_ids[i : i + batch_size]
        context.log.info(f"Fetching metadata batch {i // batch_size + 1}...")

        try:
            df = get_site_info(batch)
            if not df.empty:
                all_metadata.append(df)
        except Exception as e:
            context.log.warning(f"Failed to fetch batch: {e}")
            continue

    if not all_metadata:
        raise RuntimeError(
            f"Failed to fetch metadata for any sites in HUC {config.huc_code}"
        )

    df = pd.concat(all_metadata, ignore_index=True)

    # Store in DuckDB (full replace - this is reference data)
    with duckdb.get_connection() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{TBL_SITE_METADATA}")
        conn.execute(
            f"CREATE TABLE {RAW_SCHEMA}.{TBL_SITE_METADATA} AS SELECT * FROM df"
        )

    context.log.info(f"Stored metadata for {len(df)} sites")

    return MaterializeResult(
        metadata={
            "num_sites": len(df),
            "huc_code": config.huc_code,
            "sample_mode": config.sample_mode,
            "columns": MetadataValue.json(list(df.columns)),
        },
    )
