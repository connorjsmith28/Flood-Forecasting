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
from orchestration.utils import RAW_SCHEMA, TBL_SITE_METADATA


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

    context.log.info(f"Fetching sites in HUC {config.huc_code}...")

    # Get all sites in the HUC region
    sites_df = get_sites_by_huc(config.huc_code)

    if sites_df.empty:
        context.log.warning(f"No sites found in HUC {config.huc_code}")
        return MaterializeResult(metadata={"num_sites": 0, "status": "no_sites"})

    site_ids = sites_df["site_id"].tolist()
    context.log.info(f"Found {len(site_ids)} sites in HUC {config.huc_code}")


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
        return MaterializeResult(metadata={"num_sites": 0, "status": "fetch_failed"})

    df = pd.concat(all_metadata, ignore_index=True)

    # Store in DuckDB (full replace - this is reference data)
    duckdb.write_dataframe(df, TBL_SITE_METADATA, schema=RAW_SCHEMA, replace=True)

    context.log.info(f"Stored metadata for {len(df)} sites")

    return MaterializeResult(
        metadata={
            "num_sites": len(df),
            "huc_code": config.huc_code,
            "sample_mode": config.sample_mode,
            "columns": MetadataValue.json(list(df.columns)),
        },
    )
