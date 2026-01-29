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
    """Extract USGS site metadata for a HUC region."""
    from elt.extraction.usgs import get_site_metadata

    # Ensure schema exists
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    max_sites = config.max_sites if config.sample_mode else None
    context.log.info(f"Fetching sites in HUC {config.huc_code}..." + (f" (limit {max_sites})" if max_sites else ""))

    df = get_site_metadata(config.huc_code, max_sites=max_sites)

    if df.empty:
        raise RuntimeError(f"No sites found in HUC {config.huc_code}")

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
