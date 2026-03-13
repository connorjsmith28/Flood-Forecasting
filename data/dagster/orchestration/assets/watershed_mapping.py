"""Site-to-NLDAS3-grid mapping asset.

Maps each site to its nearest NLDAS grid cell using site coordinates.
Future work will add proper watershed boundary delineation for
area-weighted grid averaging (see README for details).
"""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from orchestration.configs import NLDAS3Config
from orchestration.resources import DuckDBResource

# Schema and table names
RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"
TBL_WATERSHED_MAPPING = "nldas3_watershed_mapping"


@asset(
    group_name="extraction",
    description="NLDAS-3 grid cell mapping for forcing data extraction",
    compute_kind="python",
    deps=["usgs_site_metadata"],
)
def nldas3_watershed_mapping(
    context: AssetExecutionContext,
    config: NLDAS3Config,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Map each site to its nearest NLDAS grid cell.

    Reads site coordinates from site_metadata and computes the nearest
    NLDAS 0.125-degree grid cell for each site (weight=1.0).

    Results are stored in raw.nldas3_watershed_mapping.
    """
    from datetime import datetime

    from elt.extraction.watershed_mapping import generate_point_weights

    # Get site coordinates
    with duckdb.get_connection() as conn:
        query = (
            f"SELECT DISTINCT site_id, longitude, latitude "
            f"FROM {RAW_SCHEMA}.{TBL_SITE_METADATA} "
            f"WHERE longitude IS NOT NULL AND latitude IS NOT NULL"
        )
        if config.sample_mode:
            query += f" LIMIT {config.max_sites}"
        result = conn.execute(query).fetchall()

    if not result:
        context.log.warning("No sites found in site_metadata")
        return MaterializeResult(metadata={"num_sites": 0, "status": "no_sites"})

    site_coords = [(row[0], row[1], row[2]) for row in result]

    # Skip if mapping already covers all requested sites
    with duckdb.get_connection() as conn:
        try:
            existing = conn.execute(
                f"SELECT COUNT(DISTINCT site_id) FROM {RAW_SCHEMA}.{TBL_WATERSHED_MAPPING}"
            ).fetchone()[0]
        except Exception:
            existing = 0

    if existing >= len(site_coords):
        context.log.info(f"Grid mapping already has {existing} sites, skipping")
        return MaterializeResult(
            metadata={
                "num_sites": existing,
                "status": "skipped (already complete)",
                "sites_requested": MetadataValue.int(len(site_coords)),
            },
        )

    # Map each site to its nearest NLDAS grid cell
    weights_df = generate_point_weights(
        site_coords=site_coords,
        log=context.log.info,
    )
    weights_df["computed_at"] = datetime.now()

    # Store in DuckDB
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        conn.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{TBL_WATERSHED_MAPPING}")
        conn.execute(
            f"CREATE TABLE {RAW_SCHEMA}.{TBL_WATERSHED_MAPPING} AS SELECT * FROM weights_df"
        )

    num_sites = weights_df["site_id"].nunique()
    context.log.info(f"Mapped {num_sites} sites to NLDAS grid cells")

    return MaterializeResult(
        metadata={
            "num_sites": num_sites,
            "sites_requested": MetadataValue.int(len(site_coords)),
            "sites_matched": MetadataValue.int(num_sites),
            "sample_mode": config.sample_mode,
        },
    )
