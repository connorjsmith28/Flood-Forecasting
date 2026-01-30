"""USGS 15-minute streamflow extraction asset."""

from datetime import datetime, timedelta

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)
import pandas as pd

from orchestration.configs import StreamflowConfig
from orchestration.resources import DuckDBResource
from orchestration.utils.timeseries import get_high_watermark, upsert_timeseries

# Schema and table names
RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"
TBL_STREAMFLOW = "streamflow_15min"


@asset(
    group_name="extraction",
    description="Raw USGS streamflow observations at 15-minute intervals (incremental)",
    compute_kind="python",
    deps=["usgs_site_metadata"],  # Depend on site metadata for site list
)
def usgs_streamflow_15min(
    context: AssetExecutionContext,
    config: StreamflowConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract streamflow data from USGS NWIS.

    Supports incremental loading:
    - First run: loads `days_back` days of history
    - Subsequent runs: loads from last timestamp minus `incremental_days` overlap

    Uses (site_id, datetime) as the unique key to avoid duplicates.
    """
    from elt.extraction.usgs import fetch_usgs_streamflow

    # Get site IDs
    if config.site_ids:
        site_ids = config.site_ids
    else:
        with duckdb.get_connection() as conn:
            query = f"SELECT site_id FROM {RAW_SCHEMA}.{TBL_SITE_METADATA}"
            if config.sample_mode:
                query += f" LIMIT {config.max_sites}"
            result = conn.execute(query).fetchall()
        site_ids = [row[0] for row in result] if result else []

    if not site_ids:
        context.log.warning("No site IDs available")
        return MaterializeResult(metadata={"num_records": 0, "status": "no_sites"})

    # Determine date range based on watermark
    end_date = datetime.now()
    watermark = get_high_watermark(duckdb, TBL_STREAMFLOW, "datetime")

    if watermark:
        # Incremental: start from watermark minus overlap
        start_date = watermark - timedelta(days=config.incremental_days)
        context.log.info(
            f"Incremental load: watermark={watermark}, "
            f"fetching from {start_date.date()}"
        )
    else:
        # Initial load: use full history
        start_date = end_date - timedelta(days=config.days_back)
        context.log.info(f"Initial load: fetching {config.days_back} days of history")

    context.log.info(
        f"Fetching streamflow for {len(site_ids)} sites "
        f"from {start_date.date()} to {end_date.date()}"
    )

    # Fetch in batches
    batch_size = 20
    all_data = []

    for i in range(0, len(site_ids), batch_size):
        batch = site_ids[i : i + batch_size]
        context.log.info(f"Fetching streamflow batch {i // batch_size + 1}...")

        try:
            df = fetch_usgs_streamflow(
                site_ids=batch,
                start_date=start_date,
                end_date=end_date,
            )
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            context.log.warning(f"Failed to fetch streamflow batch: {e}")
            continue

    if not all_data:
        return MaterializeResult(metadata={"num_records": 0, "status": "fetch_failed"})

    df = pd.concat(all_data, ignore_index=True)

    # Add extraction timestamp
    df["extracted_at"] = datetime.now()

    # Upsert to avoid duplicates
    new_records = upsert_timeseries(
        duckdb, df, TBL_STREAMFLOW, key_columns=["site_id", "datetime"]
    )

    context.log.info(f"Inserted {new_records} new records (fetched {len(df)} total)")

    return MaterializeResult(
        metadata={
            "records_fetched": len(df),
            "records_inserted": new_records,
            "num_sites": df["site_id"].nunique(),
            "sample_mode": config.sample_mode,
            "is_incremental": watermark is not None,
            "watermark": str(watermark) if watermark else "none",
            "date_range": MetadataValue.json(
                {
                    "start": str(start_date.date()),
                    "end": str(end_date.date()),
                }
            ),
        },
    )
