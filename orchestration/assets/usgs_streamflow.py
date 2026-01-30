"""USGS streamflow extraction assets."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from dagster import (
    asset,
    AssetExecutionContext,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
)
import pandas as pd

from orchestration.configs import StreamflowConfig
from orchestration.resources import DuckDBResource
from orchestration.utils.timeseries import get_high_watermark, upsert_timeseries

RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"


@dataclass
class StreamflowAssetSpec:
    """Specification for a streamflow extraction asset."""

    name: str
    table_name: str
    time_column: str  # "datetime" or "date"
    batch_size: int
    fetch_fn_name: str  # function name in elt.extraction.usgs
    description: str


def build_usgs_streamflow_asset(spec: StreamflowAssetSpec) -> AssetsDefinition:
    """Factory to create USGS streamflow extraction assets."""

    @asset(
        name=spec.name,
        group_name="extraction",
        description=spec.description,
        compute_kind="python",
        deps=["usgs_site_metadata"],
    )
    def _asset(
        context: AssetExecutionContext,
        config: StreamflowConfig,
        duckdb: DuckDBResource,
    ) -> MaterializeResult:
        """Extract streamflow data from USGS NWIS.

        Supports incremental loading:
        - First run: loads `days_back` days of history
        - Subsequent runs: loads from last timestamp minus `incremental_days` overlap
        """
        from elt.extraction import usgs

        fetch_fn: Callable = getattr(usgs, spec.fetch_fn_name)

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
        watermark = get_high_watermark(duckdb, spec.table_name, spec.time_column)

        if watermark:
            start_date = watermark - timedelta(days=config.incremental_days)
            context.log.info(
                f"Incremental load: watermark={watermark}, "
                f"fetching from {start_date.date()}"
            )
        else:
            start_date = end_date - timedelta(days=config.days_back)
            context.log.info(f"Initial load: fetching {config.days_back} days of history")

        context.log.info(
            f"Fetching {spec.name} for {len(site_ids)} sites "
            f"from {start_date.date()} to {end_date.date()}"
        )

        # Fetch in batches
        all_data = []
        for i in range(0, len(site_ids), spec.batch_size):
            batch = site_ids[i : i + spec.batch_size]
            context.log.info(f"Fetching batch {i // spec.batch_size + 1}...")
            try:
                df = fetch_fn(
                    site_ids=batch,
                    start_date=start_date,
                    end_date=end_date,
                )
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                context.log.warning(f"Failed to fetch batch: {e}")

        if not all_data:
            return MaterializeResult(metadata={"num_records": 0, "status": "fetch_failed"})

        df = pd.concat(all_data, ignore_index=True)
        df["extracted_at"] = datetime.now()

        new_records = upsert_timeseries(
            duckdb, df, spec.table_name, key_columns=["site_id", spec.time_column]
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

    return _asset


# Create the assets
usgs_streamflow_15min = build_usgs_streamflow_asset(
    StreamflowAssetSpec(
        name="usgs_streamflow_15min",
        table_name="streamflow_15min",
        time_column="datetime",
        batch_size=20,
        fetch_fn_name="fetch_usgs_streamflow",
        description="Raw USGS streamflow observations at 15-minute intervals (incremental)",
    )
)

usgs_streamflow_daily = build_usgs_streamflow_asset(
    StreamflowAssetSpec(
        name="usgs_streamflow_daily",
        table_name="streamflow_daily",
        time_column="date",
        batch_size=50,
        fetch_fn_name="fetch_usgs_daily",
        description="Raw USGS daily streamflow values (incremental)",
    )
)
