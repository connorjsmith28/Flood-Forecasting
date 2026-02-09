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
from orchestration.configs import StreamflowConfig
from orchestration.resources import DuckDBResource
from orchestration.utils.time_windows import generate_time_windows
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
    parallel_fetch_fn_name: str  # parallel version function name
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

        Supports incremental loading with time-window batching:
        - Fetches data in yearly chunks for memory efficiency
        - Uses parallel requests within each time window
        - Writes incrementally after each time window (checkpoint)
        - Respects min_date to align with USGS IV data availability
        """
        from elt.extraction import usgs

        parallel_fetch_fn: Callable = getattr(usgs, spec.parallel_fetch_fn_name)

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
        if end_date.tzinfo is not None:
            end_date = end_date.replace(tzinfo=None)
        watermark = get_high_watermark(duckdb, spec.table_name, spec.time_column)
        if watermark is not None and watermark.tzinfo is not None:
            watermark = watermark.replace(tzinfo=None)

        # Parse min_date from config
        min_date = datetime.strptime(config.min_date, "%Y-%m-%d")

        if watermark:
            start_date = watermark - timedelta(days=config.incremental_days)
            context.log.info(
                f"Incremental load: watermark={watermark}, "
                f"fetching from {start_date.date()}"
            )
        else:
            # Initial load: go back days_back but not before min_date
            start_date = max(
                end_date - timedelta(days=config.days_back),
                min_date,
            )
            context.log.info(
                f"Initial load: fetching from {start_date.date()} "
                f"(min_date={config.min_date}, days_back={config.days_back})"
            )

        # Generate time windows for batched extraction
        windows = generate_time_windows(start_date, end_date, config.time_window_days)
        context.log.info(
            f"Fetching {spec.name} for {len(site_ids)} sites "
            f"in {len(windows)} time windows "
            f"({start_date.date()} to {end_date.date()})"
        )

        total_fetched = 0
        total_inserted = 0

        for window_idx, (window_start, window_end) in enumerate(windows):
            context.log.info(
                f"Time window {window_idx + 1}/{len(windows)}: "
                f"{window_start.date()} to {window_end.date()}"
            )

            try:
                df = parallel_fetch_fn(
                    site_ids=site_ids,
                    start_date=window_start,
                    end_date=window_end,
                    batch_size=spec.batch_size,
                    max_workers=config.parallel_fetches,
                    log=context.log.info,
                )
            except Exception as e:
                context.log.warning(f"Failed to fetch window {window_idx + 1}: {e}")
                continue

            if df.empty:
                context.log.warning(f"No data for window {window_idx + 1}")
                continue

            df["extracted_at"] = datetime.now()

            new_records = upsert_timeseries(
                duckdb, df, spec.table_name, key_columns=["site_id", spec.time_column]
            )

            total_fetched += len(df)
            total_inserted += new_records
            context.log.info(
                f"Window {window_idx + 1}/{len(windows)} complete: "
                f"fetched {len(df)}, inserted {new_records} "
                f"(total: {total_fetched} fetched, {total_inserted} inserted)"
            )

        if total_fetched == 0:
            return MaterializeResult(
                metadata={"num_records": 0, "status": "fetch_failed"}
            )

        return MaterializeResult(
            metadata={
                "records_fetched": total_fetched,
                "records_inserted": total_inserted,
                "num_sites": len(site_ids),
                "num_time_windows": len(windows),
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
        parallel_fetch_fn_name="fetch_usgs_streamflow_parallel",
        description="Raw USGS streamflow observations at 15-minute intervals (incremental, parallel)",
    )
)

usgs_streamflow_daily = build_usgs_streamflow_asset(
    StreamflowAssetSpec(
        name="usgs_streamflow_daily",
        table_name="streamflow_daily",
        time_column="date",
        batch_size=50,
        fetch_fn_name="fetch_usgs_daily",
        parallel_fetch_fn_name="fetch_usgs_daily_parallel",
        description="Raw USGS daily streamflow values (incremental, parallel)",
    )
)
