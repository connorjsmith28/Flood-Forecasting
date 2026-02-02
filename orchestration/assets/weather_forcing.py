"""Weather forcing data extraction asset."""

from datetime import datetime, timedelta

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from orchestration.configs import WeatherConfig
from orchestration.resources import DuckDBResource
from orchestration.utils.time_windows import generate_time_windows
from orchestration.utils.timeseries import get_high_watermark, upsert_timeseries

# Schema and table names
RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"
TBL_WEATHER = "weather_forcing"


def _ensure_weather_table(conn, variables: list[str]) -> None:
    """Create empty weather table with expected schema if it doesn't exist."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    var_cols = ",\n                ".join(f"{v} DOUBLE" for v in variables)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TBL_WEATHER} (
            longitude DOUBLE,
            latitude DOUBLE,
            datetime TIMESTAMP,
            {var_cols},
            extracted_at TIMESTAMP
        )
    """
    )


@asset(
    group_name="extraction",
    description="Raw meteorological forcing data from Open-Meteo (incremental, parallel)",
    compute_kind="python",
    deps=[
        "usgs_streamflow_15min"
    ],  # Depend on streamflow to avoid DuckDB write lock conflicts
)
def weather_forcing_raw(
    context: AssetExecutionContext,
    config: WeatherConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract hourly meteorological forcing data from Open-Meteo.

    Supports incremental loading with time-window batching:
    - Fetches data in yearly chunks for memory efficiency
    - Uses parallel requests within each time window
    - Writes incrementally after each time window (checkpoint)
    - Respects min_date to align with USGS IV data availability

    Uses (longitude, latitude, datetime) as the unique key to avoid duplicates.
    """
    from elt.extraction.weather import fetch_weather_parallel

    # Get coordinates only for sites that have streamflow data
    with duckdb.get_connection() as conn:
        query = f"""
            SELECT DISTINCT m.site_id, m.longitude, m.latitude
            FROM {RAW_SCHEMA}.{TBL_SITE_METADATA} m
            INNER JOIN {RAW_SCHEMA}.streamflow_15min s ON m.site_id = s.site_id
            WHERE m.longitude IS NOT NULL AND m.latitude IS NOT NULL
        """
        if config.sample_mode:
            query += f" LIMIT {config.max_sites}"
        result = conn.execute(query).fetchall()

    if not result:
        context.log.warning("No sites with coordinates found")
        return MaterializeResult(
            metadata={"num_records": 0, "status": "no_coordinates"}
        )

    coordinates = [(row[1], row[2]) for row in result]

    # Determine date range based on watermark
    end_date = datetime.now() - timedelta(days=1)
    watermark = get_high_watermark(duckdb, TBL_WEATHER, "datetime")

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
        f"Fetching weather forcing for {len(coordinates)} locations "
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
            df = fetch_weather_parallel(
                coordinates=coordinates,
                start_date=window_start,
                end_date=window_end,
                variables=config.variables,
                max_workers=config.parallel_fetches,
                log=context.log.info,
            )
        except Exception as e:
            context.log.error(f"Failed to fetch window {window_idx + 1}: {e}")
            # Ensure table exists so dbt doesn't fail
            with duckdb.get_connection() as conn:
                _ensure_weather_table(conn, config.variables)
            continue

        if df.is_empty():
            context.log.warning(f"No data for window {window_idx + 1}")
            continue

        # Add extraction timestamp and upsert
        df = df.with_columns(extracted_at=datetime.now())
        pdf = df.to_pandas()

        new_records = upsert_timeseries(
            duckdb, pdf, TBL_WEATHER, key_columns=["longitude", "latitude", "datetime"]
        )

        total_fetched += len(pdf)
        total_inserted += new_records
        context.log.info(
            f"Window {window_idx + 1}/{len(windows)} complete: "
            f"fetched {len(pdf)}, inserted {new_records} "
            f"(total: {total_fetched} fetched, {total_inserted} inserted)"
        )

    # Ensure table exists even if all windows failed
    if total_fetched == 0:
        context.log.warning("No weather data fetched across all windows")
        with duckdb.get_connection() as conn:
            _ensure_weather_table(conn, config.variables)
        return MaterializeResult(metadata={"num_records": 0, "status": "empty"})

    return MaterializeResult(
        metadata={
            "records_fetched": total_fetched,
            "records_inserted": total_inserted,
            "num_locations": len(coordinates),
            "num_time_windows": len(windows),
            "sample_mode": config.sample_mode,
            "is_incremental": watermark is not None,
            "watermark": str(watermark) if watermark else "none",
            "variables": MetadataValue.json(config.variables),
            "date_range": MetadataValue.json(
                {
                    "start": str(start_date.date()),
                    "end": str(end_date.date()),
                }
            ),
        },
    )
