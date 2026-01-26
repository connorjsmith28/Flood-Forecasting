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
from orchestration.utils.timeseries import get_high_watermark, upsert_timeseries

# Schema and table names
RAW_SCHEMA = "raw"
TBL_SITE_METADATA = "site_metadata"
TBL_WEATHER = "weather_forcing"


@asset(
    group_name="extraction",
    description="Raw meteorological forcing data from Open-Meteo (incremental)",
    compute_kind="python",
    deps=["usgs_streamflow_raw"],  # Depend on streamflow to avoid DuckDB write lock conflicts
)
def weather_forcing_raw(
    context: AssetExecutionContext,
    config: WeatherConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract hourly meteorological forcing data from Open-Meteo.

    Replaces the discontinued NASA NLDAS-2 Data Rods service.

    Supports incremental loading:
    - First run: loads `days_back` days of history
    - Subsequent runs: loads from last timestamp minus `incremental_days` overlap

    Uses (longitude, latitude, datetime) as the unique key to avoid duplicates.
    """
    from elt.extraction.weather import fetch_weather_forcing

    # Get site coordinates from metadata
    with duckdb.get_connection() as conn:
        query = f"""
            SELECT site_id, longitude, latitude
            FROM {RAW_SCHEMA}.{TBL_SITE_METADATA}
            WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        """
        if config.sample_mode:
            query += f" LIMIT {config.max_sites}"
        result = conn.execute(query).fetchall()

    if not result:
        context.log.warning("No sites with coordinates found")
        return MaterializeResult(metadata={"num_records": 0, "status": "no_coordinates"})

    coordinates = [(row[1], row[2]) for row in result]

    # Determine date range based on watermark
    # NLDAS API only accepts dates up to yesterday, not today
    end_date = datetime.now() - timedelta(days=1)
    watermark = get_high_watermark(duckdb, TBL_WEATHER, "datetime")

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
        f"Fetching weather forcing for {len(coordinates)} locations "
        f"from {start_date.date()} to {end_date.date()}"
    )

    try:
        df = fetch_weather_forcing(
            coordinates=coordinates,
            start_date=start_date,
            end_date=end_date,
            variables=config.variables,
        )
    except Exception as e:
        context.log.error(f"Failed to fetch weather data: {e}")
        # Create empty table so dbt doesn't fail
        with duckdb.get_connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TBL_WEATHER} (
                    longitude DOUBLE,
                    latitude DOUBLE,
                    datetime TIMESTAMP,
                    prcp DOUBLE,
                    temp DOUBLE,
                    humidity DOUBLE,
                    wind_speed DOUBLE,
                    wind_direction DOUBLE,
                    extracted_at TIMESTAMP
                )
            """)
        return MaterializeResult(
            metadata={"num_records": 0, "status": "fetch_failed", "error": str(e)}
        )

    if df.empty:
        # Create empty table with expected schema so dbt doesn't fail
        context.log.warning("No weather data fetched, creating empty table")
        with duckdb.get_connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TBL_WEATHER} (
                    longitude DOUBLE,
                    latitude DOUBLE,
                    datetime TIMESTAMP,
                    prcp DOUBLE,
                    temp DOUBLE,
                    humidity DOUBLE,
                    wind_speed DOUBLE,
                    wind_direction DOUBLE,
                    extracted_at TIMESTAMP
                )
            """)
        return MaterializeResult(metadata={"num_records": 0, "status": "empty"})

    # Add extraction timestamp
    df["extracted_at"] = datetime.now()

    # Upsert to avoid duplicates
    new_records = upsert_timeseries(
        duckdb, df, TBL_WEATHER, key_columns=["longitude", "latitude", "datetime"]
    )

    context.log.info(f"Inserted {new_records} new records (fetched {len(df)} total)")

    return MaterializeResult(
        metadata={
            "records_fetched": len(df),
            "records_inserted": new_records,
            "num_locations": len(coordinates),
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
