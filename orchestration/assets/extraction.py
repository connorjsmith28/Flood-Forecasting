"""Data extraction assets for flood forecasting.

These assets extract data from various hydrology APIs and store them in DuckDB.
Supports both sample mode (for testing) and incremental loading (for production).
"""

from datetime import datetime, timedelta

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)
import pandas as pd

from orchestration.resources import DuckDBResource


class ExtractionConfig(Config):
    """Base configuration for extraction assets."""

    sample_mode: bool = True  # If True, only load a limited sample for USGS data
    max_sites: int = 100  # Max sites to load in sample mode (controls USGS data volume)


class StreamflowConfig(ExtractionConfig):
    """Configuration for streamflow extraction."""

    days_back: int = 30  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    site_ids: list[str] | None = None


class WeatherConfig(ExtractionConfig):
    """Configuration for weather forcing data extraction (Open-Meteo)."""

    days_back: int = 7
    incremental_days: int = 2
    variables: list[str] = [
        "prcp",
        "temp",
        "humidity",
        "wind_speed",
        "wind_direction",
        "rsds",  # Shortwave radiation
        "rlds",  # Longwave radiation
        "psurf",  # Surface pressure
        "pet",  # Evapotranspiration
    ]


class SiteConfig(ExtractionConfig):
    """Configuration for site metadata extraction."""

    huc_code: str = "10"  # HUC region to extract (default: Missouri River Basin)


# Schema for raw data tables
RAW_SCHEMA = "raw"

# Table names (different from asset names to avoid DuckDB replacement scan conflicts)
TBL_SITE_METADATA = "site_metadata"
TBL_STREAMFLOW = "streamflow_raw"
TBL_WEATHER = "weather_forcing"


def get_high_watermark(
    duckdb: DuckDBResource,
    table_name: str,
    datetime_column: str = "datetime",
) -> datetime | None:
    """Get the most recent timestamp from a table (high watermark).

    Returns None if table doesn't exist or is empty.
    """
    if not duckdb.table_exists(table_name, RAW_SCHEMA):
        return None

    with duckdb.get_connection() as conn:
        result = conn.execute(
            f"SELECT MAX({datetime_column}) FROM {RAW_SCHEMA}.{table_name}"
        ).fetchone()

    if result and result[0]:
        # Handle both datetime and date types
        val = result[0]
        if isinstance(val, datetime):
            return val
        return datetime.combine(val, datetime.min.time())

    return None


def upsert_timeseries(
    duckdb: DuckDBResource,
    df: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
) -> int:
    """Insert new records, ignoring duplicates based on key columns.

    Returns the number of new records inserted.
    """
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

        if not duckdb.table_exists(table_name, RAW_SCHEMA):
            # Create table from DataFrame
            conn.execute(
                f"CREATE TABLE {RAW_SCHEMA}.{table_name} AS SELECT * FROM df"
            )
            return len(df)

        # Get count before insert
        before_count = conn.execute(
            f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table_name}"
        ).fetchone()[0]

        # Insert only records that don't already exist (based on key columns)
        key_conditions = " AND ".join(
            f"t.{col} = df.{col}" for col in key_columns
        )

        conn.execute(f"""
            INSERT INTO {RAW_SCHEMA}.{table_name}
            SELECT df.*
            FROM df
            WHERE NOT EXISTS (
                SELECT 1 FROM {RAW_SCHEMA}.{table_name} t
                WHERE {key_conditions}
            )
        """)

        # Get count after insert
        after_count = conn.execute(
            f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table_name}"
        ).fetchone()[0]

        return after_count - before_count


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
        return MaterializeResult(metadata={"num_sites": 0, "status": "fetch_failed"})

    df = pd.concat(all_metadata, ignore_index=True)

    # Store in DuckDB (full replace - this is reference data)
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
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


@asset(
    group_name="extraction",
    description="Raw USGS streamflow observations (incremental)",
    compute_kind="python",
    deps=[usgs_site_metadata],  # Depend on site metadata for site list
)
def usgs_streamflow_raw(
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


@asset(
    group_name="extraction",
    description="Raw meteorological forcing data from Open-Meteo (incremental)",
    compute_kind="python",
    deps=[usgs_streamflow_raw],  # Depend on streamflow to avoid DuckDB write lock conflicts
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
