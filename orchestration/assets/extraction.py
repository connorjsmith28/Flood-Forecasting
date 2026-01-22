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
import polars as pl

from orchestration.resources import DuckDBResource


class ExtractionConfig(Config):
    """Base configuration for extraction assets."""

    sample_mode: bool = True  # If True, only load a small sample for testing
    max_sites: int = 10  # Max sites to load in sample mode


class StreamflowConfig(ExtractionConfig):
    """Configuration for streamflow extraction."""

    days_back: int = 30  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    site_ids: list[str] | None = None


class NLDASConfig(ExtractionConfig):
    """Configuration for NLDAS forcing data extraction."""

    days_back: int = 7
    incremental_days: int = 2
    variables: list[str] = ["prcp", "temp", "humidity", "wind_u", "wind_v"]


class SiteConfig(ExtractionConfig):
    """Configuration for site metadata extraction."""

    pass


# Schema for raw data tables
RAW_SCHEMA = "raw"

# Table names (different from asset names to avoid DuckDB replacement scan conflicts)
TBL_MISSOURI_SITES = "sites_missouri_basin"
TBL_GAGES_ATTRS = "gages_basin_attributes"
TBL_SITE_METADATA = "site_metadata"
TBL_STREAMFLOW = "streamflow_raw"
TBL_NLDAS = "nldas_forcing"


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
    df: pl.DataFrame,
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
    description="USGS sites in the Missouri River Basin (HUC 10)",
    compute_kind="python",
)
def missouri_basin_sites(
    context: AssetExecutionContext,
    config: SiteConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract list of USGS sites in the Missouri River Basin.

    The Missouri Basin is the largest tributary of the Mississippi River,
    making it a key focus area for flood forecasting.
    """
    from elt.extraction.gages import get_missouri_basin_sites

    context.log.info("Fetching Missouri Basin sites from USGS...")

    df = get_missouri_basin_sites()

    if df.is_empty():
        context.log.warning("No sites returned from USGS")
        return MaterializeResult(
            metadata={"num_sites": 0, "status": "empty"},
        )

    # Apply sample mode limit
    if config.sample_mode:
        df = df.head(config.max_sites)
        context.log.info(f"Sample mode: limited to {config.max_sites} sites")

    # Store in DuckDB (full replace - this is reference data)
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        conn.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{TBL_MISSOURI_SITES}")
        conn.execute(
            f"CREATE TABLE {RAW_SCHEMA}.{TBL_MISSOURI_SITES} AS SELECT * FROM df"
        )

    context.log.info(f"Stored {len(df)} Missouri Basin sites in DuckDB")

    return MaterializeResult(
        metadata={
            "num_sites": len(df),
            "sample_mode": config.sample_mode,
            "columns": MetadataValue.json(df.columns),
            "sample_sites": MetadataValue.json(
                df.head(5).select("site_id", "station_name").to_dicts()
            ),
        },
    )


@asset(
    group_name="extraction",
    description="GAGES-II watershed attributes for analysis basins",
    compute_kind="python",
)
def gages_attributes(
    context: AssetExecutionContext,
    config: SiteConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract GAGES-II catchment attributes.

    GAGES-II provides hundreds of watershed characteristics including
    climate, geology, hydrology, land cover, soils, and topography.
    """
    from elt.extraction.gages import fetch_gages_attributes

    context.log.info("Fetching GAGES-II attributes...")

    df = fetch_gages_attributes()

    if df.is_empty():
        context.log.warning("No GAGES-II attributes returned")
        return MaterializeResult(
            metadata={"num_basins": 0, "status": "empty"},
        )

    if config.sample_mode:
        df = df.head(config.max_sites)
        context.log.info(f"Sample mode: limited to {config.max_sites} basins")

    # Store in DuckDB (full replace - this is reference data)
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        conn.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{TBL_GAGES_ATTRS}")
        conn.execute(
            f"CREATE TABLE {RAW_SCHEMA}.{TBL_GAGES_ATTRS} AS SELECT * FROM df"
        )

    context.log.info(f"Stored GAGES-II attributes for {len(df)} basins")

    return MaterializeResult(
        metadata={
            "num_basins": len(df),
            "sample_mode": config.sample_mode,
            "num_attributes": len(df.columns),
            "columns": MetadataValue.json(df.columns[:20]),
        },
    )


@asset(
    group_name="extraction",
    description="USGS site metadata including location and drainage area",
    compute_kind="python",
    deps=[missouri_basin_sites],
)
def usgs_site_metadata(
    context: AssetExecutionContext,
    config: SiteConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract detailed metadata for USGS monitoring sites.

    Fetches site information including coordinates, drainage area,
    and HUC codes for sites in the Missouri Basin.
    """
    from elt.extraction.usgs import get_site_info

    # Get site IDs from sites table
    with duckdb.get_connection() as conn:
        query = f"SELECT site_id FROM {RAW_SCHEMA}.{TBL_MISSOURI_SITES}"
        if config.sample_mode:
            query += f" LIMIT {config.max_sites}"
        result = conn.execute(query).fetchall()

    if not result:
        context.log.warning("No sites found in sites table")
        return MaterializeResult(metadata={"num_sites": 0, "status": "no_sites"})

    site_ids = [row[0] for row in result]
    context.log.info(f"Fetching metadata for {len(site_ids)} sites...")

    # Fetch in batches to avoid API limits
    batch_size = 100
    all_metadata = []

    for i in range(0, len(site_ids), batch_size):
        batch = site_ids[i : i + batch_size]
        context.log.info(f"Fetching batch {i // batch_size + 1}...")

        try:
            df = get_site_info(batch)
            if not df.is_empty():
                all_metadata.append(df)
        except Exception as e:
            context.log.warning(f"Failed to fetch batch: {e}")
            continue

    if not all_metadata:
        return MaterializeResult(metadata={"num_sites": 0, "status": "fetch_failed"})

    df = pl.concat(all_metadata, how="diagonal")

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
            "sample_mode": config.sample_mode,
            "columns": MetadataValue.json(df.columns),
        },
    )


@asset(
    group_name="extraction",
    description="Raw USGS streamflow observations (incremental)",
    compute_kind="python",
    deps=[usgs_site_metadata],
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
            if not df.is_empty():
                all_data.append(df)
        except Exception as e:
            context.log.warning(f"Failed to fetch streamflow batch: {e}")
            continue

    if not all_data:
        return MaterializeResult(metadata={"num_records": 0, "status": "fetch_failed"})

    df = pl.concat(all_data, how="diagonal")

    # Add extraction timestamp
    df = df.with_columns(pl.lit(datetime.now()).alias("extracted_at"))

    # Upsert to avoid duplicates
    new_records = upsert_timeseries(
        duckdb, df, TBL_STREAMFLOW, key_columns=["site_id", "datetime"]
    )

    context.log.info(f"Inserted {new_records} new records (fetched {len(df)} total)")

    return MaterializeResult(
        metadata={
            "records_fetched": len(df),
            "records_inserted": new_records,
            "num_sites": df.n_unique("site_id"),
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
    description="Raw NLDAS-2 meteorological forcing data (incremental)",
    compute_kind="python",
    deps=[usgs_streamflow_raw],  # Depend on streamflow to avoid DuckDB write lock conflicts
)
def nldas_forcing_raw(
    context: AssetExecutionContext,
    config: NLDASConfig,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract NLDAS-2 hourly meteorological forcing data.

    Supports incremental loading:
    - First run: loads `days_back` days of history
    - Subsequent runs: loads from last timestamp minus `incremental_days` overlap

    Uses (longitude, latitude, datetime) as the unique key to avoid duplicates.
    """
    from elt.extraction.nldas import fetch_nldas_forcing

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
    watermark = get_high_watermark(duckdb, TBL_NLDAS, "datetime")

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
        f"Fetching NLDAS forcing for {len(coordinates)} locations "
        f"from {start_date.date()} to {end_date.date()}"
    )

    try:
        df = fetch_nldas_forcing(
            coordinates=coordinates,
            start_date=start_date,
            end_date=end_date,
            variables=config.variables,
        )
    except Exception as e:
        context.log.error(f"Failed to fetch NLDAS data: {e}")
        return MaterializeResult(
            metadata={"num_records": 0, "status": "fetch_failed", "error": str(e)}
        )

    if df.is_empty():
        # Create empty table with expected schema so dbt doesn't fail
        context.log.warning("No NLDAS data fetched, creating empty table")
        with duckdb.get_connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TBL_NLDAS} (
                    longitude DOUBLE,
                    latitude DOUBLE,
                    datetime TIMESTAMP,
                    prcp DOUBLE,
                    temp DOUBLE,
                    humidity DOUBLE,
                    wind_u DOUBLE,
                    wind_v DOUBLE,
                    extracted_at TIMESTAMP
                )
            """)
        return MaterializeResult(metadata={"num_records": 0, "status": "empty"})

    # Add extraction timestamp
    df = df.with_columns(pl.lit(datetime.now()).alias("extracted_at"))

    # Upsert to avoid duplicates
    new_records = upsert_timeseries(
        duckdb, df, TBL_NLDAS, key_columns=["longitude", "latitude", "datetime"]
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
