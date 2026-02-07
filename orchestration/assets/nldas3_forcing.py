"""NLDAS-2 V2.0 watershed-averaged forcing data extraction asset.

Extracts hourly forcing data from NLDAS-2 (NASA GES DISC) and computes
watershed-averaged values using NLDI basin boundaries. Matches CAMELS-H methodology.

Requires Earthdata Login credentials in ~/.netrc and GES DISC EULA acceptance.
"""

from datetime import datetime, timedelta

import earthaccess
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from orchestration.configs import NLDAS3Config
from orchestration.resources import DuckDBResource
from orchestration.utils.time_windows import generate_time_windows
from orchestration.utils.timeseries import get_high_watermark, upsert_timeseries

# Schema and table names
RAW_SCHEMA = "raw"
TBL_WATERSHED_MAPPING = "nldas3_watershed_mapping"
TBL_NLDAS3_FORCING = "nldas3_forcing"


def _ensure_forcing_table(conn) -> None:
    """Create NLDAS forcing table if it doesn't exist."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TBL_NLDAS3_FORCING} (
            site_id VARCHAR,
            datetime TIMESTAMP,
            air_temp_c DOUBLE,
            specific_humidity_kgkg DOUBLE,
            surface_pressure_pa DOUBLE,
            wind_u_ms DOUBLE,
            wind_v_ms DOUBLE,
            shortwave_radiation_wm2 DOUBLE,
            longwave_radiation_wm2 DOUBLE,
            precipitation_mm DOUBLE,
            convective_precip_fraction DOUBLE,
            cape_jkg DOUBLE,
            potential_evaporation_mm DOUBLE,
            extracted_at TIMESTAMP
        )
    """
    )


@asset(
    group_name="extraction",
    description="Watershed-averaged NLDAS-2 forcing data (incremental)",
    compute_kind="python",
    deps=["nldas3_watershed_mapping"],
)
def nldas3_forcing_raw(
    context: AssetExecutionContext,
    config: NLDAS3Config,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """Extract hourly NLDAS-2 forcing data with watershed averaging.

    Downloads NLDAS-2 V2.0 hourly files from GES DISC via earthaccess and
    computes watershed-averaged values using pre-computed grid weights from
    nldas3_watershed_mapping.

    Supports incremental loading:
    - First run: fetches from min_date or days_back
    - Subsequent runs: fetches from high watermark minus incremental_days
    - Accounts for NLDAS-2 data latency (lag_days)

    Uses time-window batching to manage memory and provide checkpoints.
    """
    from elt.extraction.nldas3 import fetch_nldas3_forcing, convert_units

    # Authenticate with NASA Earthdata (reads credentials from ~/.netrc)
    earthaccess.login(strategy="netrc")

    # Load watershed mapping
    with duckdb.get_connection() as conn:
        if not duckdb.table_exists(TBL_WATERSHED_MAPPING, RAW_SCHEMA):
            context.log.error("Watershed mapping table not found - run nldas3_watershed_mapping first")
            _ensure_forcing_table(conn)
            return MaterializeResult(
                metadata={"status": "error", "error": "missing_watershed_mapping"}
            )

        mapping_df = conn.execute(
            f"SELECT site_id, grid_row, grid_col, area_weight FROM {RAW_SCHEMA}.{TBL_WATERSHED_MAPPING}"
        ).fetchdf()

    if mapping_df.empty:
        context.log.warning("Watershed mapping is empty")
        with duckdb.get_connection() as conn:
            _ensure_forcing_table(conn)
        return MaterializeResult(
            metadata={"num_records": 0, "status": "no_mapping"}
        )

    num_sites = mapping_df["site_id"].nunique()
    context.log.info(f"Loaded watershed mapping for {num_sites} sites")

    # Determine date range
    # NLDAS-2 has ~4 day latency
    end_date = datetime.now() - timedelta(days=config.lag_days)
    watermark = get_high_watermark(duckdb, TBL_NLDAS3_FORCING, "datetime")

    min_date = datetime.strptime(config.min_date, "%Y-%m-%d")

    if watermark:
        start_date = watermark - timedelta(days=config.incremental_days)
        context.log.info(
            f"Incremental load: watermark={watermark}, "
            f"fetching from {start_date.date()}"
        )
    else:
        start_date = max(
            end_date - timedelta(days=config.days_back),
            min_date,
        )
        context.log.info(
            f"Initial load: fetching from {start_date.date()} "
            f"(min_date={config.min_date}, days_back={config.days_back})"
        )

    # Generate time windows (30-day chunks for NLDAS)
    window_days = min(config.time_window_days, 30)  # Cap at 30 days for memory
    windows = generate_time_windows(start_date, end_date, window_days)
    context.log.info(
        f"Fetching NLDAS-2 forcing for {num_sites} sites "
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
            df = fetch_nldas3_forcing(
                start_date=window_start,
                end_date=window_end,
                watershed_mapping=mapping_df,
                cache_dir=config.cache_dir,
                variables=config.variables,
                max_workers=config.parallel_fetches,
                log=context.log.info,
            )
        except Exception as e:
            context.log.error(f"Failed to fetch window {window_idx + 1}: {e}")
            with duckdb.get_connection() as conn:
                _ensure_forcing_table(conn)
            continue

        if df.is_empty():
            context.log.warning(f"No data for window {window_idx + 1}")
            continue

        # Convert units (K->C)
        df = convert_units(df)

        # Add extraction timestamp
        df = df.with_columns(extracted_at=datetime.now())

        # Convert to pandas for upsert
        pdf = df.to_pandas()

        # Upsert to DuckDB
        new_records = upsert_timeseries(
            duckdb, pdf, TBL_NLDAS3_FORCING, key_columns=["site_id", "datetime"]
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
        context.log.warning("No NLDAS-2 data fetched across all windows")
        with duckdb.get_connection() as conn:
            _ensure_forcing_table(conn)
        return MaterializeResult(metadata={"num_records": 0, "status": "empty"})

    return MaterializeResult(
        metadata={
            "records_fetched": total_fetched,
            "records_inserted": total_inserted,
            "num_sites": num_sites,
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
