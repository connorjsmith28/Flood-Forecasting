"""Utility functions for extraction assets."""

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orchestration.resources import DuckDBResource


# Schema for raw data tables
RAW_SCHEMA = "raw"

# Table names (different from asset names to avoid DuckDB replacement scan conflicts)
TBL_SITE_METADATA = "site_metadata"
TBL_STREAMFLOW = "streamflow_raw"
TBL_WEATHER = "weather_forcing"


def get_high_watermark(
    duckdb: "DuckDBResource",
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
        val = result[0]
        if isinstance(val, datetime):
            # Strip timezone info for consistent comparisons with naive datetimes
            return val.replace(tzinfo=None)
        return datetime.combine(val, datetime.min.time())

    return None
