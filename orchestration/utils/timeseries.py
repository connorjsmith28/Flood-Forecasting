"""Timeseries utilities for incremental data loading."""

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from orchestration.resources import DuckDBResource

# Schema for raw data tables
RAW_SCHEMA = "raw"


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
        # Handle both datetime and date types
        val = result[0]
        if isinstance(val, datetime):
            return val
        return datetime.combine(val, datetime.min.time())

    return None


def upsert_timeseries(
    duckdb: "DuckDBResource",
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
