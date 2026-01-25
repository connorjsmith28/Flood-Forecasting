"""DuckDB resource for Dagster."""

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger


class DuckDBResource(ConfigurableResource):
    """DuckDB database resource with local and production mode support.

    Local mode: Recent data window from reference date (for development).
    Production mode: Full historical data with current date (for ML training).
    """

    database_path: str = "flood_forecasting.duckdb"

    # Mode settings
    is_production: bool = False
    full_refresh: bool = False  # If True, drop and recreate tables

    # Local mode settings (ignored in production)
    local_days_back: int = 30
    local_reference_date: str = "2025-12-31"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a DuckDB connection."""
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(db_path))

    def table_exists(self, table_name: str, schema: str = "main") -> bool:
        """Check if a table exists."""
        with self.get_connection() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table_name],
            ).fetchone()
            return result[0] > 0 if result else False

    def drop_table(self, table_name: str, schema: str = "main") -> None:
        """Drop a table if it exists."""
        with self.get_connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "raw",
        key_columns: list[str] | None = None,
        replace: bool = False,
    ) -> int:
        """Write a DataFrame to a table.

        Args:
            df: DataFrame to write
            table_name: Target table name
            schema: Target schema (default: raw)
            key_columns: Columns for upsert deduplication (optional)
            replace: If True, drop and recreate table (for reference data)

        Behavior:
        - replace=True or full_refresh=True: Drop table and create fresh
        - With key_columns: Upsert (insert new, skip duplicates)
        - Without key_columns: Append

        Returns the number of records written/inserted.
        """
        logger = get_dagster_logger()

        with self.get_connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            if self.full_refresh or replace:
                logger.info(f"Full refresh: dropping {schema}.{table_name}")
                conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")

            if not self.table_exists(table_name, schema):
                conn.execute(
                    f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df"
                )
                return len(df)

            if key_columns:
                # Upsert: insert only new records
                before_count = conn.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table_name}"
                ).fetchone()[0]

                key_conditions = " AND ".join(
                    f"t.{col} = df.{col}" for col in key_columns
                )
                conn.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT df.*
                    FROM df
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {schema}.{table_name} t
                        WHERE {key_conditions}
                    )
                """)

                after_count = conn.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table_name}"
                ).fetchone()[0]
                return after_count - before_count
            else:
                # Simple append
                conn.execute(
                    f"INSERT INTO {schema}.{table_name} SELECT * FROM df"
                )
                return len(df)

    def get_reference_date(self) -> datetime:
        """Get the reference date for data extraction.

        Production: current time
        Local: configured reference date (default end of 2025)
        """
        if self.is_production:
            return datetime.now()
        return datetime.strptime(self.local_reference_date, "%Y-%m-%d")

    def get_days_back(self) -> int | None:
        """Get the number of days of history to load.

        Production: None (full history / incremental from watermark)
        Local: configured days_back (default 30)
        """
        if self.is_production:
            return None  # Full history
        return self.local_days_back
