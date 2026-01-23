"""Dagster resources for flood forecasting pipelines."""

import os
from pathlib import Path

from dagster import ConfigurableResource, InitResourceContext
import duckdb


class DuckDBResource(ConfigurableResource):
    """DuckDB database resource for storing extracted data.

    The database file is stored locally and gitignored to avoid committing
    large data files to version control.
    """

    database_path: str = "flood_forecasting.duckdb"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a DuckDB connection."""
        # Ensure parent directory exists
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        return duckdb.connect(str(db_path))

    def execute(self, query: str, parameters: list | None = None) -> None:
        """Execute a query without returning results."""
        with self.get_connection() as conn:
            if parameters:
                conn.execute(query, parameters)
            else:
                conn.execute(query)

    def query(self, query: str, parameters: list | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a query and return results as a DuckDB relation."""
        conn = self.get_connection()
        if parameters:
            return conn.execute(query, parameters)
        return conn.execute(query)

    def table_exists(self, table_name: str, schema: str = "main") -> bool:
        """Check if a table exists in the database."""
        with self.get_connection() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table_name],
            ).fetchone()
            return result[0] > 0 if result else False

    def create_schema_if_not_exists(self, schema: str) -> None:
        """Create a schema if it doesn't exist."""
        with self.get_connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def drop_table(self, table_name: str, schema: str = "main") -> None:
        """Drop a table if it exists."""
        with self.get_connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")


def get_default_duckdb_path() -> str:
    """Get the default path for the DuckDB database.

    Returns a path in the project root directory.
    """
    # I think we want to figure out where we will put the db. It would be great to better sync this with dbt. 
    return "flood_forecasting.duckdb"
