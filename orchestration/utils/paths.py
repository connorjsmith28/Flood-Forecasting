"""Project path utilities for flood forecasting orchestration."""

from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "elt" / "transformation"


def get_db_path() -> str:
    """Get path to DuckDB database file."""
    return str(PROJECT_ROOT / "flood_forecasting.duckdb")
