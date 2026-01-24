"""Dagster definitions for flood forecasting pipelines.

Run the Dagster webserver with:
    dagster dev -m orchestration.definitions

Or from the orchestration directory:
    dagster dev
"""

import os
from pathlib import Path

from dagster import Definitions, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from orchestration.resources import DuckDBResource
from orchestration.assets import (
    usgs_site_metadata,
    usgs_streamflow_raw,
    weather_forcing_raw,
)
from orchestration.assets.dbt_assets import dbt_flood_forecasting

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "elt" / "transformation"


def get_db_path() -> str:
    """Get path to DuckDB database file."""
    return str(PROJECT_ROOT / "flood_forecasting.duckdb")


# Set DUCKDB_PATH env var so dbt profiles.yml can reference it
# This ensures Dagster and dbt always use the same database file
os.environ["DUCKDB_PATH"] = get_db_path()


# Define jobs
extraction_job = define_asset_job(
    name="extraction_job",
    description="Extract raw data from USGS and Open-Meteo",
    selection=AssetSelection.groups("extraction"),
)

transformation_job = define_asset_job(
    name="transformation_job",
    description="Run dbt transformations (seeds + staging + marts)",
    selection=AssetSelection.groups("transformation"),
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description="Run full ELT pipeline: extract then transform",
    selection=AssetSelection.groups("extraction", "transformation"),
)


# Dagster definitions
defs = Definitions(
    assets=[
        usgs_site_metadata,
        usgs_streamflow_raw,
        weather_forcing_raw,
        dbt_flood_forecasting,
    ],
    jobs=[
        extraction_job,
        transformation_job,
        full_pipeline_job,
    ],
    resources={
        "duckdb": DuckDBResource(database_path=get_db_path()),
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        ),
    },
)
