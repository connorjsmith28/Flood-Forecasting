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
    missouri_basin_sites,
    nldi_basin_attributes,
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


# Define asset jobs
extraction_job = define_asset_job(
    name="extraction_job",
    description="Extract all raw data from USGS and Open-Meteo",
    selection=AssetSelection.groups("extraction"),
)

site_setup_job = define_asset_job(
    name="site_setup_job",
    description="Set up site metadata (run first)",
    selection=AssetSelection.assets(missouri_basin_sites, nldi_basin_attributes, usgs_site_metadata),
)

streamflow_update_job = define_asset_job(
    name="streamflow_update_job",
    description="Incremental streamflow data update",
    selection=AssetSelection.assets(usgs_streamflow_raw),
)

weather_update_job = define_asset_job(
    name="weather_update_job",
    description="Incremental weather forcing data update",
    selection=AssetSelection.assets(weather_forcing_raw),
)

transformation_job = define_asset_job(
    name="transformation_job",
    description="Run dbt transformations (staging + marts)",
    selection=AssetSelection.groups("transformation"),
)

# Full pipeline: extraction -> transformation
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description="Run full ELT pipeline: extract raw data then transform",
    selection=AssetSelection.groups("extraction", "transformation"),
)


# Dagster definitions
defs = Definitions(
    assets=[
        missouri_basin_sites,
        nldi_basin_attributes,
        usgs_site_metadata,
        usgs_streamflow_raw,
        weather_forcing_raw,
        dbt_flood_forecasting,
    ],
    jobs=[
        extraction_job,
        site_setup_job,
        streamflow_update_job,
        weather_update_job,
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
