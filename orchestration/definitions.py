"""Dagster definitions for flood forecasting pipelines.

Run the Dagster webserver with:
    dagster dev -m orchestration.definitions

Or from the orchestration directory:
    dagster dev
"""

import os
from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.resources import DuckDBResource
from orchestration.assets import (
    usgs_site_metadata,
    usgs_streamflow_raw,
    weather_forcing_raw,
    dbt_flood_forecasting,
)
from orchestration.jobs import (
    extraction_job,
    transformation_job,
    full_pipeline_job,
)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "elt" / "transformation"


# Set DUCKDB_PATH env var so dbt profiles.yml can reference it
# This ensures Dagster and dbt always use the same database file
os.environ["DUCKDB_PATH"] = str(PROJECT_ROOT / "flood_forecasting.duckdb")


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
        "duckdb": DuckDBResource(database_path=os.environ["DUCKDB_PATH"]),
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        ),
    },
)
