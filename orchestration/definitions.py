"""Dagster definitions for flood forecasting pipelines.

Run the Dagster webserver with:
    dagster dev -m orchestration.definitions

Or from the orchestration directory:
    dagster dev
"""

import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.resources import DuckDBResource
from orchestration.utils import get_db_path, DBT_PROJECT_DIR
from orchestration.jobs import extraction_job, transformation_job, full_pipeline_job
from orchestration.assets import (
    usgs_site_metadata,
    usgs_streamflow_raw,
    weather_forcing_raw,
    dbt_flood_forecasting,
)

# Set DUCKDB_PATH env var so dbt profiles.yml can reference it
# This ensures Dagster and dbt always use the same database file
os.environ["DUCKDB_PATH"] = get_db_path()

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
