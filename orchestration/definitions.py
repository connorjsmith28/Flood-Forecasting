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
from orchestration.jobs import (
    extraction_job,
    transformation_job,
    full_pipeline_job,
    wandb_sync_job,
    sync_job,
)
from orchestration.assets import (
    usgs_site_metadata,
    usgs_streamflow_15min,
    usgs_streamflow_daily,
    nldas3_watershed_mapping,
    nldas3_forcing_raw,
    dbt_flood_forecasting,
    wandb_dataset,
    wandb_dataset_daily,
    wandb_raw_tables,
)

# Set DUCKDB_PATH env var so dbt profiles.yml can reference it
# This ensures Dagster and dbt always use the same database file
os.environ["DUCKDB_PATH"] = get_db_path()

# Dagster definitions
defs = Definitions(
    assets=[
        usgs_site_metadata,
        usgs_streamflow_15min,
        usgs_streamflow_daily,
        nldas3_watershed_mapping,
        nldas3_forcing_raw,
        dbt_flood_forecasting,
        wandb_dataset,
        wandb_dataset_daily,
        wandb_raw_tables,
    ],
    jobs=[
        extraction_job,
        transformation_job,
        full_pipeline_job,
        wandb_sync_job,
        sync_job,
    ],
    resources={
        "duckdb": DuckDBResource(database_path=get_db_path()),
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        ),
    },
)
