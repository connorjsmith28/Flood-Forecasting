"""Dagster job definitions for flood forecasting pipelines."""

from dagster import define_asset_job, AssetSelection, in_process_executor

# In-process so only one step touches DuckDB at a time (no concurrent writers).
extraction_job = define_asset_job(
    name="extraction_job",
    description="Extract raw data from USGS and Open-Meteo",
    selection=AssetSelection.groups("extraction"),
    executor_def=in_process_executor,
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

sync_job = define_asset_job(
    name="sync_job",
    description="Full pipeline + upload to W&B: extract, transform, sync dataset",
    selection=AssetSelection.groups("extraction", "transformation", "sync"),
)
