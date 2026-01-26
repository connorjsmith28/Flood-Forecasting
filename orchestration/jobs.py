"""Dagster job definitions for flood forecasting pipelines."""

from dagster import define_asset_job, AssetSelection

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
