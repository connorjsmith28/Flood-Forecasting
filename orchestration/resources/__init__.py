"""Dagster resources for flood forecasting pipelines."""

from orchestration.resources.duckdb import DuckDBResource
from orchestration.resources.wandb import WandBResource

__all__ = ["DuckDBResource", "WandBResource"]
