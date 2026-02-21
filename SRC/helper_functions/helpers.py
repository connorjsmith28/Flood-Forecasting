import wandb
import polars as pl
from pathlib import Path
import re
import wandb
import duckdb

def pull_wandb(file_name: str,file_path: str = None,n_rows: int | None = None) -> pl.DataFrame:
    run = wandb.init(
        project="flood-forecasting",
        entity="connorjsmith28-rice-university",
        job_type="preprocessing"
    )
    artifact = run.use_artifact(
        f"connorjsmith28-rice-university/flood-forecasting/{file_path}:latest"
    )
    artifact_dir = artifact.download()
    df = pl.read_parquet(
        f"{artifact_dir}/{file_name}.parquet",
        n_rows=55000,
    )
    return (
        df.filter(pl.col("site_id").cast(pl.Utf8) == "06923250")
        .sort("observation_hour")
        .head(100)
    )