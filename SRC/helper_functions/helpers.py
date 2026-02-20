import wandb
import polars as pl


def pull_wandb(
    config: dict,
    file_name: str,
    n_rows: int | None = None,
) -> pl.DataFrame:
    """
    Pull a W&B artifact and return it as a Polars DataFrame.

    Optional n_rows: read only the first n_rows from the parquet (saves memory and time).
    """
    run = wandb.init(
        project="flood-forecasting",
        entity="connorjsmith28-rice-university",
        job_type="preprocessing",
        config=config,
    )
    artifact = run.use_artifact(
        f"connorjsmith28-rice-university/flood-forecasting/{file_name}:latest"
    )
    artifact_dir = artifact.download()
    return pl.read_parquet(
        f"{artifact_dir}/{file_name}.parquet",
        n_rows=n_rows,
    )

 