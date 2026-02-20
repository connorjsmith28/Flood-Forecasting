import wandb
import polars as pl
def pull_wandb(config,file_name):
    run = wandb.init(
                    project="flood-forecasting",
                    entity="connorjsmith28-rice-university",
                    job_type="preprocessing",
                    config=config
                    )

    # 1. Load data from wandb

    artifact = run.use_artifact(f"connorjsmith28-rice-university/flood-forecasting/{file_name}:latest")
    artifact_dir = artifact.download()

    return pl.read_parquet(f"{artifact_dir}/{file_name}.parquet")