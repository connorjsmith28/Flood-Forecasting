# src/elt/weights_biases_integration/

Scripts to sync processed datasets between local DuckDB and W&B Artifacts.

## Files

### `download_repository.py`

Downloads parquet/CSV artifacts from a W&B run and imports them into DuckDB under the `wandb.*` schema. Used after the Dagster `wandb_dataset` asset produces an artifact, so notebooks can query locally without re-downloading every session.

## Typical workflow

1. Run `just extract` + `just transform` to populate `final.flood_model` in DuckDB
2. Run the Dagster `wandb_dataset` asset (or `full_pipeline_job`) to upload the table as a W&B artifact
3. Use `pull_wandb()` in notebooks to download and filter the artifact, or `pull_duckdb()` to query the local copy directly
