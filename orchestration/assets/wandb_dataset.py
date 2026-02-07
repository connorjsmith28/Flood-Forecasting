"""W&B dataset artifact assets for flood forecasting."""

import hashlib
import json
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import wandb
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Config,
)

from orchestration.utils import get_db_path


class WandbDatasetConfig(Config):
    """Configuration for W&B dataset upload."""

    full_refresh: bool = False
    project: str = "flood-forecasting"
    artifact_name: str = "flood-dataset"


class WandbDatasetDailyConfig(Config):
    """Configuration for W&B daily dataset upload."""

    full_refresh: bool = False
    project: str = "flood-forecasting"
    artifact_name: str = "flood-dataset-daily"


def get_schema_fingerprint(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> tuple[str, dict]:
    """Generate a fingerprint of the table schema."""
    schema_info = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
    """,
        [table_name],
    ).fetchall()

    schema_dict = {col: dtype for col, dtype in schema_info}
    schema_json = json.dumps(schema_dict, sort_keys=True)
    fingerprint = hashlib.sha256(schema_json.encode()).hexdigest()[:12]

    return fingerprint, schema_dict


def download_existing_artifact(
    api: wandb.Api,
    project: str,
    artifact_name: str,
    parquet_filename: str,
    download_dir: Path,
) -> Path | None:
    """Download the existing artifact from W&B."""
    try:
        artifact = api.artifact(f"{project}/{artifact_name}:latest")
        artifact_dir = artifact.download(root=str(download_dir))
        parquet_path = Path(artifact_dir) / parquet_filename
        if parquet_path.exists():
            return parquet_path
        return None
    except wandb.errors.CommError:
        return None


def merge_datasets(
    local_df: pl.DataFrame,
    existing_path: Path | None,
    time_column: str,
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Merge local data with existing W&B data."""
    if existing_path is None:
        context.log.info("No existing artifact, using local data only")
        return local_df

    existing_df = pl.read_parquet(existing_path)
    context.log.info(f"Existing artifact: {len(existing_df):,} rows")
    context.log.info(f"Local data: {len(local_df):,} rows")

    # Combine with local data taking precedence for duplicates
    existing_only = existing_df.join(
        local_df.select(["site_id", time_column]),
        on=["site_id", time_column],
        how="anti",
    )

    merged = pl.concat([local_df, existing_only])
    context.log.info(f"Merged result: {len(merged):,} rows")
    context.log.info(f"  New/updated from local: {len(local_df):,}")
    context.log.info(f"  Retained from existing: {len(existing_only):,}")

    return merged


def delete_old_versions(
    api: wandb.Api, project: str, artifact_name: str, context: AssetExecutionContext
):
    """Delete old versions of an artifact, skipping those with aliases."""
    try:
        artifact_path = f"{project}/{artifact_name}"
        versions = api.artifacts(type_name="dataset", name=artifact_path)

        for artifact in versions:
            # Skip artifacts with aliases (like "latest")
            if artifact.aliases:
                context.log.debug(
                    f"Skipping {artifact.version} (has aliases: {artifact.aliases})"
                )
                continue
            try:
                context.log.info(f"Deleting old version: {artifact.version}")
                artifact.delete()
            except Exception as e:
                context.log.debug(f"Could not delete {artifact.version}: {e}")
    except Exception as e:
        context.log.warning(f"Could not clean old versions: {e}")


def _sync_table_to_wandb(
    context: AssetExecutionContext,
    project: str,
    artifact_name: str,
    full_refresh: bool,
    table_name: str,
    time_column: str,
    parquet_filename: str,
    description: str,
    normalize_tz: bool = False,
) -> MaterializeResult:
    """Shared logic to export a DuckDB table and upload as a W&B artifact."""
    db_path = get_db_path()
    con = duckdb.connect(db_path, read_only=True)

    # Get schema fingerprint
    fingerprint, schema_dict = get_schema_fingerprint(con, table_name)
    context.log.info(f"Schema fingerprint: {fingerprint}")
    context.log.info(f"Columns: {len(schema_dict)}")

    # Load local data
    local_df = con.execute(f"SELECT * FROM main.{table_name}").pl()
    con.close()

    context.log.info(f"Local {table_name}: {len(local_df):,} rows")

    # Normalize timezone if needed (hourly data has timestamptz)
    if normalize_tz and time_column in local_df.columns:
        local_df = local_df.with_columns(
            pl.col(time_column).dt.convert_time_zone("UTC")
        )

    # Check previous fingerprint to detect schema changes
    api = wandb.Api()
    schema_changed = False
    previous_fingerprint = None

    try:
        prev_artifact = api.artifact(f"{project}/{artifact_name}:latest")
        previous_fingerprint = prev_artifact.metadata.get("schema_fingerprint")
        if previous_fingerprint and previous_fingerprint != fingerprint:
            schema_changed = True
            context.log.warning(
                f"Schema change detected! {previous_fingerprint} -> {fingerprint}"
            )
    except wandb.errors.CommError:
        context.log.info("No previous artifact found, creating initial version")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Merge with existing data unless full refresh or schema changed
        if full_refresh:
            context.log.info("Full refresh requested, using local data only")
            final_df = local_df
        elif schema_changed:
            context.log.info("Schema changed, using local data only (incompatible)")
            final_df = local_df
        else:
            existing_path = download_existing_artifact(
                api,
                project,
                artifact_name,
                parquet_filename,
                tmpdir_path / "existing",
            )
            if existing_path is not None and normalize_tz:
                # Normalize existing artifact timezone too for consistent merge
                existing_raw = pl.read_parquet(existing_path)
                existing_raw = existing_raw.with_columns(
                    pl.col(time_column).dt.convert_time_zone("UTC")
                )
                existing_path.unlink()
                existing_raw.write_parquet(existing_path)
            final_df = merge_datasets(local_df, existing_path, time_column, context)

        # Get stats from merged data
        row_count = len(final_df)
        site_count = final_df["site_id"].n_unique()
        min_date = final_df[time_column].min()
        max_date = final_df[time_column].max()

        # Export to parquet
        parquet_path = tmpdir_path / parquet_filename
        final_df.write_parquet(parquet_path)

        file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        context.log.info(f"Parquet file size: {file_size_mb:.1f} MB")

        # Initialize W&B run
        run = wandb.init(
            project=project,
            job_type="dataset-sync",
            config={
                "table_name": table_name,
                "row_count": row_count,
                "site_count": site_count,
                "min_date": str(min_date),
                "max_date": str(max_date),
                "full_refresh": full_refresh,
                "schema_fingerprint": fingerprint,
                "schema_changed": schema_changed,
            },
        )

        # Create artifact with schema metadata
        artifact = wandb.Artifact(
            name=artifact_name,
            type="dataset",
            description=description,
            metadata={
                "schema_fingerprint": fingerprint,
                "schema": schema_dict,
                "row_count": row_count,
                "site_count": site_count,
                "date_range": {"min": str(min_date), "max": str(max_date)},
                "source_table": f"main.{table_name}",
                "uploaded_at": datetime.now().isoformat(),
                "file_size_mb": round(file_size_mb, 2),
            },
        )

        artifact.add_file(str(parquet_path))

        # Log artifact with "latest" alias
        run.log_artifact(artifact, aliases=["latest"])

        # Log metrics
        wandb.log(
            {
                "dataset_rows": row_count,
                "dataset_sites": site_count,
                "dataset_size_mb": file_size_mb,
                "schema_changed": 1 if schema_changed else 0,
            }
        )

        # Log schema change as alert if detected
        if schema_changed:
            wandb.alert(
                title="Dataset Schema Changed",
                text=f"Schema fingerprint changed from {previous_fingerprint} to {fingerprint}",
                level=wandb.AlertLevel.INFO,
            )

        run.finish()

    # Clean up old versions to save storage
    delete_old_versions(api, project, artifact_name, context)

    context.log.info(
        f"Uploaded {row_count:,} rows ({file_size_mb:.1f} MB) "
        f"to {project}/{artifact_name}"
    )

    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "site_count": site_count,
            "file_size_mb": MetadataValue.float(round(file_size_mb, 2)),
            "schema_fingerprint": fingerprint,
            "schema_changed": schema_changed,
            "full_refresh": full_refresh,
            "date_range": MetadataValue.json(
                {
                    "min": str(min_date),
                    "max": str(max_date),
                }
            ),
        }
    )


@asset(
    group_name="sync",
    description="Upload flood_model (hourly) dataset to W&B as an artifact",
    compute_kind="wandb",
    deps=["dbt_flood_forecasting"],
)
def wandb_dataset(
    context: AssetExecutionContext,
    config: WandbDatasetConfig,
) -> MaterializeResult:
    """Export flood_model table and upload as W&B artifact."""
    return _sync_table_to_wandb(
        context=context,
        project=config.project,
        artifact_name=config.artifact_name,
        full_refresh=config.full_refresh,
        table_name="flood_model",
        time_column="observation_hour",
        parquet_filename="flood_model.parquet",
        description="ML-ready flood forecasting dataset (hourly resolution)",
        normalize_tz=True,
    )


@asset(
    group_name="sync",
    description="Upload flood_model_daily dataset to W&B as an artifact",
    compute_kind="wandb",
    deps=["dbt_flood_forecasting"],
)
def wandb_dataset_daily(
    context: AssetExecutionContext,
    config: WandbDatasetDailyConfig,
) -> MaterializeResult:
    """Export flood_model_daily table and upload as W&B artifact."""
    return _sync_table_to_wandb(
        context=context,
        project=config.project,
        artifact_name=config.artifact_name,
        full_refresh=config.full_refresh,
        table_name="flood_model_daily",
        time_column="observed_date",
        parquet_filename="flood_model_daily.parquet",
        description="ML-ready flood forecasting dataset (daily resolution)",
        normalize_tz=False,
    )
