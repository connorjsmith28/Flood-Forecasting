"""W&B dataset artifact asset for flood forecasting."""

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


def get_schema_fingerprint(con: duckdb.DuckDBPyConnection) -> tuple[str, dict]:
    """Generate a fingerprint of the table schema."""
    schema_info = con.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = 'flood_model'
        ORDER BY ordinal_position
    """).fetchall()

    schema_dict = {col: dtype for col, dtype in schema_info}
    schema_json = json.dumps(schema_dict, sort_keys=True)
    fingerprint = hashlib.sha256(schema_json.encode()).hexdigest()[:12]

    return fingerprint, schema_dict


def download_existing_artifact(
    api: wandb.Api, project: str, artifact_name: str, download_dir: Path
) -> Path | None:
    """Download the existing artifact from W&B."""
    try:
        artifact = api.artifact(f"{project}/{artifact_name}:latest")
        artifact_dir = artifact.download(root=str(download_dir))
        parquet_path = Path(artifact_dir) / "flood_model.parquet"
        if parquet_path.exists():
            return parquet_path
        return None
    except wandb.errors.CommError:
        return None


def normalize_timezone(df: pl.DataFrame) -> pl.DataFrame:
    """Convert observation_hour to UTC for consistent merging."""
    return df.with_columns(pl.col("observation_hour").dt.convert_time_zone("UTC"))


def merge_datasets(
    local_df: pl.DataFrame, existing_path: Path | None, context: AssetExecutionContext
) -> pl.DataFrame:
    """Merge local data with existing W&B data."""
    if existing_path is None:
        context.log.info("No existing artifact, using local data only")
        return normalize_timezone(local_df)

    existing_df = pl.read_parquet(existing_path)
    context.log.info(f"Existing artifact: {len(existing_df):,} rows")
    context.log.info(f"Local data: {len(local_df):,} rows")

    # Normalize timezones to UTC for consistent joining
    local_normalized = normalize_timezone(local_df)
    existing_normalized = normalize_timezone(existing_df)

    # Combine with local data taking precedence for duplicates
    existing_only = existing_normalized.join(
        local_normalized.select(["site_id", "observation_hour"]),
        on=["site_id", "observation_hour"],
        how="anti",
    )

    merged = pl.concat([local_normalized, existing_only])
    context.log.info(f"Merged result: {len(merged):,} rows")
    context.log.info(f"  New/updated from local: {len(local_normalized):,}")
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


@asset(
    group_name="sync",
    description="Upload flood_model dataset to W&B as an artifact",
    compute_kind="wandb",
    deps=["dbt_flood_forecasting"],
)
def wandb_dataset(
    context: AssetExecutionContext,
    config: WandbDatasetConfig,
) -> MaterializeResult:
    """Export flood_model table and upload as W&B artifact.

    For incremental runs, merges local data with existing W&B artifact.
    For full refresh, uploads local data only (replacing everything).

    Maintains only a single version to save storage. Schema changes
    are tracked via fingerprints logged as metadata.
    """
    db_path = get_db_path()
    con = duckdb.connect(db_path, read_only=True)

    # Get schema fingerprint
    fingerprint, schema_dict = get_schema_fingerprint(con)
    context.log.info(f"Schema fingerprint: {fingerprint}")
    context.log.info(f"Columns: {len(schema_dict)}")

    # Load local data
    local_df = con.execute("SELECT * FROM main.flood_model").pl()
    con.close()

    context.log.info(f"Local flood_model: {len(local_df):,} rows")

    # Check previous fingerprint to detect schema changes
    api = wandb.Api()
    schema_changed = False
    previous_fingerprint = None

    try:
        prev_artifact = api.artifact(f"{config.project}/{config.artifact_name}:latest")
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
        if config.full_refresh:
            context.log.info("Full refresh requested, using local data only")
            final_df = normalize_timezone(local_df)
        elif schema_changed:
            context.log.info("Schema changed, using local data only (incompatible)")
            final_df = normalize_timezone(local_df)
        else:
            existing_path = download_existing_artifact(
                api, config.project, config.artifact_name, tmpdir_path / "existing"
            )
            final_df = merge_datasets(local_df, existing_path, context)

        # Get stats from merged data
        row_count = len(final_df)
        site_count = final_df["site_id"].n_unique()
        min_date = final_df["observation_hour"].min()
        max_date = final_df["observation_hour"].max()

        # Export to parquet
        parquet_path = tmpdir_path / "flood_model.parquet"
        final_df.write_parquet(parquet_path)

        file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        context.log.info(f"Parquet file size: {file_size_mb:.1f} MB")

        # Initialize W&B run
        run = wandb.init(
            project=config.project,
            job_type="dataset-sync",
            config={
                "row_count": row_count,
                "site_count": site_count,
                "min_date": str(min_date),
                "max_date": str(max_date),
                "full_refresh": config.full_refresh,
                "schema_fingerprint": fingerprint,
                "schema_changed": schema_changed,
            },
        )

        # Create artifact with schema metadata
        artifact = wandb.Artifact(
            name=config.artifact_name,
            type="dataset",
            description="ML-ready flood forecasting dataset",
            metadata={
                "schema_fingerprint": fingerprint,
                "schema": schema_dict,
                "row_count": row_count,
                "site_count": site_count,
                "date_range": {"min": str(min_date), "max": str(max_date)},
                "source_table": "main.flood_model",
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
    delete_old_versions(api, config.project, config.artifact_name, context)

    context.log.info(
        f"Uploaded {row_count:,} rows ({file_size_mb:.1f} MB) "
        f"to {config.project}/{config.artifact_name}"
    )

    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "site_count": site_count,
            "file_size_mb": MetadataValue.float(round(file_size_mb, 2)),
            "schema_fingerprint": fingerprint,
            "schema_changed": schema_changed,
            "full_refresh": config.full_refresh,
            "date_range": MetadataValue.json(
                {
                    "min": str(min_date),
                    "max": str(max_date),
                }
            ),
        }
    )
