#!/usr/bin/env python3
"""Upload flood_model dataset to W&B as an artifact (single version).

Supports incremental updates by merging local data with existing W&B artifact.
"""

import argparse
import hashlib
import json
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import wandb


def get_schema_fingerprint(con: duckdb.DuckDBPyConnection) -> tuple[str, dict]:
    """Generate a fingerprint of the table schema.

    Returns:
        tuple: (fingerprint_hash, schema_dict)
    """
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


def delete_old_versions(api: wandb.Api, project: str, artifact_name: str):
    """Delete all but the latest version of an artifact."""
    try:
        artifact_path = f"{project}/{artifact_name}"
        versions = api.artifacts(type_name="dataset", name=artifact_path)

        # Sort by version number and delete all but latest
        version_list = list(versions)
        if len(version_list) > 1:
            # Keep the highest version (most recent)
            for artifact in version_list[:-1]:
                print(f"Deleting old version: {artifact.name}:{artifact.version}")
                artifact.delete()
    except Exception as e:
        print(f"Note: Could not clean old versions: {e}")


def download_existing_artifact(
    api: wandb.Api, project: str, artifact_name: str, download_dir: Path
) -> Path | None:
    """Download the existing artifact from W&B.

    Returns:
        Path to the downloaded parquet file, or None if no artifact exists.
    """
    try:
        artifact = api.artifact(f"{project}/{artifact_name}:latest")
        artifact_dir = artifact.download(root=str(download_dir))
        parquet_path = Path(artifact_dir) / "flood_model.parquet"
        if parquet_path.exists():
            return parquet_path
        return None
    except wandb.errors.CommError:
        return None


def merge_datasets(local_df: pl.DataFrame, existing_path: Path | None) -> pl.DataFrame:
    """Merge local data with existing W&B data.

    Deduplicates on (site_id, observation_hour), preferring local data
    for overlapping records.

    Args:
        local_df: DataFrame from local DuckDB
        existing_path: Path to existing parquet from W&B, or None

    Returns:
        Merged DataFrame
    """
    if existing_path is None:
        print("No existing artifact, using local data only")
        return local_df

    existing_df = pl.read_parquet(existing_path)
    print(f"Existing artifact: {len(existing_df):,} rows")
    print(f"Local data: {len(local_df):,} rows")

    # Combine with local data taking precedence for duplicates
    # Use anti-join to get existing rows not in local, then concat with local
    existing_only = existing_df.join(
        local_df.select(["site_id", "observation_hour"]),
        on=["site_id", "observation_hour"],
        how="anti",
    )

    merged = pl.concat([local_df, existing_only])
    print(f"Merged result: {len(merged):,} rows")
    print(f"  New/updated from local: {len(local_df):,}")
    print(f"  Retained from existing: {len(existing_only):,}")

    return merged


def upload_dataset(
    db_path: str = "flood_forecasting.duckdb",
    project: str = "flood-forecasting",
    artifact_name: str = "flood-dataset",
    full_refresh: bool = False,
):
    """Export flood_model table and upload as W&B artifact.

    For incremental runs, merges local data with existing W&B artifact.
    For full refresh, uploads local data only (replacing everything).

    Maintains only a single version to save storage. Schema changes
    are tracked via fingerprints logged as metadata.

    Args:
        db_path: Path to DuckDB database
        project: W&B project name
        artifact_name: Name for the artifact
        full_refresh: If True, skip merge and upload local data only
    """
    con = duckdb.connect(db_path, read_only=True)

    # Get schema fingerprint
    fingerprint, schema_dict = get_schema_fingerprint(con)
    print(f"Schema fingerprint: {fingerprint}")
    print(f"Columns: {len(schema_dict)}")

    # Load local data
    local_df = con.execute("SELECT * FROM main.flood_model").pl()
    con.close()

    print(f"Local flood_model: {len(local_df):,} rows")

    # Check previous fingerprint to detect schema changes
    api = wandb.Api()
    schema_changed = False
    previous_fingerprint = None

    try:
        prev_artifact = api.artifact(f"{project}/{artifact_name}:latest")
        previous_fingerprint = prev_artifact.metadata.get("schema_fingerprint")
        if previous_fingerprint and previous_fingerprint != fingerprint:
            schema_changed = True
            print(f"Schema change detected! {previous_fingerprint} -> {fingerprint}")
    except wandb.errors.CommError:
        print("No previous artifact found, creating initial version")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Merge with existing data unless full refresh or schema changed
        if full_refresh:
            print("Full refresh requested, using local data only")
            final_df = local_df
        elif schema_changed:
            print("Schema changed, using local data only (incompatible schemas)")
            final_df = local_df
        else:
            existing_path = download_existing_artifact(
                api, project, artifact_name, tmpdir_path / "existing"
            )
            final_df = merge_datasets(local_df, existing_path)

        # Get stats from merged data
        row_count = len(final_df)
        site_count = final_df["site_id"].n_unique()
        min_date = final_df["observation_hour"].min()
        max_date = final_df["observation_hour"].max()

        # Export to parquet
        parquet_path = tmpdir_path / "flood_model.parquet"
        final_df.write_parquet(parquet_path)

        file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        print(f"Parquet file size: {file_size_mb:.1f} MB")

        # Initialize W&B run
        run = wandb.init(
            project=project,
            job_type="dataset-sync",
            config={
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
    delete_old_versions(api, project, artifact_name)

    print(
        f"Uploaded {row_count:,} rows ({file_size_mb:.1f} MB) to {project}/{artifact_name}"
    )
    return artifact


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload flood_model dataset to W&B")
    parser.add_argument(
        "--db-path",
        default="flood_forecasting.duckdb",
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--project",
        default="flood-forecasting",
        help="W&B project name",
    )
    parser.add_argument(
        "--artifact-name",
        default="flood-dataset",
        help="Name for the W&B artifact",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Skip merge and upload local data only (replaces everything)",
    )
    args = parser.parse_args()

    upload_dataset(
        db_path=args.db_path,
        project=args.project,
        artifact_name=args.artifact_name,
        full_refresh=args.full_refresh,
    )
