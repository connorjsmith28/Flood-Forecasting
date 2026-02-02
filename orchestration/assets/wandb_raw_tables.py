"""W&B artifact sync for raw database tables."""

import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import wandb
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Config,
)

from orchestration.utils import get_db_path


# Tables to sync (excluding weather_forcing which has issues)
RAW_TABLES = [
    "site_metadata",
    "streamflow_15min",
    "streamflow_daily",
]


class WandbRawTablesConfig(Config):
    """Configuration for raw tables W&B upload."""

    project: str = "flood-forecasting"
    artifact_name: str = "raw-tables"


def get_table_stats(con: duckdb.DuckDBPyConnection, table_name: str) -> dict:
    """Get basic statistics for a table."""
    result = con.execute(f"""
        SELECT COUNT(*) as row_count
        FROM raw.{table_name}
    """).fetchone()

    return {"row_count": result[0]}


def delete_old_versions(
    api: wandb.Api, project: str, artifact_name: str, context: AssetExecutionContext
):
    """Delete old versions of an artifact, skipping those with aliases."""
    try:
        artifact_path = f"{project}/{artifact_name}"
        versions = api.artifacts(type_name="dataset", name=artifact_path)

        for artifact in versions:
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
    description="Upload raw tables (site_metadata, streamflow_15min, streamflow_daily) to W&B as artifacts",
    compute_kind="wandb",
    deps=["usgs_site_metadata", "usgs_streamflow_15min", "usgs_streamflow_daily"],
)
def wandb_raw_tables(
    context: AssetExecutionContext,
    config: WandbRawTablesConfig,
) -> MaterializeResult:
    """Export raw tables and upload as W&B artifact.

    Uploads all raw tables (except weather_forcing) as parquet files
    in a single artifact for easy download and use in training.
    """
    db_path = get_db_path()
    con = duckdb.connect(db_path, read_only=True)

    table_metadata = {}
    total_rows = 0
    total_size_mb = 0.0

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        parquet_files = []

        for table_name in RAW_TABLES:
            context.log.info(f"Exporting raw.{table_name}...")

            # Get stats
            stats = get_table_stats(con, table_name)
            row_count = stats["row_count"]
            total_rows += row_count

            # Export to parquet
            df = con.execute(f"SELECT * FROM raw.{table_name}").pl()
            parquet_path = tmpdir_path / f"{table_name}.parquet"
            df.write_parquet(parquet_path)

            file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
            total_size_mb += file_size_mb

            parquet_files.append(parquet_path)
            table_metadata[table_name] = {
                "row_count": row_count,
                "file_size_mb": round(file_size_mb, 2),
            }

            context.log.info(
                f"  {table_name}: {row_count:,} rows, {file_size_mb:.1f} MB"
            )

        con.close()

        # Initialize W&B run
        run = wandb.init(
            project=config.project,
            job_type="raw-tables-sync",
            config={
                "total_rows": total_rows,
                "total_size_mb": round(total_size_mb, 2),
                "tables": list(RAW_TABLES),
                "table_details": table_metadata,
            },
        )

        # Create artifact
        artifact = wandb.Artifact(
            name=config.artifact_name,
            type="dataset",
            description="Raw flood forecasting tables (site_metadata, streamflow_15min, streamflow_daily)",
            metadata={
                "total_rows": total_rows,
                "total_size_mb": round(total_size_mb, 2),
                "tables": table_metadata,
                "uploaded_at": datetime.now().isoformat(),
            },
        )

        # Add all parquet files
        for parquet_path in parquet_files:
            artifact.add_file(str(parquet_path))

        # Log artifact with "latest" alias
        run.log_artifact(artifact, aliases=["latest"])

        # Log metrics
        wandb.log(
            {
                "raw_tables_total_rows": total_rows,
                "raw_tables_total_size_mb": total_size_mb,
            }
        )

        run.finish()

    # Clean up old versions
    api = wandb.Api()
    delete_old_versions(api, config.project, config.artifact_name, context)

    context.log.info(
        f"Uploaded {len(RAW_TABLES)} tables ({total_rows:,} total rows, "
        f"{total_size_mb:.1f} MB) to {config.project}/{config.artifact_name}"
    )

    return MaterializeResult(
        metadata={
            "total_rows": total_rows,
            "total_size_mb": MetadataValue.float(round(total_size_mb, 2)),
            "tables": MetadataValue.json(table_metadata),
        }
    )
