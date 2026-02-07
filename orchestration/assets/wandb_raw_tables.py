"""W&B artifact sync for raw database tables.

Exports each raw DuckDB table as a separate W&B artifact containing
year-partitioned parquet files (for large tables) or a single parquet
file (for small tables). Uses DuckDB's native COPY TO PARQUET to avoid
loading data into Python memory.
"""

import shutil
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


WANDB_STAGING = (
    Path.home() / "Library" / "Application Support" / "wandb" / "artifacts" / "staging"
)

# One artifact per table. partition_col=None means single-file export.
TABLES = {
    "site_metadata":            {"artifact": "raw-site-metadata",     "partition_col": None},
    "nldas3_watershed_mapping": {"artifact": "raw-watershed-mapping", "partition_col": None},
    "streamflow_daily":         {"artifact": "raw-streamflow-daily",  "partition_col": None},
    "nldas3_forcing":           {"artifact": "raw-nldas3-forcing",    "partition_col": "datetime"},
    "streamflow_15min":         {"artifact": "raw-streamflow-15min",  "partition_col": "datetime"},
}


class WandbRawTablesConfig(Config):
    """Configuration for raw tables W&B upload."""

    project: str = "flood-forecasting"


def clean_wandb_temp(wandb_dir: Path) -> None:
    """Delete local wandb run directories and staging cache."""
    if wandb_dir.exists():
        for d in wandb_dir.glob("run-*"):
            shutil.rmtree(d, ignore_errors=True)
    if WANDB_STAGING.exists():
        shutil.rmtree(WANDB_STAGING, ignore_errors=True)


def export_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    partition_col: str | None,
    export_dir: Path,
    context: AssetExecutionContext,
) -> tuple[Path, dict]:
    """Export a table to parquet file(s). Returns (directory, stats)."""
    table_dir = export_dir / table
    if table_dir.exists():
        shutil.rmtree(table_dir)
    table_dir.mkdir(parents=True)

    if partition_col is None:
        out_path = table_dir / f"{table}.parquet"
        row_count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        con.execute(
            f"COPY raw.{table} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        context.log.info(f"  {table}: {row_count:,} rows, {size_mb:.1f} MB")
        return table_dir, {"row_count": row_count, "size_mb": round(size_mb, 2), "files": 1}

    # Year-partitioned export
    min_year, max_year = con.execute(
        f"SELECT MIN(YEAR({partition_col})), MAX(YEAR({partition_col})) FROM raw.{table}"
    ).fetchone()

    total_rows = 0
    total_size = 0.0
    file_count = 0

    for year in range(min_year, max_year + 1):
        row_count = con.execute(
            f"SELECT COUNT(*) FROM raw.{table} WHERE YEAR({partition_col}) = {year}"
        ).fetchone()[0]
        if row_count == 0:
            continue

        out_path = table_dir / f"{table}_{year}.parquet"
        con.execute(
            f"COPY (SELECT * FROM raw.{table} WHERE YEAR({partition_col}) = {year}) "
            f"TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        total_rows += row_count
        total_size += size_mb
        file_count += 1
        context.log.info(f"  {table}/{year}: {row_count:,} rows, {size_mb:.1f} MB")

    context.log.info(
        f"  {table} total: {total_rows:,} rows, {total_size:.1f} MB ({file_count} files)"
    )
    return table_dir, {"row_count": total_rows, "size_mb": round(total_size, 2), "files": file_count}


def upload_artifact(
    table: str,
    artifact_name: str,
    table_dir: Path,
    stats: dict,
    project: str,
    context: AssetExecutionContext,
) -> None:
    """Upload a table's parquet files as a single wandb artifact."""
    run = wandb.init(
        project=project,
        job_type="raw-tables-sync",
        config={"table": table, **stats},
    )

    artifact = wandb.Artifact(
        name=artifact_name,
        type="dataset",
        description=f"Raw table: raw.{table}",
        metadata={
            **stats,
            "source_table": f"raw.{table}",
            "uploaded_at": datetime.now().isoformat(),
        },
    )

    artifact.add_dir(str(table_dir), name=table)
    run.log_artifact(artifact, aliases=["latest"])
    run.finish()
    context.log.info(f"  Uploaded -> {project}/{artifact_name}")


def delete_old_versions(
    api: wandb.Api, project: str, artifact_name: str, context: AssetExecutionContext
) -> None:
    """Delete old versions of an artifact, skipping those with aliases."""
    try:
        artifact_path = f"{project}/{artifact_name}"
        versions = api.artifacts(type_name="dataset", name=artifact_path)

        for artifact in versions:
            if artifact.aliases:
                continue
            try:
                context.log.info(f"Deleting old version: {artifact_name}:{artifact.version}")
                artifact.delete()
            except Exception as e:
                context.log.debug(f"Could not delete {artifact.version}: {e}")
    except Exception as e:
        context.log.warning(f"Could not clean old versions of {artifact_name}: {e}")


@asset(
    group_name="sync",
    description="Upload raw tables to W&B as separate per-table artifacts with year-partitioned parquet files",
    compute_kind="wandb",
    deps=["usgs_site_metadata", "usgs_streamflow_15min", "usgs_streamflow_daily",
          "nldas3_forcing", "nldas3_watershed_mapping"],
)
def wandb_raw_tables(
    context: AssetExecutionContext,
    config: WandbRawTablesConfig,
) -> MaterializeResult:
    """Export each raw table and upload as its own W&B artifact.

    Uses DuckDB COPY TO PARQUET (streaming, no Python memory overhead).
    Large tables are year-partitioned. Cleans up disk between uploads.
    """
    db_path = get_db_path()
    project_root = Path(db_path).parent
    export_dir = project_root / ".cache" / "wandb_export"
    wandb_dir = project_root / "wandb"
    export_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path, read_only=True)
    all_metadata = {}

    for table, cfg in TABLES.items():
        context.log.info(f"=== {table} ===")

        # Export
        context.log.info("Exporting...")
        table_dir, stats = export_table(con, table, cfg["partition_col"], export_dir, context)

        # Upload
        context.log.info("Uploading to W&B...")
        upload_artifact(table, cfg["artifact"], table_dir, stats, config.project, context)

        all_metadata[table] = stats

        # Clean up between tables to save disk space
        shutil.rmtree(table_dir, ignore_errors=True)
        clean_wandb_temp(wandb_dir)
        context.log.info(f"  Cleaned up local files for {table}")

    con.close()
    if export_dir.exists():
        shutil.rmtree(export_dir)

    # Clean up old artifact versions
    api = wandb.Api()
    for cfg in TABLES.values():
        delete_old_versions(api, config.project, cfg["artifact"], context)

    total_rows = sum(m["row_count"] for m in all_metadata.values())
    total_size = sum(m["size_mb"] for m in all_metadata.values())

    context.log.info(
        f"Done! {len(TABLES)} tables synced ({total_rows:,} rows, {total_size:.1f} MB)"
    )

    return MaterializeResult(
        metadata={
            "total_rows": total_rows,
            "total_size_mb": MetadataValue.float(round(total_size, 2)),
            "tables": MetadataValue.json(all_metadata),
        }
    )
