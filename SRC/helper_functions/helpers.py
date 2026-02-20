import wandb
import polars as pl
from pathlib import Path
import re
import wandb
import duckdb

def pull_wandb(
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
        job_type="preprocessing"
    )
    artifact = run.use_artifact(
        f"connorjsmith28-rice-university/flood-forecasting/{file_name}:latest"
    )
    artifact_dir = artifact.download()
    return pl.read_parquet(
        f"{artifact_dir}/{file_name}.parquet",
        n_rows=n_rows,
    )

 
def sanitize_table_name(name: str) -> str:
    n = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if re.match(r"^[0-9]", n):
        n = "t_" + n
    return n.lower()


def pull_wandb_to_duckdb(artifact_strings: list[str], db_path: Path):
    api = wandb.Api()
    artifact_strings = ["flood-forecasting/flood-dataset","flood-forecasting/flood-dataset-missouri","flood-forecasting/flood-dataset-daily"]
    # use the repository root DuckDB file
    db_path = Path(__file__).resolve().parents[2] / "flood_forecasting.duckdb"
    con = duckdb.connect(str(db_path))

    # ensure a dedicated schema for wandb imports
    con.execute("CREATE SCHEMA IF NOT EXISTS wandb")

    files_processed = 0
    for artifact_str in artifact_strings:
        artifact = api.artifact(artifact_str+":latest")
        artifact_dir = Path(artifact.download())
        print(f"{artifact_str} downloaded to: {artifact_dir}")
        for p in artifact_dir.rglob("*"):
            print(f"Processing {p}...")
            if not p.is_file():
                continue
        
            suffix = p.suffix.lower()
            table = sanitize_table_name(p.stem)
            full_table = f"wandb.{table}"
            print(f"Importing {p.name} as table {full_table}...")
            try:
                if suffix == ".parquet":
                    con.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_parquet('{p.as_posix()}')")
                    files_processed += 1
                elif suffix in (".csv", ".txt"):
                    con.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_csv_auto('{p.as_posix()}')")
                    files_processed += 1
                else:
                    print(f"skipped {p.name}")
                    continue
                print(f"Imported {p.name} -> table: {full_table}")
            except Exception as e:
                print(f"Failed to import {p}: {e}")

    con.close()
    print(f"Wrote {files_processed} files to DuckDB: {db_path}")
