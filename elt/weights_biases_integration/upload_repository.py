from pathlib import Path
import wandb
from typing import Optional
import tempfile
import duckdb
import uuid


def upload_path_to_wandb(path_str: str, artifact_name: str, artifact_type: str, project: str = "flood-forecasting", description: Optional[str] = None) -> bool:
    p = Path(path_str).expanduser()
    try:
        p = p.resolve(strict=True)
    except FileNotFoundError:
        print(f"Path not found: {path_str}")
        return False
    run = None
    try:
        run = wandb.init(project=project, job_type="upload_repository")
        artifact = wandb.Artifact(name=artifact_name, type=artifact_type, description=description)
        if p.is_file():
            artifact.add_file(str(p))
        else:
            artifact.add_dir(str(p))

        run.log_artifact(artifact)
        print(f"Uploaded {p} as artifact '{artifact_name}:latest' to project '{project}'")
        return True
    except Exception as e:
        print(f"Failed to upload to W&B: {e}")
        return False
    finally:
        if run is not None:
            try:
                run.finish()
            except Exception:
                pass


def main():
    table = input("Enter DuckDB table name (schema.table or table): ").strip()
    if not table:
        print("No table provided. Exiting.")
        return

    db_path = Path(__file__).resolve().parents[2] / "flood_forecasting.duckdb"
    if not db_path.exists():
        print(f"DuckDB not found at {db_path}")
        return

    con = None
    tmpfile_path = None
    success = False
    try:
        con = duckdb.connect(str(db_path))
        # verify table exists
        con.execute(f"SELECT * FROM {table} LIMIT 0")

        tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        tmpfile_path = Path(tmpfile.name)
        tmpfile.close()
        print(f"Exporting {table} -> {tmpfile_path}...")
        con.execute(f"COPY (SELECT * FROM {table}) TO '{tmpfile_path.as_posix()}' (FORMAT PARQUET)")

        default_name = table.replace('.', '_')
        name = input(f"Artifact name [{default_name}]: ").strip() or default_name
        atype = "dataset"
        project = "flood-forecasting"
        desc = input("Optional description (press enter to skip): ").strip() or None

        success = upload_path_to_wandb(str(tmpfile_path), name, atype, project, desc)
        if success:
            print("Upload complete.")
        else:
            print("Upload failed.")
    except Exception as e:
        print(f"Failed exporting table from DuckDB: {e}")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        if tmpfile_path is not None and tmpfile_path.exists():
            try:
                tmpfile_path.unlink()
            except Exception:
                pass


if __name__ == "__main__":
    main()
