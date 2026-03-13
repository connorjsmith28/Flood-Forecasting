import wandb
import polars as pl
from pathlib import Path
import duckdb

def pull_wandb(file_name: str, file_path: str = None, n_rows: int | None = None) -> pl.DataFrame:
    api = wandb.Api(timeout=60)
    artifact = api.artifact(f"connorjsmith28-rice-university/flood-forecasting/{file_path}:latest")
    artifact_dir = artifact.download()
    df = pl.read_parquet(
        f"{artifact_dir}/{file_name}.parquet",
    )
    # filter to site 06923250 and only noon observations, will change this later
    df = df.filter(pl.col("site_id").cast(pl.Utf8) == "06923250")
    df = df.filter(pl.col("observation_hour").dt.hour() == 12)
    df = df.sort("observation_hour")
    return df.head(n_rows)
 

def pull_duckdb(file_name: str, limit: int | None = None) -> pl.DataFrame:
    """Query the local DuckDB file and return a Polars DataFrame.

    The DuckDB file is at data/database/database.duckdb relative to the repository root.
    """
    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "data" / "database" / "database.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")

    # open connection after verifying path
    con = duckdb.connect(database=str(db_path), read_only=True)
    limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""
    # hard-coded filters: site_id '06923250' and observation_hour at 12:00
    sql = (
        f"SELECT * FROM {file_name} "
        f"WHERE site_id = '06923250' AND EXTRACT(HOUR FROM observation_hour) = 12 "
        f"ORDER BY observation_hour{limit_clause};"
    )

    try:
        arrow_tbl = con.execute(sql).fetch_arrow_table()
        df = pl.from_arrow(arrow_tbl)
    finally:
        con.close()

    return df
