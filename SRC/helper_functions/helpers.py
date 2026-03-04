import wandb
import polars as pl
from pathlib import Path
import duckdb

def pull_wandb(
    file_name: str,
    file_path: str = None,
    site: str | None = None,
    sites: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str | None = None,
) -> pl.DataFrame:
    api = wandb.Api(timeout=60)
    artifact = api.artifact(f"connorjsmith28-rice-university/flood-forecasting/{file_path}:latest")
    artifact_dir = artifact.download()
    df = pl.read_parquet(f"{artifact_dir}/{file_name}.parquet")

    # Filter by site(s)
    if sites is not None:
        df = df.filter(pl.col("site_id").cast(pl.Utf8).is_in(sites))
    elif site is not None:
        df = df.filter(pl.col("site_id").cast(pl.Utf8) == site)
    
    # Filter by date range
    if start_date is not None:
        df = df.filter(pl.col("observation_hour") >= pl.lit(start_date).str.strptime(pl.Datetime("us"), "%Y-%m-%d"))
    if end_date is not None:
        df = df.filter(pl.col("observation_hour") <= pl.lit(end_date).str.strptime(pl.Datetime("us"), "%Y-%m-%d"))
    
    # Filter to noon observations if daily frequency
    if frequency == "daily":
        df = df.filter(pl.col("observation_hour").dt.hour() == 12)

    return df.sort(["site_id", "observation_hour"])

def pull_duckdb(
    file_name: str,
    site: int | str | None = None,
    sites: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str | None = None,
) -> pl.DataFrame:
    """Query the local DuckDB file and return a Polars DataFrame.

    Args:
        file_name: DuckDB table name (e.g. 'wandb.flood_model_missouri').
        site: Single site ID string (legacy, kept for backward compat).
        sites: List of site ID strings. If provided, overrides `site`.
        start_date: Inclusive start date string, e.g. '2015-01-01'.
        end_date: Inclusive end date string, e.g. '2024-12-31'.
        frequency: Frequency of the data, e.g. 'daily' or '15min'.
    The DuckDB file `flood_forecasting.duckdb` is expected at the repository root.
    """
    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "flood_forecasting.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")

    con = duckdb.connect(database=str(db_path), read_only=True)

    # Build WHERE clause
    where_parts = []
    if frequency == "daily":
        where_parts.append("EXTRACT(HOUR FROM observation_hour) = 12")
    if sites is not None:
        quoted = ", ".join(f"'{s}'" for s in sites)
        where_parts.append(f"site_id IN ({quoted})")
    elif site is not None:
        where_parts.append(f"site_id = '{site}'")
    if start_date is not None:
        where_parts.append(f"observation_hour >= '{start_date}'")
    if end_date is not None:
        where_parts.append(f"observation_hour <= '{end_date}'")

    if where_parts:
        where_clause = " AND ".join(where_parts)
        sql = (
            f"SELECT * FROM {file_name} "
            f"WHERE {where_clause} "
            f"ORDER BY site_id, observation_hour;"
        )
    else:
        sql = (
            f"SELECT * FROM {file_name} "
            f"ORDER BY site_id, observation_hour;"
        )

    try:
        arrow_tbl = con.execute(sql).fetch_arrow_table()
        df = pl.from_arrow(arrow_tbl)
    finally:
        con.close()

    return df
