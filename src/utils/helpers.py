import wandb
import polars as pl
from pathlib import Path
import duckdb
import numpy as np

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
    if ":" in file_path:
        artifact_ref = f"connorjsmith28-rice-university/flood-forecasting/{file_path}"
    else:
        artifact_ref = f"connorjsmith28-rice-university/flood-forecasting/{file_path}:latest"
    
    artifact = api.artifact(artifact_ref)
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
    The DuckDB file is at data/database/database.duckdb relative to the repository root.
    """
    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "data" / "database" / "database.duckdb"
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

def create_sequences(
    X: pl.DataFrame,
    y: pl.DataFrame | None,
    window_size: int,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray]:

    """Create sliding window sequences per site for sequence models (LSTM, GRU, Transformer, etc.).

    For each site, slides a window of `window_size` timesteps across the data.
    Each sequence uses `window_size` timesteps of features to predict the target
    at the final timestep of the window.

    Args:
        X: Polars DataFrame with a 'site_id' column and feature columns.
        y: Polars DataFrame with a single target column, row-aligned with X. If None,
           the returned y_seq will also be None (useful for inference).
        window_size: Number of timesteps per sequence (e.g. 24 for 24-hour lookback).

    Returns:
        X_seq: np.ndarray of shape (num_sequences, window_size, num_features)
        y_seq: np.ndarray of shape (num_sequences,), or None if y was not provided.

    Notes:
        - Sites with fewer rows than `window_size` are skipped entirely.
        - Sequences do not cross site boundaries.
        - Compatible with PyTorch (LSTM, GRU) and TensorFlow/Keras 3D input.
    """
    feature_cols = [c for c in X.columns if c not in ("site_id", "observation_hour")]

    if y is not None:
        target_col = y.columns[0]
        combined = X.with_columns(y[target_col])
    else:
        target_col = None
        combined = X

    X_sequences, y_sequences, site_id_sequences = [], [], []
    for site in combined["site_id"].unique().sort().to_list():
        site_df = combined.filter(pl.col("site_id") == site)
        site_X = site_df.select(feature_cols).to_numpy()
        if len(site_X) < window_size:
            continue
        site_y = site_df[target_col].to_numpy() if target_col is not None else None
        for i in range(len(site_X) - window_size):
            X_sequences.append(site_X[i : i + window_size])
            if site_y is not None:
                y_sequences.append(site_y[i + window_size - 1])
            site_id_sequences.append(site)

    y_out = np.array(y_sequences) if y is not None else None
    return np.array(X_sequences), y_out, np.array(site_id_sequences)