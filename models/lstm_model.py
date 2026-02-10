"""
LSTM Flood Forecasting Model (PyTorch)
Predicts next-hour gage height using historical weather and streamflow data.
Target: gage_height_ft_target_1h
"""

import polars as pl
import wandb

# ── Step 1: Load data from wandb ────────────────────────────────────────────

api = wandb.Api()
artifact = api.artifact("flood-forecasting/flood-dataset:latest")
artifact_dir = artifact.download()

df = pl.read_parquet(f"{artifact_dir}/flood_model.parquet")

print(f"Full dataset: {df.shape[0]:,} rows x {df.shape[1]} columns")
print(f"Date range: {df['observation_hour'].min()} to {df['observation_hour'].max()}")

# ── Step 2: Select features and filter ──────────────────────────────────────

FEATURES = [
    "site_id",
    "observation_hour",
    "latitude",
    "longitude",
    "streamflow_cfs_mean",
    "gage_height_ft_mean",
    "precipitation_mm",
    "temperature_c",
    "specific_humidity_kgkg",
]

# Select columns
df = df.select(FEATURES)

# Drop rows missing gage height or streamflow (most rows are weather-only)
df = df.drop_nulls(subset=["gage_height_ft_mean", "streamflow_cfs_mean"])
print(f"After dropping nulls: {df.shape[0]:,} rows")

# Filter to last 2 years
max_date = df["observation_hour"].max()
two_years_ago = max_date - pl.duration(days=730)
df = df.filter(pl.col("observation_hour") >= two_years_ago)
print(f"After 2-year filter: {df.shape[0]:,} rows")

# Sample 100 sites (pick those with the most data for better sequences)
site_counts = df.group_by("site_id").len().sort("len", descending=True)
top_sites = site_counts.head(100)["site_id"]
df = df.filter(pl.col("site_id").is_in(top_sites.to_list()))
print(f"After top 100 sites: {df.shape[0]:,} rows")

# Remove duplicates
df = df.unique(subset=["site_id", "observation_hour"], keep="first")

# Sort by site and time (required for LSTM sequences)
df = df.sort(["site_id", "observation_hour"])

# Create target: next hour's gage height (shift within each site)
df = df.with_columns(
    pl.col("gage_height_ft_mean")
    .shift(-1)
    .over("site_id")
    .alias("gage_height_ft_target_1h")
)

TARGET = "gage_height_ft_target_1h"

# Drop rows where target is null (last row per site from the shift)
df = df.drop_nulls(subset=[TARGET])

print(f"\nFiltered dataset: {df.shape[0]:,} rows x {df.shape[1]} columns")
print(f"Sites: {df['site_id'].n_unique()}")
print(f"Date range: {df['observation_hour'].min()} to {df['observation_hour'].max()}")
print(f"Null counts:\n{df.null_count()}")

# ── Step 3: Scale features and create LSTM sequences ────────────────────────

import numpy as np
from sklearn.preprocessing import StandardScaler

# Columns the LSTM will use as input features (numeric only, no IDs or timestamps)
INPUT_COLS = [
    "latitude",
    "longitude",
    "streamflow_cfs_mean",
    "gage_height_ft_mean",
    "precipitation_mm",
    "temperature_c",
    "specific_humidity_kgkg",
]

WINDOW_SIZE = 24  # Use past 24 hours to predict next hour's gage height

# Split by time: first 80% train, next 10% val, last 10% test
# (time-based split avoids data leakage - no future data in training)
split_dates = df["observation_hour"].quantile(0.8), df["observation_hour"].quantile(0.9)
train_df = df.filter(pl.col("observation_hour") < split_dates[0])
val_df = df.filter(
    (pl.col("observation_hour") >= split_dates[0])
    & (pl.col("observation_hour") < split_dates[1])
)
test_df = df.filter(pl.col("observation_hour") >= split_dates[1])

print(f"\nTrain: {len(train_df):,} rows ({train_df['observation_hour'].min()} to {train_df['observation_hour'].max()})")
print(f"Val:   {len(val_df):,} rows ({val_df['observation_hour'].min()} to {val_df['observation_hour'].max()})")
print(f"Test:  {len(test_df):,} rows ({test_df['observation_hour'].min()} to {test_df['observation_hour'].max()})")

# Fit scaler on training data only (prevents data leakage)
scaler = StandardScaler()
scaler.fit(train_df.select(INPUT_COLS).to_numpy())

# Scale all splits
train_X_scaled = scaler.transform(train_df.select(INPUT_COLS).to_numpy())
val_X_scaled = scaler.transform(val_df.select(INPUT_COLS).to_numpy())
test_X_scaled = scaler.transform(test_df.select(INPUT_COLS).to_numpy())

train_y = train_df[TARGET].to_numpy()
val_y = val_df[TARGET].to_numpy()
test_y = test_df[TARGET].to_numpy()

# Also scale the target (helps LSTM training stability)
target_scaler = StandardScaler()
train_y_scaled = target_scaler.fit_transform(train_y.reshape(-1, 1)).flatten()
val_y_scaled = target_scaler.transform(val_y.reshape(-1, 1)).flatten()
test_y_scaled = target_scaler.transform(test_y.reshape(-1, 1)).flatten()


def create_sequences(X, y, site_ids, window_size):
    """Create sliding window sequences per site.

    For each site, slides a window of `window_size` hours across the data.
    Each sequence uses `window_size` hours of features to predict the target
    at the last timestep.

    Returns:
        X_seq: array of shape (num_sequences, window_size, num_features)
        y_seq: array of shape (num_sequences,)
    """
    X_sequences, y_sequences = [], []
    unique_sites = np.unique(site_ids)

    for site in unique_sites:
        mask = site_ids == site
        site_X = X[mask]
        site_y = y[mask]

        # Only create sequences if site has enough data
        if len(site_X) < window_size:
            continue

        for i in range(len(site_X) - window_size):
            X_sequences.append(site_X[i : i + window_size])
            y_sequences.append(site_y[i + window_size - 1])

    return np.array(X_sequences), np.array(y_sequences)


# Create sequences for each split
train_sites = train_df["site_id"].to_numpy()
val_sites = val_df["site_id"].to_numpy()
test_sites = test_df["site_id"].to_numpy()

print("\nCreating sequences...")
X_train, y_train = create_sequences(train_X_scaled, train_y_scaled, train_sites, WINDOW_SIZE)
X_val, y_val = create_sequences(val_X_scaled, val_y_scaled, val_sites, WINDOW_SIZE)
X_test, y_test = create_sequences(test_X_scaled, test_y_scaled, test_sites, WINDOW_SIZE)

print(f"Train sequences: {X_train.shape}")
print(f"Val sequences:   {X_val.shape}")
print(f"Test sequences:  {X_test.shape}")
