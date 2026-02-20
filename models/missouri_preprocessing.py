"""
Missouri Flood Preprocessing Pipeline
Loads raw data from W&B, filters, scales, and uploads
preprocessed flat arrays as a W&B artifact for use by any model.
"""

import polars as pl
import wandb
import numpy as np
import joblib
import os
from sklearn.preprocessing import StandardScaler
from helper_functions.helpers import pull_wandb
# Config

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

INPUT_COLS = [
    "latitude",
    "longitude",
    "streamflow_cfs_mean",
    "gage_height_ft_mean",
    "precipitation_mm",
    "temperature_c",
    "specific_humidity_kgkg",
]

TARGET = "streamflow_cfs_target_24h"

TRAIN_SPLIT = 0.8
VAL_SPLIT = 0.9
# Start W&B run
config={
            "train_split": TRAIN_SPLIT,
            "val_split": VAL_SPLIT,
            "input_cols": INPUT_COLS,
            "target": TARGET}

df = pull_wandb(config, "flood_model_missouri")

print(f"Full dataset: {df.shape[0]:,} rows x {df.shape[1]} columns")
print(f"Date range: {df['observation_hour'].min()} to {df['observation_hour'].max()}")

# 2. Select features and filter

# Select columns
df = df.select(FEATURES)

# Drop rows missing gage height or streamflow (most rows are weather-only)
df = df.drop_nulls(subset=["gage_height_ft_mean", "streamflow_cfs_mean"])
print(f"\nAfter dropping nulls: {df.shape[0]:,} rows x {df.shape[1]} columns")

# Sort by site and time (required for LSTM sequences)
df = df.sort(["site_id", "observation_hour"])

# Create target: streamflow 24 hours (1 day) ahead (shift within each site)
df = df.with_columns(
    pl.col("streamflow_cfs_mean")
    .shift(-24)
    .over("site_id")
    .alias("streamflow_cfs_target_24h")
)

# Drop rows where target is null (last 24 rows per site from the shift)
df = df.drop_nulls(subset=[TARGET])

print(f"\nAfter creating target: {df.shape[0]:,} rows x {df.shape[1]} columns")
print(f"Sites: {df['site_id'].n_unique()}")
print(f"Date range: {df['observation_hour'].min()} to {df['observation_hour'].max()}")

# 3. Split

# Split by time: first 80% train, next 10% val, last 10% test
split_dates = df["observation_hour"].quantile(TRAIN_SPLIT), df["observation_hour"].quantile(VAL_SPLIT)
train_df = df.filter(pl.col("observation_hour") < split_dates[0])
val_df = df.filter(
    (pl.col("observation_hour") >= split_dates[0])
    & (pl.col("observation_hour") < split_dates[1])
)
test_df = df.filter(pl.col("observation_hour") >= split_dates[1])

print(f"\nTrain: {len(train_df):,} rows ({train_df['observation_hour'].min()} to {train_df['observation_hour'].max()})")
print(f"Val:   {len(val_df):,} rows ({val_df['observation_hour'].min()} to {val_df['observation_hour'].max()})")
print(f"Test:  {len(test_df):,} rows ({test_df['observation_hour'].min()} to {test_df['observation_hour'].max()})")

# 4. Scale features

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

# Also scale the target (helps model training stability)
target_scaler = StandardScaler()
train_y_scaled = target_scaler.fit_transform(train_y.reshape(-1, 1)).flatten()
val_y_scaled = target_scaler.transform(val_y.reshape(-1, 1)).flatten()
test_y_scaled = target_scaler.transform(test_y.reshape(-1, 1)).flatten()

# Also save site_ids so model scripts can use them for create_sequences()
# Convert to numpy array (site_id is string, so we preserve dtype)
train_sites = train_df["site_id"].to_numpy()
val_sites = val_df["site_id"].to_numpy()
test_sites = test_df["site_id"].to_numpy()

# 5. Save and upload artifact

os.makedirs("pipeline_outputs", exist_ok=True)

# Save scaled arrays
np.save("pipeline_outputs/train_X_scaled.npy", train_X_scaled)
np.save("pipeline_outputs/val_X_scaled.npy", val_X_scaled)
np.save("pipeline_outputs/test_X_scaled.npy", test_X_scaled)
np.save("pipeline_outputs/train_y_scaled.npy", train_y_scaled)
np.save("pipeline_outputs/val_y_scaled.npy", val_y_scaled)
np.save("pipeline_outputs/test_y_scaled.npy", test_y_scaled)

# Save site ids (needed by models that build per-site sequences)
np.save("pipeline_outputs/train_sites.npy", train_sites)
np.save("pipeline_outputs/val_sites.npy", val_sites)
np.save("pipeline_outputs/test_sites.npy", test_sites)

# Save scalers
joblib.dump(scaler, "pipeline_outputs/feature_scaler.pkl")
joblib.dump(target_scaler, "pipeline_outputs/target_scaler.pkl")

# Log everything as a single W&B artifact
artifact = wandb.Artifact(
    name="flood-preprocessed-missouri",
    type="preprocessed-dataset",
    description="Scaled flat arrays + scalers + site ids. Model-agnostic. Target=24h ahead streamflow."
)
artifact.add_dir("pipeline_outputs")
run.log_artifact(artifact, aliases=["latest"])

run.finish()
print("\nPipeline outputs uploaded to W&B.")
