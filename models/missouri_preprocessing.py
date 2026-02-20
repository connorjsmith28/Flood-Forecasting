"""
Missouri Flood Preprocessing Pipeline
Loads raw data from W&B, filters, scales, and uploads
preprocessed flat arrays as a W&B artifact for use by any model.
"""

import wandb
import numpy as np
import joblib
import os
from helper_functions.preprocessing import preprocess_missouri

# Config
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
# Limit rows read from W&B (None = full dataset). Example: 100_000 for faster runs.
N_ROWS = None

config = {
    "train_split": TRAIN_SPLIT,
    "val_split": VAL_SPLIT,
    "input_cols": INPUT_COLS,
    "target": TARGET,
    "n_rows": N_ROWS,
}

# Pipeline: load -> select -> filter -> sort -> add target -> split -> scale (TorchStandardScaler)
(
    train_X_scaled,
    val_X_scaled,
    test_X_scaled,
    train_y_scaled,
    val_y_scaled,
    test_y_scaled,
    train_sites,
    val_sites,
    test_sites,
    scaler,
    target_scaler,
) = preprocess_missouri(config)

print(f"\nTrain: {train_X_scaled.shape[0]:,} | Val: {val_X_scaled.shape[0]:,} | Test: {test_X_scaled.shape[0]:,}")

# 5. Save and upload artifact

os.makedirs("pipeline_outputs", exist_ok=True)

# Save scaled arrays (tensors from pipeline -> numpy for .npy)
np.save("pipeline_outputs/train_X_scaled.npy", train_X_scaled.numpy())
np.save("pipeline_outputs/val_X_scaled.npy", val_X_scaled.numpy())
np.save("pipeline_outputs/test_X_scaled.npy", test_X_scaled.numpy())
np.save("pipeline_outputs/train_y_scaled.npy", train_y_scaled.numpy())
np.save("pipeline_outputs/val_y_scaled.npy", val_y_scaled.numpy())
np.save("pipeline_outputs/test_y_scaled.npy", test_y_scaled.numpy())

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
