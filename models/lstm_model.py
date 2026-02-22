"""
LSTM Flood Forecasting Model (PyTorch)
Predicts streamflow 24 hours (1 day) in advance using historical weather and streamflow data.
Target: streamflow_cfs_target_24h

Downloads preprocessed data from the W&B artifact produced by missouri_preprocessing.py
"""

import wandb
import numpy as np
import joblib
from SRC.helper_functions.helpers import shift_df
from SRC.helper_functions.preprocessing import processor
config = {
    "input_cols": [
        "latitude",
        "longitude",
        "streamflow_cfs_mean",
        "gage_height_ft_mean",
        "precipitation_mm",
        "temperature_c",
        "specific_humidity_kgkg",
    ], 
    "target": "streamflow_cfs_mean",
    "train_split": 0.8,
    "val_split": 0.9,
    "n_rows": 100,
    "file_path": "flood-dataset-missouri",
    "file_name": "flood_model_missouri",
    "table": "wandb.flood_model_missouri"
}

pcr = processor(config)
pcr.pull_duckdb()
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
    feature_scaler,
    target_scaler,
) = pcr.return_outputs()

df = shift_df(train_X_scaled, config["input_cols"], 7)
df
#example of how to set up your model sasha

##

























'''# 1. Download preprocessed artifact from W&B

run = wandb.init(
    project="flood-forecasting",
    entity="connorjsmith28-rice-university",
    job_type="training",
    config={
        "window_size": 168,
        "model": "lstm",
    }
)

# Download the preprocessed artifact (created by missouri_preprocessing.py)
artifact = run.use_artifact("flood-preprocessed-missouri:latest")
artifact_dir = artifact.download()

# Load scaled arrays
train_X_scaled = np.load(f"{artifact_dir}/train_X_scaled.npy")
val_X_scaled   = np.load(f"{artifact_dir}/val_X_scaled.npy")
test_X_scaled  = np.load(f"{artifact_dir}/test_X_scaled.npy")
train_y_scaled = np.load(f"{artifact_dir}/train_y_scaled.npy")
val_y_scaled   = np.load(f"{artifact_dir}/val_y_scaled.npy")
test_y_scaled  = np.load(f"{artifact_dir}/test_y_scaled.npy")

# Load site ids (needed for create_sequences)
# site_id is string, so allow_pickle=True preserves the string dtype
train_sites = np.load(f"{artifact_dir}/train_sites.npy", allow_pickle=True)
val_sites   = np.load(f"{artifact_dir}/val_sites.npy", allow_pickle=True)
test_sites  = np.load(f"{artifact_dir}/test_sites.npy", allow_pickle=True)

# Load scalers (needed to convert predictions back to real CFS values)
target_scaler  = joblib.load(f"{artifact_dir}/target_scaler.pkl")
feature_scaler = joblib.load(f"{artifact_dir}/feature_scaler.pkl")

print(f"Downloaded preprocessed data from W&B artifact")
print(f"train_X_scaled: {train_X_scaled.shape}, train_y_scaled: {train_y_scaled.shape}")

# 2. Create LSTM sequences (LSTM-specific, stays here)

WINDOW_SIZE = run.config["window_size"]


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


print("\nCreating sequences...")
X_train, y_train = create_sequences(train_X_scaled, train_y_scaled, train_sites, WINDOW_SIZE)
X_val, y_val     = create_sequences(val_X_scaled, val_y_scaled, val_sites, WINDOW_SIZE)
X_test, y_test   = create_sequences(test_X_scaled, test_y_scaled, test_sites, WINDOW_SIZE)

print(f"Train sequences: {X_train.shape}")
print(f"Val sequences:   {X_val.shape}")
print(f"Test sequences:  {X_test.shape}")

# 3. LSTM model goes here'''