import os
import joblib
import numpy as np
import polars as pl
import torch
import torch.nn as nn
import wandb
from SRC.helper_functions.helpers import pull_wandb


class TorchStandardScaler:
    """StandardScaler for PyTorch tensors: z = (x - mean) / std. Fit on train, then transform."""

    def __init__(self) -> None:
        self.mean_: torch.Tensor | None = None
        self.scale_: torch.Tensor | None = None

    def fit(self, X: torch.Tensor) -> "TorchStandardScaler":
        self.mean_ = X.to(torch.float64).mean(dim=0)
        self.scale_ = X.to(torch.float64).std(dim=0)
        self.scale_[self.scale_ == 0] = 1.0
        return self

    def transform(self, X: torch.Tensor) -> torch.Tensor:
        return (X.to(torch.float64) - self.mean_) / self.scale_

    def inverse_transform(self, X: torch.Tensor) -> torch.Tensor:
        return X * self.scale_ + self.mean_


def train_val_test_split_by_time(
    df: pl.DataFrame,
    time_col: str,
    train_frac: float,
    val_frac: float,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Split a Polars DataFrame by time using torch quantiles and masks.
    Returns (train_df, val_df, test_df): first train_frac train, next (val_frac - train_frac) val, rest test.
    """
    observation_hour = torch.tensor(df[time_col].to_numpy(), dtype=torch.float64)
    split_thresholds = torch.quantile(
        observation_hour,
        torch.tensor([train_frac, val_frac], dtype=torch.float64),
    )
    train_mask = observation_hour < split_thresholds[0]
    val_mask = (observation_hour >= split_thresholds[0]) & (
        observation_hour < split_thresholds[1]
    )
    test_mask = observation_hour >= split_thresholds[1]

    train_idx = torch.where(train_mask)[0].numpy()
    val_idx = torch.where(val_mask)[0].numpy()
    test_idx = torch.where(test_mask)[0].numpy()

    train_df = df.filter(pl.int_range(0, df.height).is_in(pl.Series(train_idx)))
    val_df = df.filter(pl.int_range(0, df.height).is_in(pl.Series(val_idx)))
    test_df = df.filter(pl.int_range(0, df.height).is_in(pl.Series(test_idx)))
    
    return train_df, val_df, test_df

def preprocess(config: dict) -> tuple:
    """
    Pipeline: load -> select -> filter -> sort -> add target -> split by time -> scale (TorchStandardScaler).
    Returns (train_X_scaled, val_X_scaled, test_X_scaled, train_y_scaled, val_y_scaled, test_y_scaled,
             train_sites, val_sites, test_sites, feature_scaler, target_scaler).
    X and y are torch.Tensor; sites are numpy (string IDs).
    """
    # 1. Load (optional config["n_rows"] limits rows read from artifact)
    df = pull_wandb(
        {"train_split": config["train_split"], 
        "val_split": config["val_split"], 
        "input_cols": config["input_cols"], 
        "target": config["target"]},
        config["file_name"],
        n_rows=config["n_rows"],
    )   

    # 2. Select features and filter
    features = ["site_id", "observation_hour"] + config["input_cols"]
    df = df.select(features)
    df = df.drop_nulls(subset=["gage_height_ft_mean", "streamflow_cfs_mean"])

    # 3. Sort by site and time (required for LSTM sequences)
    df = df.sort(["site_id", "observation_hour"])

    # 4. Create target: streamflow 24h ahead (shift within each site)
    target_col = config["target"]
    df = df.with_columns(
        pl.col("streamflow_cfs_mean")
        .shift(-24)
        .over("site_id")
        .alias(target_col)
    )
    df = df.drop_nulls(subset=[target_col])

    # 5. Split by time (torch quantiles + masks)
    train_df, val_df, test_df = train_val_test_split_by_time(
        df,
        "observation_hour",
        config["train_split"],
        config["val_split"],
    )

    # 6. Convert to tensors and scale with TorchStandardScaler (keep as tensors)
    input_cols = config["input_cols"]
    target_col = config["target"]

    train_X = torch.tensor(train_df.select(input_cols).to_numpy(), dtype=torch.float64)
    val_X = torch.tensor(val_df.select(input_cols).to_numpy(), dtype=torch.float64)
    test_X = torch.tensor(test_df.select(input_cols).to_numpy(), dtype=torch.float64)

    feature_scaler = TorchStandardScaler()
    feature_scaler.fit(train_X)
    train_X_scaled = feature_scaler.transform(train_X)
    val_X_scaled = feature_scaler.transform(val_X)
    test_X_scaled = feature_scaler.transform(test_X)

    train_y = torch.tensor(train_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)
    val_y = torch.tensor(val_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)
    test_y = torch.tensor(test_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)

    target_scaler = TorchStandardScaler()
    target_scaler.fit(train_y)
    train_y_scaled = target_scaler.transform(train_y).squeeze()
    val_y_scaled = target_scaler.transform(val_y).squeeze()
    test_y_scaled = target_scaler.transform(test_y).squeeze()

    train_sites = train_df["site_id"].to_numpy()
    val_sites = val_df["site_id"].to_numpy()
    test_sites = test_df["site_id"].to_numpy()

    return (
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
    )


def preprocess_and_save(
    config: dict,
    *,
    out_dir: str = "pipeline_outputs",
    artifact_name: str = "flood-preprocessed-missouri",
    artifact_type: str = "preprocessed-dataset",
    artifact_description: str = "Scaled flat arrays + scalers + site ids. Model-agnostic. Target=24h ahead streamflow.") -> None:
    """
    Run preprocess_missouri(config), save outputs to disk, and log as a W&B artifact.
    Expects an active W&B run (e.g. from pull_wandb inside preprocess_missouri).
    """
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
    ) = preprocess(config)

    print(
        f"\nTrain: {train_X_scaled.shape[0]:,} | Val: {val_X_scaled.shape[0]:,} | Test: {test_X_scaled.shape[0]:,}"
    )

    os.makedirs(out_dir, exist_ok=True)

    # Save scaled arrays (tensors -> numpy for .npy)
    np.save(f"{out_dir}/train_X_scaled.npy", train_X_scaled.numpy())
    np.save(f"{out_dir}/val_X_scaled.npy", val_X_scaled.numpy())
    np.save(f"{out_dir}/test_X_scaled.npy", test_X_scaled.numpy())
    np.save(f"{out_dir}/train_y_scaled.npy", train_y_scaled.numpy())
    np.save(f"{out_dir}/val_y_scaled.npy", val_y_scaled.numpy())
    np.save(f"{out_dir}/test_y_scaled.npy", test_y_scaled.numpy())

    np.save(f"{out_dir}/train_sites.npy", train_sites)
    np.save(f"{out_dir}/val_sites.npy", val_sites)
    np.save(f"{out_dir}/test_sites.npy", test_sites)

    joblib.dump(feature_scaler, f"{out_dir}/feature_scaler.pkl")
    joblib.dump(target_scaler, f"{out_dir}/target_scaler.pkl")

    run = wandb.run
    if run is not None:
        artifact = wandb.Artifact(
            name=artifact_name,
            type=artifact_type,
            description=artifact_description,
        )
        artifact.add_dir(out_dir)
        run.log_artifact(artifact, aliases=["latest"])
        run.finish()
        print("\nPipeline outputs uploaded to W&B.")
