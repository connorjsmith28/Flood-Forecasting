import os
import joblib
import numpy as np
import polars as pl
import torch
import torch.nn as nn
import wandb
from SRC.helper_functions.helpers import pull_wandb,pull_duckdb
import wandb

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
    timestamps = df[time_col].cast(pl.Int64).to_numpy()

    observation_hour = torch.tensor(
        timestamps,
        dtype=torch.float64
    )

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
class processor():
    def __init__(self, config: dict) -> None:
        self.config = config
        self.df = None
        self.train_X_scaled = None
        self.val_X_scaled = None
        self.test_X_scaled = None
        self.train_y_scaled = None
        self.val_y_scaled = None
        self.test_y_scaled = None
        self.train_sites = None
        self.val_sites = None
        self.test_sites = None
        self.feature_scaler = None
        self.target_scaler = None
    def pull_wandb(self):
        self.df = pull_wandb(self.config["file_name"],self.config["file_path"],self.config['n_rows'])
        self.preprocess()
    def pull_duckdb(self):
        self.df = pull_duckdb(self.config["table"], self.config["n_rows"])
        print(self.df.head())
        self.preprocess()
         
    def preprocess(self):
        """
        Pipeline: load -> select -> filter -> sort -> add target -> split by time -> scale (TorchStandardScaler).
        Returns (train_X_scaled, val_X_scaled, test_X_scaled, train_y_scaled, val_y_scaled, test_y_scaled,
                train_sites, val_sites, test_sites, feature_scaler, target_scaler).
        X and y are torch.Tensor; sites are numpy (string IDs).
        """
        # 1. Load (optional config["n_rows"] limits rows read from artifact)
        df = self.df
        # 2. Select features and filter
        features = ["site_id", "observation_hour"] + self.config["input_cols"]
        df = df.select(features)
        #df = df.drop_nulls(subset=["gage_height_ft_mean", "streamflow_cfs_mean"])

        # 3. Sort by site and time (required for LSTM sequences)
        df = df.sort(["site_id", "observation_hour"])

        # 4. Create target: streamflow 24h ahead (shift within each site)
        target_col = self.config["target"]
        df = df.with_columns(
            pl.col("streamflow_cfs_mean")
            .shift(-24)
            .over("site_id")
            .alias(target_col)
        )
        # drop rows where target is null (last 24 rows per site after shift)
        df = df.drop_nulls(subset=[target_col])

        # 5. Split by time (torch quantiles + masks)
        train_df, val_df, test_df = train_val_test_split_by_time(
            df,
            "observation_hour",
            self.config["train_split"],
            self.config["val_split"],
        )

        # 6. Convert to tensors and scale with TorchStandardScaler (keep as tensors)
        input_cols = self.config["input_cols"]
        target_col = self.config["target"]

        # Convert input columns to torch tensors for scaler fitting
        train_X = torch.tensor(train_df.select(input_cols).to_numpy(), dtype=torch.float64)
        val_X = torch.tensor(val_df.select(input_cols).to_numpy(), dtype=torch.float64)
        test_X = torch.tensor(test_df.select(input_cols).to_numpy(), dtype=torch.float64)

        feature_scaler = TorchStandardScaler()
        feature_scaler.fit(train_X)
        train_X_scaled_t = feature_scaler.transform(train_X)
        val_X_scaled_t = feature_scaler.transform(val_X)
        test_X_scaled_t = feature_scaler.transform(test_X)

        # Convert scaled tensors back to Polars DataFrames and attach site/time columns
        train_scaled_arr = train_X_scaled_t.numpy()
        val_scaled_arr = val_X_scaled_t.numpy()
        test_scaled_arr = test_X_scaled_t.numpy()

        train_scaled_df = pl.DataFrame(train_scaled_arr, schema=input_cols)
        val_scaled_df = pl.DataFrame(val_scaled_arr, schema=input_cols)
        test_scaled_df = pl.DataFrame(test_scaled_arr, schema=input_cols)

        # Preserve site_id and observation_hour for alignment and downstream processing
        train_scaled_df = pl.concat([train_df.select(["site_id", "observation_hour"]), train_scaled_df], how="horizontal")
        val_scaled_df = pl.concat([val_df.select(["site_id", "observation_hour"]), val_scaled_df], how="horizontal")
        test_scaled_df = pl.concat([test_df.select(["site_id", "observation_hour"]), test_scaled_df], how="horizontal")

        self.train_X_scaled = train_scaled_df
        self.val_X_scaled = val_scaled_df
        self.test_X_scaled = test_scaled_df

        # Scale targets and keep as DataFrames as well
        train_y = torch.tensor(train_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)
        val_y = torch.tensor(val_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)
        test_y = torch.tensor(test_df[target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)

        target_scaler = TorchStandardScaler()
        target_scaler.fit(train_y)
        # keep as 2D arrays when converting back to numpy to avoid zero-dim issues
        train_y_scaled_t = target_scaler.transform(train_y)
        val_y_scaled_t = target_scaler.transform(val_y)
        test_y_scaled_t = target_scaler.transform(test_y)

        train_y_arr = train_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        val_y_arr = val_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        test_y_arr = test_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)

        self.train_y_scaled = pl.DataFrame(train_y_arr, schema=[target_col])
        self.val_y_scaled = pl.DataFrame(val_y_arr, schema=[target_col])
        self.test_y_scaled = pl.DataFrame(test_y_arr, schema=[target_col])

        # store scalers on the instance for downstream use
        self.feature_scaler = feature_scaler
        self.target_scaler = target_scaler

        self.train_sites = train_df["site_id"].to_numpy()
        self.val_sites = val_df["site_id"].to_numpy()
        self.test_sites = test_df["site_id"].to_numpy()
        # store observation times (as ISO strings) aligned with the X arrays
        # do not store test_times per request
    def return_outputs(self):
        return (
            self.train_X_scaled,
            self.val_X_scaled,
            self.test_X_scaled,
            self.train_y_scaled,
            self.val_y_scaled,
            self.test_y_scaled,
            self.train_sites,
            self.val_sites,
            self.test_sites,
            self.feature_scaler,
            self.target_scaler,
        )
    def save_to_wandb(self, artifact_name, artifact_type, artifact_description, out_dir):

        os.makedirs(out_dir, exist_ok=True)

        # Save scaled arrays (tensors -> numpy for .npy)
        # Save scaled arrays (DataFrames -> numpy for .npy)
        # X DataFrames include site_id and observation_hour as first two columns
        np.save(f"{out_dir}/train_X_scaled.npy", self.train_X_scaled.to_numpy())
        np.save(f"{out_dir}/val_X_scaled.npy", self.val_X_scaled.to_numpy())
        np.save(f"{out_dir}/test_X_scaled.npy", self.test_X_scaled.to_numpy())
        np.save(f"{out_dir}/train_y_scaled.npy", self.train_y_scaled.to_numpy())
        np.save(f"{out_dir}/val_y_scaled.npy", self.val_y_scaled.to_numpy())
        np.save(f"{out_dir}/test_y_scaled.npy", self.test_y_scaled.to_numpy())

        np.save(f"{out_dir}/train_sites.npy", self.train_sites)
        np.save(f"{out_dir}/val_sites.npy", self.val_sites)
        np.save(f"{out_dir}/test_sites.npy", self.test_sites)
        # save observation times
        # do not save train/val/test times per request

        joblib.dump(self.feature_scaler, f"{out_dir}/feature_scaler.pkl")
        joblib.dump(self.target_scaler, f"{out_dir}/target_scaler.pkl")

        run = wandb.run
        #sasha please test and modularize code below to helpers
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
        #todo write to return output