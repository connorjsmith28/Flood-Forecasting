import os
import joblib
import numpy as np
import polars as pl
import torch
import wandb
from SRC.helper_functions.helpers import pull_wandb,pull_duckdb

class TorchStandardScaler:
    """StandardScaler for PyTorch tensors: z = (x - mean) / std. Fit on train, then transform."""

    def __init__(self) -> None:
        self.mean_: torch.Tensor | None = None
        self.scale_: torch.Tensor | None = None

    def fit(self, X: torch.Tensor) -> "TorchStandardScaler":
        self.mean_ = X.to(torch.float64).nanmean(dim=0)
        variance = ((X.to(torch.float64) - self.mean_) ** 2).nanmean(dim=0)
        self.scale_ = variance.sqrt()
        self.scale_[self.scale_ == 0] = 1.0
        self.scale_[torch.isnan(self.scale_)] = 1.0
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
    """
    This class is used to preprocess the data.
    The config file should contain the following keys:
    - input_cols: list of columns to include in the model
    - target: the column to predict
    - train_split: the fraction of the data to use for training
    - val_split: the fraction of the data to use for validation
    - n_rows: the number of rows to read from the W&B artifact or DuckDB table
    - file_path: the path to the W&B artifact
    - file_name: the name of the W&B artifact
    - table: the name of the DuckDB table
    - lag_window: the number of hours to lag the data
    """
    def __init__(self, config: dict) -> None:
        self.config = config
        self.df: pl.DataFrame | None = None  # single combined DataFrame
        self.train_X_scaled: pl.DataFrame | None = None
        self.val_X_scaled: pl.DataFrame | None = None
        self.test_X_scaled: pl.DataFrame | None = None
        self.train_y_scaled: pl.DataFrame | None = None
        self.val_y_scaled: pl.DataFrame | None = None
        self.test_y_scaled: pl.DataFrame | None = None
        self.feature_scaler = TorchStandardScaler()
        self.target_scaler = TorchStandardScaler()

    def pull_wandb(self):
        dfs = []
        for site in self.config['sites']:
            dfs.append(pull_wandb(self.config["file_name"], self.config["file_path"], self.config['n_rows'], site))
        self.df = pl.concat(dfs)
        self.preprocess()

    def pull_duckdb(self):
        self.df = pull_duckdb(
            self.config["table"],
            sites=self.config["sites"],
            start_date=self.config.get("start_date"),
            end_date=self.config.get("end_date"),
        )
        self.preprocess()
         
    def preprocess(self):
        """
        Single-pass pipeline: feature-engineer the combined DataFrame, fit scalers
        on all sites, split by time, scale, then partition into per-site lists.

        All lag / shift operations use `.over("site_id")` so values never leak
        across sites.
        """
        df = self.df
        self.target_col = self.config["target"]

        # --- 1. Select & sort ------------------------------------------------
        features = ["site_id", "observation_hour"] + self.config["input_cols"]
        df = df.select(features).sort(["site_id", "observation_hour"])

        # --- 2. Target: streamflow 24 h ahead (within each site) ------------
        df = df.with_columns(
            pl.col("streamflow_cfs_mean")
            .shift(-24)
            .over("site_id")
            .alias(self.target_col)
        )
        df = df.drop_nulls(subset=[self.target_col])
        df = df.drop_nulls(subset=["gage_height_ft_mean", "streamflow_cfs_mean"])

        # --- 3. Lag features (within each site – no cross-site leakage) ------
        exprs = []
        for col in self.config["input_cols"]:
            if col in ["latitude", "longitude"]:
                continue
            for lag in range(1, self.config["lag_window"]):
                exprs.append(
                    pl.col(col).shift(lag).over("site_id").alias(f"{col}{lag}")
                )
        df = df.with_columns(exprs)

        # Drop rows where lags are null (first `lag_window - 1` rows per site)
        lag_cols_all = [f"{col}{lag}" for col in self.config["input_cols"]
                        if col not in ["latitude", "longitude"]
                        for lag in range(1, self.config["lag_window"])]
        df = df.drop_nulls(subset=lag_cols_all)

        # --- 4. Build column lists -------------------------------------------
        original_cols = [v for v in self.config["input_cols"] if v not in ["latitude", "longitude"]]
        static_cols = [v for v in self.config["input_cols"] if v in ["latitude", "longitude"]]
        self.model_input_cols = original_cols + static_cols + lag_cols_all

        # --- 5. Fit scalers on ALL data (before splitting) -------------------
        self.feature_scaler.fit(
            torch.tensor(df.select(self.model_input_cols).to_numpy(), dtype=torch.float64)
        )
        self.target_scaler.fit(
            torch.tensor(df.select(self.target_col).to_numpy(), dtype=torch.float64)
        )

        # --- 6. Split by time PER SITE, scale, then concatenate ---------------
        train_parts_x, val_parts_x, test_parts_x = [], [], []
        train_parts_y, val_parts_y, test_parts_y = [], [], []

        for site in df["site_id"].unique().sort().to_list():
            site_df = df.filter(pl.col("site_id") == site)
            train_df, val_df, test_df = train_val_test_split_by_time(
                site_df, "observation_hour",
                self.config["train_split"], self.config["val_split"],
            )

            for split_df, x_parts in [
                (train_df, train_parts_x),
                (val_df, val_parts_x),
                (test_df, test_parts_x),
            ]:
                X = torch.tensor(split_df.select(self.model_input_cols).to_numpy(), dtype=torch.float64)
                X_scaled = self.feature_scaler.transform(X).numpy()
                scaled_df = pl.DataFrame(X_scaled, schema=self.model_input_cols)
                scaled_df = pl.concat(
                    [split_df.select(["site_id", "observation_hour"]), scaled_df],
                    how="horizontal",
                )
                x_parts.append(scaled_df)

            for split_df, y_parts in [
                (train_df, train_parts_y),
                (val_df, val_parts_y),
                (test_df, test_parts_y),
            ]:
                y = torch.tensor(split_df[self.target_col].to_numpy(), dtype=torch.float64).reshape(-1, 1)
                y_scaled = self.target_scaler.transform(y).detach().cpu().numpy().reshape(-1, 1)
                y_parts.append(pl.DataFrame(y_scaled, schema=[self.target_col]))

        self.train_X_scaled = pl.concat(train_parts_x)
        self.val_X_scaled = pl.concat(val_parts_x)
        self.test_X_scaled = pl.concat(test_parts_x)
        self.train_y_scaled = pl.concat(train_parts_y)
        self.val_y_scaled = pl.concat(val_parts_y)
        self.test_y_scaled = pl.concat(test_parts_y)
    def return_outputs(self):
        return (
            self.train_X_scaled,
            self.val_X_scaled,
            self.test_X_scaled,
            self.train_y_scaled,
            self.val_y_scaled,
            self.test_y_scaled,
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

def safe_drop(df, cols):
    existing = [c for c in cols if c in df.columns]
    return df.drop(existing)

def _to_3d(df, cols_to_drop, timesteps):
    """Convert a single Polars DataFrame to a 3D NumPy array (samples, timesteps, features).
    If the DataFrame has fewer features than timesteps (e.g. a single-column target),
    broadcast the values across all timesteps."""
    features_df = safe_drop(df, cols_to_drop)
    arr = features_df.to_numpy()
    # drop first `timesteps` rows for lag alignment
    if timesteps is not None and timesteps > 0:
        arr = arr[timesteps:]
    if arr.ndim == 3:
        return arr
    if timesteps is None:
        raise ValueError('timesteps must be provided when reshaping to 3D')
    samples, total_features = arr.shape
    # single-feature case (e.g. target column): broadcast across timesteps
    if total_features < timesteps:
        return np.repeat(arr[:, np.newaxis, :], timesteps, axis=1)
    if total_features % timesteps != 0:
        raise ValueError(f'Total features ({total_features}) not divisible by timesteps ({timesteps})')
    features_per_step = total_features // timesteps
    return arr.reshape(samples, timesteps, features_per_step)

def prep_x_for_tf_horiz(df, cols_to_drop=None, timesteps=None):
    """Convert a multi-site Polars DataFrame to a 3D NumPy array for TensorFlow.

    Processes each site independently (drops first `timesteps` rows per site),
    then truncates all sites to the same sample count and vstacks.

    Args:
        df: Polars DataFrame with a 'site_id' column and all sites combined.
        cols_to_drop: columns to remove before converting to NumPy.
        timesteps: lag window / number of timesteps for reshaping.

    Returns:
        3D NumPy array of shape (n_sites * min_samples, timesteps, features_per_step).
    """
    if cols_to_drop is None:
        cols_to_drop = ['latitude', 'longitude', 'site_id', 'observation_hour']

    sites = df["site_id"].unique().sort().to_list()
    blocks = []
    for site in sites:
        site_df = df.filter(pl.col("site_id") == site)
        blocks.append(_to_3d(site_df, cols_to_drop, timesteps))

    min_samples = min(b.shape[0] for b in blocks)
    return np.vstack([b[:min_samples] for b in blocks])


def prep_y_for_tf_horiz(y_df, site_ids, timesteps=0):
    """Prepare target array for TensorFlow by splitting per-site, trimming, and vstacking.

    Args:
        y_df: Polars DataFrame with a single target column (rows aligned with X).
        site_ids: Polars Series of site_id values (from the corresponding X DataFrame).
        timesteps: number of initial rows to drop per site to match X alignment from prep_x_for_tf.

    Returns:
        2D NumPy array of shape (n_sites * min_samples, 1).
    """
    target_col = y_df.columns[0]
    combined = y_df.with_columns(site_ids.alias("site_id"))
    sites = combined["site_id"].unique().sort().to_list()
    parts = [
        combined.filter(pl.col("site_id") == s)[target_col].to_numpy()[timesteps:].reshape(-1, 1)
        for s in sites
    ]
    min_len = min(len(p) for p in parts)
    return np.vstack([p[:min_len] for p in parts])