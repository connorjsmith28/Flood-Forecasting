# =============================================================================
# preprocessing.py — Missouri River Basin Flood Forecasting Pipeline
# =============================================================================
#
# CONFIG REFERENCE
# ----------------
# The `processor` class accepts a config dict with the following keys:
#
# REQUIRED:
#   input_cols      (list[str])   Columns to use as model input features.
#                                 e.g. ["streamflow_cfs_mean", "precipitation", "latitude", "longitude"]
#   target          (str)         Name of the target column to predict (streamflow shifted forward).
#                                 e.g. "streamflow_cfs_mean_target"
#   train_split     (float)       Fraction of data for training.        e.g. 0.7
#   val_split       (float)       Fraction of data for train + val.     e.g. 0.85
#   lag_window      (int)         Number of hourly timesteps to lag dynamic features. e.g. 24
#
# DATA SOURCE (one of the following must be used):
#   file_path       (str)         W&B artifact path.   Used by pull_wandb().
#   file_name       (str)         W&B artifact name.   Used by pull_wandb().
#   table           (str)         DuckDB table name.   Used by pull_duckdb().
#
# OPTIONAL:
#   static_cols     (list[str])   Columns treated as static (not lagged). Default: ["latitude", "longitude"]
#   sites           (list[str])   Subset of site IDs to load. Default: all sites.
#   start_date      (str)         ISO date string to filter data start.  e.g. "2000-01-01"
#   end_date        (str)         ISO date string to filter data end.    e.g. "2020-12-31"
#   frequency       (str)         Temporal resolution of the data. "daily" shifts target by 1,
#                                 anything else (e.g. "hourly") shifts by 24. Default: "hourly"
#   split_time_days (int)         If set, splits data in rolling chunks of N days rather than
#                                 a single chronological cut. Default: None (chronological split)
#   site_scaling    (bool)        If True, fit/transform scalers per site instead of globally.
#                                 Default: False
#
# EXAMPLE CONFIG:
#   config = {
#       "input_cols":   ["streamflow_cfs_mean", "precipitation", "temperature", "latitude", "longitude"],
#       "static_cols":  ["latitude", "longitude"],
#       "target":       "streamflow_cfs_mean_target",
#       "train_split":  0.7,
#       "val_split":    0.85,
#       "lag_window":   24,
#       "table":        "missouri_basin",
#       "sites":        ["06600000", "06610000"],
#       "frequency":    "hourly",
#       "split_time_days": 365,
#       "site_scaling": True,
#   }
# =============================================================================

import os
import joblib
import numpy as np
import polars as pl
import torch
import wandb
from pathlib import Path
from src.utils.helpers import pull_wandb, pull_duckdb

class TorchStandardScaler:
    """StandardScaler for PyTorch tensors: z = (x - mean) / std. Fit on train, then transform."""

    def __init__(self) -> None:
        self.mean_: torch.Tensor | None = None
        self.scale_: torch.Tensor | None = None
        self.site_mean_: dict | None = None
        self.site_scale_: dict | None = None

    def fit(self, X: torch.Tensor) -> "TorchStandardScaler":
        self.mean_ = X.to(torch.float64).nanmean(dim=0)
        variance = ((X.to(torch.float64) - self.mean_) ** 2).nanmean(dim=0)
        self.scale_ = variance.sqrt()
        self.scale_[self.scale_ == 0] = 1.0
        self.scale_[torch.isnan(self.scale_)] = 1.0
        return self

    def fit_by_site(self, X: torch.Tensor, site_ids: np.ndarray) -> "TorchStandardScaler":
        """Fit a separate mean and scale for each site."""
        self.site_mean_ = {}
        self.site_scale_ = {}
        for site in np.unique(site_ids):
            mask = site_ids == site
            site_X = X[mask].to(torch.float64)
            mean = site_X.nanmean(dim=0)
            variance = ((site_X - mean) ** 2).nanmean(dim=0)
            scale = variance.sqrt()
            scale[scale == 0] = 1.0
            scale[torch.isnan(scale)] = 1.0
            self.site_mean_[site] = mean
            self.site_scale_[site] = scale
        return self
        
    def transform(self, X: torch.Tensor) -> torch.Tensor:
        return (X.to(torch.float64) - self.mean_) / self.scale_

    def transform_by_site(self, X: torch.Tensor, site_ids: np.ndarray) -> torch.Tensor:
        """Per-site transform — requires fit_by_site() to have been called."""
        result = X.to(torch.float64).clone()
        for site in np.unique(site_ids):
            mask = site_ids == site
            result[mask] = (X[mask].to(torch.float64) - self.site_mean_[site]) / self.site_scale_[site]
        return result

    def inverse_transform(self, X: torch.Tensor) -> torch.Tensor:
        return X * self.scale_ + self.mean_

    def inverse_transform_by_site(self, X: torch.Tensor, site_ids: np.ndarray) -> torch.Tensor:
        """Per-site inverse transform."""
        result = X.to(torch.float64).clone()
        for site in np.unique(site_ids):
            mask = site_ids == site
            result[mask] = X[mask] * self.site_scale_[site] + self.site_mean_[site]
        return result

def train_val_test_split_by_time(
    df: pl.DataFrame,
    time_col: str,
    train_frac: float,
    val_frac: float,
    split_time_days: int | None = None, 
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Split a Polars DataFrame by time.
    If split_time_days is provided, splits into chunks of that many days and
    takes train_frac of each chunk for train, next (val_frac - train_frac) for val,
    and the rest for test. Otherwise splits chronologically.
    """

    if split_time_days is None:
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
        
    else:
        dates = df[time_col].unique().sort()
        n_dates = len(dates)

        train_dates = []
        val_dates = []
        test_dates = []

        # Slide through the dates in chunks of split_time_days
        for chunk_start in range(0, n_dates, split_time_days):
            chunk = dates[chunk_start: chunk_start + split_time_days]
            n_chunk = len(chunk)
            
            if n_chunk < 10:
                continue

            train_end = int(n_chunk * train_frac)
            val_end = int(n_chunk * val_frac)

            train_dates.extend(chunk[:train_end].to_list())
            val_dates.extend(chunk[train_end:val_end].to_list())
            test_dates.extend(chunk[val_end:].to_list())

        train_df = df.filter(pl.col(time_col).is_in(train_dates))
        val_df = df.filter(pl.col(time_col).is_in(val_dates))
        test_df = df.filter(pl.col(time_col).is_in(test_dates))

    return train_df, val_df, test_df


class processor():
    """
    This class is used to preprocess the data.
    The config file should contain the following keys:
    - input_cols: list of columns to include in the model
    - target: the column to predict
    - train_split: the fraction of the data to use for training
    - val_split: the fraction of the data to use for validation
    - file_path: the path to the W&B artifact
    - file_name: the name of the W&B artifact
    - table: the name of the DuckDB table
    - lag_window: the number of hours to lag the data
    """
    def __init__(self, config: dict) -> None:
        self.config = config
        self.df: pl.DataFrame | None = None
        self.train_X_scaled: pl.DataFrame | None = None
        self.val_X_scaled: pl.DataFrame | None = None
        self.test_X_scaled: pl.DataFrame | None = None
        self.train_y_scaled: pl.DataFrame | None = None
        self.val_y_scaled: pl.DataFrame | None = None
        self.test_y_scaled: pl.DataFrame | None = None
        self.test_site_ids: np.ndarray | None = None
        self.feature_scaler = TorchStandardScaler()
        self.target_scaler = TorchStandardScaler()

    def pull_wandb(self):
        self.df = pull_wandb(
            self.config["file_name"],
            self.config["file_path"],
            sites=self.config.get("sites"),
            start_date=self.config.get("start_date"),
            end_date=self.config.get("end_date"),
            frequency=self.config.get("frequency"),
        )
        self.preprocess()

    def pull_duckdb(self):
        self.df = pull_duckdb(
            self.config["table"],
            sites=self.config.get("sites"),
            start_date=self.config.get("start_date"),
            end_date=self.config.get("end_date"),
            frequency=self.config.get("frequency"),
        )
        self.preprocess()
         
    def preprocess(self):
        """
        Pipeline: load -> select -> filter -> sort -> add target -> split by time -> scale (TorchStandardScaler).
        Returns (train_X_scaled, val_X_scaled, test_X_scaled, train_y_scaled, val_y_scaled, test_y_scaled,
            feature_scaler, target_scaler).
        X and y are torch.Tensor; sites are numpy (string IDs).
        """
        # 1. Load (optional config["n_rows"] limits rows read from artifact)
        df = self.df
        self.target_col = self.config["target"]
        print(f"Starting preprocessing: {df.shape[0]:,} rows, {df.shape[1]} columns")
        
        # 2. Select features and filter
        features = ["site_id", "observation_hour"] + self.config["input_cols"]
        df = df.select(features)

        # 3. Sort by site and time (required for LSTM sequences)
        df = df.sort(["site_id", "observation_hour"])

        shift_amount = 1 if self.config.get("frequency") == "daily" else 24

        df = df.with_columns(
            pl.col("streamflow_cfs_mean")
            .shift(-shift_amount)
            .over("site_id")
            .alias(self.target_col)
        )
        # drop rows where target is null (last 24 rows per site after shift)
        df = df.drop_nulls(subset=[self.target_col])

        # Add lag features for dynamic columns
        static_col_names = self.config.get("static_cols", ["latitude", "longitude"])

        exprs = []
        for val in self.config["input_cols"]:
            if val in static_col_names:
                continue
            for idx in range(1, self.config["lag_window"]):
                exprs.append(pl.col(val).shift(idx).over("site_id").alias(f"{val}{idx}"))

        df= df.with_columns(exprs)

        original_cols = [v for v in self.config["input_cols"] if v not in static_col_names]
        static_cols = [v for v in self.config["input_cols"] if v in static_col_names]
        lagged_cols = [f"{val}{idx}" for val in self.config["input_cols"] 
                    if val not in static_col_names 
                    for idx in range(1, self.config["lag_window"])]
        model_input_cols = original_cols + static_cols + lagged_cols

        df = df.drop_nulls(subset=lagged_cols)
        df = df.drop_nulls()
        print(f"After lag null removal: {df.shape[0]:,} rows, {len(model_input_cols)} features")

        # 5. Split by time (torch quantiles + masks)
        print("Splitting by time...")
        train_df, val_df, test_df = train_val_test_split_by_time(
            df,
            "observation_hour",
            self.config["train_split"],
            self.config["val_split"],
            split_time_days=self.config.get("split_time_days"),
        )
        print(f"Train: {train_df.shape[0]:,} | Val: {val_df.shape[0]:,} | Test: {test_df.shape[0]:,}")

        print("Scaling features and targets...")
        # 6. Convert to tensors and scale with TorchStandardScaler (keep as tensors)
        target_col = self.config["target"]

        # Convert input columns to torch tensors for scaler fitting
        train_X = torch.tensor(train_df.select(model_input_cols).to_numpy(), dtype=torch.float64)
        val_X = torch.tensor(val_df.select(model_input_cols).to_numpy(), dtype=torch.float64)
        test_X = torch.tensor(test_df.select(model_input_cols).to_numpy(), dtype=torch.float64)

        train_site_ids = train_df["site_id"].to_numpy()
        val_site_ids = val_df["site_id"].to_numpy()
        test_site_ids = test_df["site_id"].to_numpy()
        self.test_site_ids = test_site_ids

        use_site_scaling = self.config.get("site_scaling", False)

        feature_scaler = TorchStandardScaler()

        if use_site_scaling:
            feature_scaler.fit_by_site(train_X, train_site_ids)
            train_X_scaled_t = feature_scaler.transform_by_site(train_X, train_site_ids)
            val_X_scaled_t = feature_scaler.transform_by_site(val_X, val_site_ids)
            test_X_scaled_t = feature_scaler.transform_by_site(test_X, test_site_ids)
        else:
            feature_scaler.fit(train_X)
            train_X_scaled_t = feature_scaler.transform(train_X)
            val_X_scaled_t = feature_scaler.transform(val_X)
            test_X_scaled_t = feature_scaler.transform(test_X)

        self.feature_scaler = feature_scaler

        # Convert scaled tensors back to Polars DataFrames and attach site/time columns
        train_scaled_arr = train_X_scaled_t.numpy()
        val_scaled_arr = val_X_scaled_t.numpy()
        test_scaled_arr = test_X_scaled_t.numpy()

        train_scaled_df = pl.DataFrame(train_scaled_arr, schema=model_input_cols)
        val_scaled_df = pl.DataFrame(val_scaled_arr, schema=model_input_cols)
        test_scaled_df = pl.DataFrame(test_scaled_arr, schema=model_input_cols)

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

        if use_site_scaling:
            target_scaler.fit_by_site(train_y, train_site_ids)
            train_y_scaled_t = target_scaler.transform_by_site(train_y, train_site_ids)
            val_y_scaled_t = target_scaler.transform_by_site(val_y, val_site_ids)
            test_y_scaled_t = target_scaler.transform_by_site(test_y, test_site_ids)
        else:
            target_scaler.fit(train_y)
            train_y_scaled_t = target_scaler.transform(train_y)
            val_y_scaled_t = target_scaler.transform(val_y)
            test_y_scaled_t = target_scaler.transform(test_y)

        self.target_scaler = target_scaler

        # keep as 2D arrays when converting back to numpy to avoid zero-dim issues
        train_y_arr = train_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        val_y_arr = val_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        test_y_arr = test_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)

        self.train_y_scaled = pl.DataFrame(train_y_arr, schema=[self.target_col])
        self.val_y_scaled = pl.DataFrame(val_y_arr, schema=[self.target_col])
        self.test_y_scaled = pl.DataFrame(test_y_arr, schema=[self.target_col])
        print("Scaling complete.")
        # store scalers on the instance for downstream use

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
        )

    def unscale(self, arr: np.ndarray, site_ids: np.ndarray | None = None) -> np.ndarray:
        """Inverse-transform predictions using the fitted target_scaler.

        Args:
            arr: Scaled predictions, shape (n,) or (n, 1).
            site_ids: Site ID strings aligned with arr. Required only when the
                      processor was configured with site_scaling=True.

        Returns:
            Unscaled numpy array of the same shape as ``arr``.
        """
        return unscale(arr, self.target_scaler, site_ids)

    def save_to_wandb(self, artifact_name, artifact_type, artifact_description, out_dir):
        os.makedirs(out_dir, exist_ok=True)

        # Save X as parquet (preserves site_id string and observation_hour datetime)
        self.train_X_scaled.write_parquet(f"{out_dir}/train_X_scaled.parquet")
        self.val_X_scaled.write_parquet(f"{out_dir}/val_X_scaled.parquet")
        self.test_X_scaled.write_parquet(f"{out_dir}/test_X_scaled.parquet")

        # Save y as npy (purely numeric)
        np.save(f"{out_dir}/train_y_scaled.npy", self.train_y_scaled.to_numpy())
        np.save(f"{out_dir}/val_y_scaled.npy", self.val_y_scaled.to_numpy())
        np.save(f"{out_dir}/test_y_scaled.npy", self.test_y_scaled.to_numpy())

        joblib.dump(self.feature_scaler, f"{out_dir}/feature_scaler.pkl")
        joblib.dump(self.target_scaler, f"{out_dir}/target_scaler.pkl")

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

    def save(self, path: str | None = None, name: str = "preprocessor") -> None:
        """Serialize config and fitted scalers to a file via joblib."""
        
        repo_root = Path(__file__).resolve().parents[2]

        if path is None:
            directory = repo_root / "models" / "preprocessors"
        else:
            directory = repo_root / path

        directory.mkdir(parents=True, exist_ok=True)
        out_path = directory / f"{name}.pkl"

        payload = {
            "config": self.config,
            "feature_scaler": self.feature_scaler,
            "target_scaler": self.target_scaler,
        }
        joblib.dump(payload, out_path)
        print(f"Preprocessor saved to {out_path}")

    @classmethod
    def load(cls, name: str, path: str | None = None) -> "processor":
        """Load a processor instance from a file saved with save()."""
        
        repo_root = Path(__file__).resolve().parents[2]

        if path is None:
            file_path = repo_root / "models" / "preprocessors" / f"{name}.pkl"
        else:
            file_path = repo_root / path / f"{name}.pkl"

        if not file_path.exists():
            raise FileNotFoundError(f"No preprocessor found at {file_path}")

        payload = joblib.load(file_path)
        if not isinstance(payload, dict):
            raise TypeError(f"Expected a dict payload, got {type(payload)}")

        instance = cls(payload["config"])
        instance.feature_scaler = payload["feature_scaler"]
        instance.target_scaler = payload["target_scaler"]
        return instance

    def prep_inference(
        self,
        df: pl.DataFrame | None = None,
        *,
        wandb_config: dict | None = None,
    ) -> pl.DataFrame:
        """Prepare data for model inference using the fitted scalers.

        Applies the same feature engineering as preprocess() (column selection,
        sorting, lag creation) but uses the already-fitted feature_scaler —
        no refitting, no train/val/test splitting.

        Supply exactly one data source:
          - ``df``           : a raw Polars DataFrame already in memory.
          - ``wandb_config`` : a dict with W&B keys ``file_name``, ``file_path``
                               and optionally ``sites``, ``start_date``,
                               ``end_date``, ``frequency``. The artifact will be
                               pulled and used as the input DataFrame.

        Args:
            df: Raw Polars DataFrame with at least site_id, observation_hour,
                and all columns listed in config["input_cols"].
            wandb_config: W&B artifact config dict. When provided, ``df`` is
                ignored and data is fetched from W&B instead.

        Returns:
            Scaled Polars DataFrame with site_id, observation_hour, and all
            model input columns (original + lag features), ready for inference.
        """
        if self.feature_scaler.mean_ is None and self.feature_scaler.site_mean_ is None:
            raise RuntimeError("Scaler is not fitted. Run preprocess() or load a saved processor first.")

        if wandb_config is not None:
            df = pull_wandb(
                wandb_config["file_name"],
                wandb_config["file_path"],
                sites=wandb_config.get("sites"),
                start_date=wandb_config.get("start_date"),
                end_date=wandb_config.get("end_date"),
                frequency=wandb_config.get("frequency"),
            )
        elif df is None:
            raise ValueError("Provide either a DataFrame via 'df' or a W&B config via 'wandb_config'.")

        features = ["site_id", "observation_hour"] + self.config["input_cols"]
        df = df.select(features)
        df = df.sort(["site_id", "observation_hour"])

        static_col_names = self.config.get("static_cols", ["latitude", "longitude"])

        exprs = []
        for val in self.config["input_cols"]:
            if val in static_col_names:
                continue
            for idx in range(1, self.config["lag_window"]):
                exprs.append(pl.col(val).shift(idx).over("site_id").alias(f"{val}{idx}"))
        df = df.with_columns(exprs)

        original_cols = [v for v in self.config["input_cols"] if v not in static_col_names]
        static_cols = [v for v in self.config["input_cols"] if v in static_col_names]
        lagged_cols = [
            f"{val}{idx}"
            for val in self.config["input_cols"]
            if val not in static_col_names
            for idx in range(1, self.config["lag_window"])
        ]
        model_input_cols = original_cols + static_cols + lagged_cols

        df = df.drop_nulls(subset=lagged_cols)
        df = df.drop_nulls()

        X = torch.tensor(df.select(model_input_cols).to_numpy(), dtype=torch.float64)
        site_ids = df["site_id"].to_numpy()

        use_site_scaling = self.config.get("site_scaling", False)
        if use_site_scaling:
            X_scaled = self.feature_scaler.transform_by_site(X, site_ids)
        else:
            X_scaled = self.feature_scaler.transform(X)

        scaled_df = pl.DataFrame(X_scaled.numpy(), schema=model_input_cols)
        return pl.concat([df.select(["site_id", "observation_hour"]), scaled_df], how="horizontal")

def unscale(arr: np.ndarray, scaler: "TorchStandardScaler", site_ids: np.ndarray | None = None) -> np.ndarray:
    """Inverse-transform a scaled numpy array using a fitted TorchStandardScaler.

    Args:
        arr: Scaled values as a numpy array of shape (n,) or (n, 1).
        scaler: A fitted TorchStandardScaler (e.g. processor.target_scaler).
        site_ids: Optional array of site ID strings, shape (n,). Required when
                  the scaler was fitted with fit_by_site(); ignored otherwise.

    Returns:
        Unscaled numpy array of the same shape as ``arr``.
    """
    t = torch.tensor(arr, dtype=torch.float64).reshape(-1, 1)
    if site_ids is not None and scaler.site_mean_ is not None:
        out = scaler.inverse_transform_by_site(t, site_ids)
    else:
        out = scaler.inverse_transform(t)
    return out.numpy().reshape(arr.shape)


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
    then truncates all sites to the same sample count and concatenates along
    the feature axis so each timestep contains features from ALL sites.

    Args:
        df: Polars DataFrame with a 'site_id' column and all sites combined.
        cols_to_drop: columns to remove before converting to NumPy.
        timesteps: lag window / number of timesteps for reshaping.

    Returns:
        3D NumPy array of shape (min_samples, timesteps, features_per_step * n_sites).
    """
    if cols_to_drop is None:
        cols_to_drop = ['latitude', 'longitude', 'site_id', 'observation_hour']

    sites = df["site_id"].unique().sort().to_list()
    blocks = []
    for site in sites:
        site_df = df.filter(pl.col("site_id") == site)
        blocks.append(_to_3d(site_df, cols_to_drop, timesteps))

    min_samples = min(b.shape[0] for b in blocks)
    return np.concatenate([b[:min_samples] for b in blocks], axis=2)


def prep_y_for_tf_horiz(y_df, site_ids, timesteps=0):
    """Prepare target array for TensorFlow by splitting per-site, trimming, and hstacking.

    Each site becomes one output column so the model predicts all sites simultaneously.

    Args:
        y_df: Polars DataFrame with a single target column (rows aligned with X).
        site_ids: Polars Series of site_id values (from the corresponding X DataFrame).
        timesteps: number of initial rows to drop per site to match X alignment from prep_x_for_tf.

    Returns:
        2D NumPy array of shape (min_samples, n_sites).
    """
    target_col = y_df.columns[0]
    combined = y_df.with_columns(site_ids.alias("site_id"))
    sites = combined["site_id"].unique().sort().to_list()
    parts = [
        combined.filter(pl.col("site_id") == s)[target_col].to_numpy()[timesteps:].reshape(-1, 1)
        for s in sites
    ]
    min_len = min(len(p) for p in parts)
    return np.hstack([p[:min_len] for p in parts])
