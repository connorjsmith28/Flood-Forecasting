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

        # Adds a lag for the last 7 hours of the data
        exprs = []
        for val in self.config["input_cols"]:
            if val in ["latitude", "longitude"]:
                continue
            for idx in range(1, self.config["lag_window"]):
                exprs.append(pl.col(val).shift(idx).over("site_id").alias(f"{val}{idx}"))

        df= df.with_columns(exprs)

        original_cols = [v for v in self.config["input_cols"] if v not in ["latitude", "longitude"]]
        static_cols = [v for v in self.config["input_cols"] if v in ["latitude", "longitude"]]
        lagged_cols = [f"{val}{idx}" for val in self.config["input_cols"] 
                    if val not in ["latitude", "longitude"] 
                    for idx in range(1, self.config["lag_window"])]
        model_input_cols = original_cols + static_cols + lagged_cols

        df = df.drop_nulls(subset=lagged_cols)

        # 5. Split by time (torch quantiles + masks)
        train_df, val_df, test_df = train_val_test_split_by_time(
            df,
            "observation_hour",
            self.config["train_split"],
            self.config["val_split"],
            split_time_days=self.config.get("split_time_days"),
        )

        # 6. Convert to tensors and scale with TorchStandardScaler (keep as tensors)
        target_col = self.config["target"]

        # Convert input columns to torch tensors for scaler fitting
        train_X = torch.tensor(train_df.select(model_input_cols).to_numpy(), dtype=torch.float64)
        val_X = torch.tensor(val_df.select(model_input_cols).to_numpy(), dtype=torch.float64)
        test_X = torch.tensor(test_df.select(model_input_cols).to_numpy(), dtype=torch.float64)

        feature_scaler = TorchStandardScaler()
        feature_scaler.fit(train_X)
        self.feature_scaler = feature_scaler

        train_X_scaled_t = feature_scaler.transform(train_X)
        val_X_scaled_t = feature_scaler.transform(val_X)
        test_X_scaled_t = feature_scaler.transform(test_X)

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
        target_scaler.fit(train_y)
        self.target_scaler = target_scaler

        # keep as 2D arrays when converting back to numpy to avoid zero-dim issues
        train_y_scaled_t = target_scaler.transform(train_y)
        val_y_scaled_t = target_scaler.transform(val_y)
        test_y_scaled_t = target_scaler.transform(test_y)

        train_y_arr = train_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        val_y_arr = val_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)
        test_y_arr = test_y_scaled_t.detach().cpu().numpy().reshape(-1, 1)

        self.train_y_scaled = pl.DataFrame(train_y_arr, schema=[self.target_col])
        self.val_y_scaled = pl.DataFrame(val_y_arr, schema=[self.target_col])
        self.test_y_scaled = pl.DataFrame(test_y_arr, schema=[self.target_col])

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
