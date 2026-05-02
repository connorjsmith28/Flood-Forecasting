# src/preprocessing/

Data preprocessing for flood forecasting model training. The main entry point is the `processor` class in `preprocessing.py`.

## What it does

1. Pulls data from W&B artifacts or DuckDB
2. Selects features and sorts by site + time
3. Creates the 24h-ahead streamflow target via a negative shift (`.over("site_id")` to prevent cross-site bleed)
4. Optionally expands lag features (when `lag_window > 1`)
5. Splits into train/val/test by time (chronological or rolling N-day chunks)
6. Scales features and targets with `TorchStandardScaler` (globally or per-site)

## Key classes

### `processor`

Config-driven pipeline. See the config reference at the top of `preprocessing.py` for all keys.

```python
from src.preprocessing.preprocessing import processor

config = {
    "input_cols": ["streamflow_cfs_mean", "precipitation_mm", "latitude", "longitude"],
    "static_cols": ["latitude", "longitude"],
    "target": "streamflow_cfs_target_24h",
    "train_split": 0.8,
    "val_split": 0.9,
    "lag_window": 1,           # set >1 for built-in lag expansion; use 1 + create_sequences() for windowing
    "split_time_days": 30,     # rolling 30-day windows; None = single chronological cut
    "table": "wandb.flood_model_top30",
}
pcr = processor(config)
pcr.pull_duckdb()
train_X, val_X, test_X, train_y, val_y, test_y = pcr.return_outputs()
```

### `TorchStandardScaler`

StandardScaler for PyTorch tensors. Supports global fit (`fit`) or per-site fit (`fit_by_site`). Fitted scalers are preserved on the `processor` instance and can be saved/loaded via `processor.save()` / `processor.load()`.

### `create_sequences` (in `src/utils/helpers.py`)

When `lag_window=1`, use `create_sequences(X, y, window_size)` to produce the 3D `(samples, timesteps, features)` arrays expected by LSTM/GRU/TFT.

## Column ordering

The output column order is: **dynamic → static → lagged**. This matters for TFT, which expects static features in the trailing columns (controlled by `num_static_features`).
