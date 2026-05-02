# src/utils/

Shared helper functions used across notebooks and the preprocessing pipeline.

## `helpers.py`

| Function | Description |
|----------|-------------|
| `pull_wandb(file_name, file_path, ...)` | Download a parquet artifact from W&B and return as a Polars DataFrame. Supports site/date/frequency filtering. |
| `pull_duckdb(file_name, ...)` | Query a table from the local DuckDB file (`data/database/database.duckdb`) and return as Polars. |
| `create_sequences(X, y, window_size)` | Slide a window across each site's time series to produce `(samples, timesteps, features)` arrays for sequence models. |

### Notes

- `pull_wandb` is hardcoded to the `connorjsmith28-rice-university/flood-forecasting` W&B entity/project.
- For `frequency="daily"`, both helpers filter to noon observations (12:00) to get exactly one row per calendar day.
- `create_sequences` never crosses site boundaries; sites with fewer rows than `window_size` are silently skipped.
