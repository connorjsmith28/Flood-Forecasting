# notebooks/

Jupyter notebooks for data exploration, model training, and evaluation.

## Subfolders

| Folder | Description |
|--------|-------------|
| [data_exploration/](data_exploration/) | EDA, site coverage, correlation analysis, flood frequency (Log-Pearson III), variable trends |
| [model_training/](model_training/) | Training scripts for LSTM, GRU, TFT, Hybrid, GNN models with W&B sweep configs |
| [test/](test/) | Model evaluation: per-site metrics, SHAP explainability, persistence baseline |
| [train/](train/) | See `train/README.md` for W&B sweep setup and training instructions |

## Prerequisites

All notebooks assume:
- A W&B account linked to the `connorjsmith28-rice-university/flood-forecasting` project, **or** a local DuckDB at `data/database/database.duckdb`
- `uv` virtual environment activated (`.venv/`)
- Repo root on `sys.path` (the path-setup cell at the top of each notebook handles this)
