# src/

Source code for the flood forecasting pipeline.

## Subfolders

| Folder | Description |
|--------|-------------|
| [elt/](elt/) | Extract-Load-Transform: data ingestion from USGS/NLDAS-2 APIs and dbt transformations |
| [models/](models/) | ML model definitions (LSTM, GRU, Transformer, TFT, Hybrid) |
| [preprocessing/](preprocessing/) | Feature engineering, scaling, windowing, train/val/test splitting |
| [postprocessing/](postprocessing/) | Evaluation, SHAP explainability, inference utilities |
| [utils/](utils/) | Shared helpers: W&B artifact download, DuckDB query, sequence creation |
| [static/](static/) | Static reference data (flood quantile thresholds JSON) |
