# data/

Local data storage for the flood forecasting project.

## Contents

| Path | Description |
|------|-------------|
| `database/database.duckdb` | Main DuckDB database (76 MB). Contains `raw`, `seeds`, `staging`, `marts`, and `final` schemas. |
| `dagster/orchestration/` | Dagster asset definitions, jobs, configs, and resources for the extraction and transformation pipeline. |
| `gage_connections.csv` | Reference table of upstream/downstream gauge connections for the Missouri Basin. |

## DuckDB schemas

| Schema | Contents |
|--------|----------|
| `raw` | Extracted USGS streamflow (15-min and daily) and NLDAS-2 forcing data |
| `seeds` | Static watershed attributes from GAGES-II, HydroATLAS, NLDAS-2 climate indices |
| `staging` | Cleaned and renamed raw tables (dbt staging layer) |
| `marts` | Joined dimensional and fact tables (`dim_sites`, `fct_streamflow_hourly`) |
| `final` | ML-ready training table (`flood_model`) |
| `wandb` | Tables imported from W&B artifacts via `download_repository.py` |

## Quick access

```bash
just db          # open DuckDB UI (read-only)
just db-write    # open DuckDB UI (write access)
```

## Dagster orchestration

See `dagster/orchestration/` for asset definitions. Launch the UI with:

```bash
just dagster
```
