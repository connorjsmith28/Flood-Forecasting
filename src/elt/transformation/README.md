# src/elt/transformation/

dbt project that transforms raw extracted data into ML-ready tables.

## Layers

| Layer | Schema | Description |
|-------|--------|-------------|
| seeds | `seeds` | Static watershed attributes: GAGES-II, HydroATLAS, NLDAS-2 climate indices |
| staging | `staging` | Clean, renamed raw tables (one model per source) |
| marts | `marts` | Joined dimensions and fact tables (`dim_sites`, `fct_streamflow_hourly`) |
| final | `final` | ML-ready training table (`flood_model`) joining streamflow + weather forcing |

## Run

```bash
just transform        # dbt build (seeds + models + tests)
just dbt-docs         # generate and serve docs at localhost:8080
```

## Key output: `final.flood_model`

One row per site per hour. Columns include streamflow observations (CFS), all NLDAS-2 forcing variables, and static watershed attributes from seeds. This is the table consumed by `src/preprocessing/preprocessing.py`.

## Notes

- Weather is hourly; 15-min streamflow is aggregated to hourly in the marts layer
- Sites without hourly data fall back to daily values
- `data_vis.py` in this folder is a lightweight exploration script for the seed CSVs
