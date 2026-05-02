# src/elt/

Extract-Load-Transform pipeline for hydrological and meteorological data.

## Subfolders

| Folder | Description |
|--------|-------------|
| [extraction/](extraction/) | API clients for USGS NWIS (streamflow) and NLDAS-2 (weather forcing). See `extraction/README.md` for full details. |
| [transformation/](transformation/) | dbt project: staging → marts → final ML-ready tables. Run with `just transform`. |
| [weights_biases_integration/](weights_biases_integration/) | Scripts to sync processed datasets to/from W&B artifacts. |

## Data flow

```
USGS NWIS API          NLDAS-2 (NASA GES DISC)
      │                        │
  extraction/usgs.py    extraction/nldas3.py
      │                        │
      └────────┬───────────────┘
               ▼
         DuckDB raw schema
               │
         transformation/ (dbt)
               │
         DuckDB final.flood_model
               │
  weights_biases_integration/
               │
         W&B artifacts (parquet)
```
