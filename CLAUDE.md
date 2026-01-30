# Flood Forecasting Project

## Project Overview
A flood forecasting project focused on the Missouri River Basin (HUC 10). Currently building the data pipeline to extract, load, and transform hydrological and meteorological data from public APIs into a DuckDB database. ML model development is planned for later.

The original CAMELSH dataset paper can be found: https://www.osti.gov/pages/servlets/purl/2574906

## Current Focus
- Extracting streamflow and weather data for Missouri Basin sites
- Building staging and mart layers in dbt
- Joining weather forcing data to streamflow observations for model training

## Data Sources
| Source | Data | Key ID |
|--------|------|--------|
| USGS NWIS | Streamflow (15-min IV + daily) + site metadata | site_id |
| Open-Meteo | Hourly weather forcing (precip, temp, humidity, wind, radiation) | lat/long |
| GAGES-II (seeds) | Static watershed attributes (from CAMELS) | site_id |
| HydroATLAS (seeds) | 195+ catchment attributes (from CAMELS) | site_id |
| NLDAS-2 (seeds) | Climate indices (from CAMELS) | site_id |

## Project Structure
```
elt/
  extraction/           # Python scripts to fetch data from APIs
    usgs.py             # USGS site discovery, metadata, and streamflow
    weather.py          # Open-Meteo historical weather
  transformation/       # dbt project
    seeds/              # Static attributes (GAGES-II, HydroATLAS, NLDAS)
    models/staging/     # Clean raw data
    models/marts/       # Business logic (dim_sites, fct_streamflow_hourly)
    models/final/       # ML-ready tables (flood_model)
orchestration/          # Dagster assets and jobs
  assets/extraction.py  # Extraction assets with incremental loading
  assets/dbt_assets.py  # dbt integration
  definitions.py        # Job definitions
exploratory/            # Notebooks (not yet populated)
models/                 # ML models (not yet implemented)
```

## Database
- **DuckDB**: `flood_forecasting.duckdb`
- **Schemas**: `raw` (extracted data), `seeds` (dbt seeds), `staging`, `marts`, `final`
- **Key tables**:
  - `raw.site_metadata` - Site locations, drainage areas, and data availability flags
  - `raw.streamflow_15min` - USGS 15-minute interval observations
  - `raw.streamflow_daily` - USGS daily mean values
  - `raw.weather_forcing` - Open-Meteo hourly data
  - `final.flood_model` - ML-ready training data

## Commands
| Command | Description |
|---------|-------------|
| `just extract` | Run extraction job (USGS sites, streamflow, weather) |
| `just extract-fresh` | Clear HTTP cache and run extraction |
| `just transform` | Run dbt build (seeds + models + tests) |
| `just dagster` | Launch Dagster UI (opens browser) |
| `just db` | Launch DuckDB UI (read-only) |
| `just db-write` | Launch DuckDB UI (write access) |
| `just lint` | Lint Python (ruff) and SQL (sqlfluff) |
| `just dbt-docs` | Generate and serve dbt documentation |

## Dagster Jobs
- `extraction_job` - Extract raw data from USGS and Open-Meteo
- `transformation_job` - Run dbt (seeds + staging + marts)
- `full_pipeline_job` - Extract then transform

## Tech Stack
- **Data Processing**: Polars (extraction), DuckDB (storage/analytics)
- **Orchestration**: Dagster
- **Transformation**: dbt with DuckDB adapter
- **Linting**: ruff (Python), sqlfluff (SQL)
- **Package Manager**: uv

## Code Style
- Use ruff for Python formatting and linting
- Use sqlfluff for SQL linting
- Follow PEP 8 conventions for Python
- Use type hints where practical
