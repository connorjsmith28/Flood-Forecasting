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
| USGS NWIS | Streamflow (15-min) + site metadata | site_id |
| Open-Meteo | Hourly weather forcing (precip, temp, humidity, wind, radiation) | lat/long |
| GAGES-II | Static watershed attributes | gauge_id |
| NLDI | 126+ basin characteristics | site_id → ComID |

## Project Structure
```
elt/
  extraction/           # Python scripts to fetch data from APIs
    usgs.py             # USGS streamflow and site info
    weather.py          # Open-Meteo historical weather
    gages.py            # GAGES-II watershed attributes
    nldi_attributes.py  # NLDI basin characteristics
  transformation/       # dbt project
    models/staging/     # Clean raw data
    models/marts/       # Business logic (dim_sites, fct_streamflow_hourly)
orchestration/          # Dagster assets and jobs
  assets/extraction.py  # Extraction assets with incremental loading
  assets/dbt_assets.py  # dbt integration
  definitions.py        # Job definitions
exploratory/            # Notebooks (not yet populated)
models/                 # ML models (not yet implemented)
```

## Database
- **DuckDB**: `flood_forecasting.duckdb`
- **Schemas**: `raw` (extracted data), `main` (dbt models)
- **Key tables**:
  - `raw.streamflow_raw` - USGS observations
  - `raw.weather_forcing` - Open-Meteo hourly data
  - `raw.site_metadata` - Site locations and drainage areas
  - `marts.fct_streamflow_hourly` - Hourly streamflow joined with weather

## Commands
| Command | Description |
|---------|-------------|
| `just extract` | Run extraction job (USGS, Open-Meteo, GAGES-II, NLDI) |
| `just extract-fresh` | Clear HTTP cache and run extraction |
| `just transform` | Run dbt build (run + test) |
| `just dagster` | Launch Dagster UI (opens browser) |
| `just db` | Launch DuckDB UI (read-only) |
| `just db-write` | Launch DuckDB UI (write access) |
| `just lint` | Lint Python (ruff) and SQL (sqlfluff) |
| `just dbt-docs` | Generate and serve dbt documentation |

## Dagster Jobs
- `extraction_job` - Run all extraction assets
- `site_setup_job` - Extract site metadata only
- `streamflow_update_job` - Incremental streamflow update
- `weather_update_job` - Incremental weather update
- `transformation_job` - Run dbt models
- `full_pipeline_job` - Extract + transform

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
