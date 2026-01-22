# Flood Forecasting Project

## Project Overview
A flood forecasting project using ML models with polars, PyTorch, and scikit-learn.

## Project Structure
- `elt/extraction/` - Data extraction scripts
- `elt/transformation/` - dbt models for data transformation
- `orchestration/` - Dagster pipelines
- `exploratory/` - Exploratory data analysis notebooks
- `models/` - ML model code

## Tech Stack
- **Data Processing**: Polars
- **ML Frameworks**: PyTorch, scikit-learn
- **Orchestration**: Dagster
- **Transformation**: dbt (fusion)
- **SQL Linting**: sqlfluff

## Commands
- `just dagster` - Launch Dagster UI (opens browser)
- `just db` - Launch DuckDB UI (read-only)
- `just db-write` - Launch DuckDB UI (write access)
- `just extract` - Run extraction job (USGS, NLDAS, GAGES-II)
- `just extract-fresh` - Clear HTTP cache and run extraction
- `just transform` - Run dbt build (run + test)
- `just lint` - Lint Python (ruff) and SQL (sqlfluff) files
- `just dbt-docs` - Generate and serve dbt documentation

## Code Style
- Use ruff for Python formatting and linting
- Use sqlfluff for SQL linting
- Follow PEP 8 conventions for Python
- Use type hints where practical
