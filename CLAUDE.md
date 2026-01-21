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
- `just lint` - Lint Python (ruff) and SQL (sqlfluff) files

## Code Style
- Use ruff for Python formatting and linting
- Use sqlfluff for SQL linting
- Follow PEP 8 conventions for Python
- Use type hints where practical
