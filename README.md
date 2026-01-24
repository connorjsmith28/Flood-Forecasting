# Flood Forecasting

A flood forecasting project using ML models with Polars, PyTorch, and scikit-learn. Data is extracted from USGS, NLDAS, and GAGES-II sources, transformed with dbt, and orchestrated with Dagster.

## Getting Started

### Prerequisites

Install Homebrew dependencies:

```bash
brew bundle install
```

This installs:
- `uv` - Python package manager
- `just` - Command runner
- `duckdb` - Database CLI with web UI

Note, if you don't have a mac you can't use homebrew or brew bundle. You'll need to manually download uv, just, and duckdb. 
Once you do that, just and uv will handle the remaining libraries. 

### Install Python Dependencies

```bash
uv sync
```

### Run the Pipeline

1. **Extract data** from hydrology APIs:
   ```bash
   just extract
   ```

2. **Transform data** with dbt:
   ```bash
   just transform
   ```

### Development

| Command | Description |
|---------|-------------|
| `just dagster` | Launch Dagster UI (opens browser) |
| `just db` | Launch DuckDB UI (read-only) |
| `just db-write` | Launch DuckDB UI (write access) |
| `just extract` | Run extraction job |
| `just extract-fresh` | Clear cache and run extraction |
| `just transform` | Run dbt build (run + test) |
| `just lint` | Lint Python and SQL files |
| `just dbt-docs` | Generate and serve dbt docs |

## Project Structure

```
elt/
  extraction/     # Data extraction scripts
  transformation/ # dbt models
orchestration/    # Dagster pipelines
exploratory/      # Notebooks
models/           # ML model code
```

## Tech Stack

- **Data Processing**: Polars
- **ML Frameworks**: PyTorch, scikit-learn
- **Orchestration**: Dagster
- **Transformation**: dbt + DuckDB
- **Linting**: ruff (Python), sqlfluff (SQL)
