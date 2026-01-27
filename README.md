# Flood Forecasting

ML flood forecasting for the Missouri River Basin (HUC 10). Extracts hydrological and meteorological data from public APIs, transforms it with dbt, and trains models tracked with Weights & Biases.

## Quick Start

### 1. Install Prerequisites

**macOS:**
```bash
brew bundle install
```

**Windows:** Install using Chocolatey:

First, install Chocolatey (run PowerShell as Administrator):
```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
```

Then install the packages (run PowerShell as Administrator):
```powershell
choco install uv just duckdb -y
```

**Linux:** Install manually:
- [uv](https://docs.astral.sh/uv/getting-started/installation/) - Python package manager
- [just](https://github.com/casey/just#installation) - Command runner
- [duckdb](https://duckdb.org/docs/installation/) - Database CLI

### 2. Install Python Dependencies

```bash
uv sync
```

### 3. Build the Database

Extract data from USGS, Open-Meteo, GAGES-II, and NLDI APIs, then transform with dbt:

```bash
just extract     # ~3-5 min first run
just transform   # Build dbt models
```

This creates `flood_forecasting.duckdb` with streamflow observations joined to weather forcing data.

### 4. Run Experiments

Authenticate with Weights & Biases (one-time):
```bash
uv run wandb login
```

Run a model:
```bash
just experiment test_model      # Single training run
just sweep test_model           # Hyperparameter sweep (5 runs)
just sweep test_model 20        # Sweep with 20 runs
```

Results are logged to the [flood-forecasting](https://wandb.ai) W&B project.

## Commands

| Command | Description |
|---------|-------------|
| `just extract` | Extract data from APIs |
| `just transform` | Run dbt build (run + test) |
| `just experiment <model>` | Run single training experiment |
| `just sweep <model> [n]` | Run hyperparameter sweep |
| `just dagster` | Launch Dagster UI |
| `just db` | Launch DuckDB UI (read-only) |
| `just lint` | Lint Python and SQL |
| `just dbt-docs` | Generate and serve dbt docs |

## Project Structure

```
elt/
  extraction/           # Python scripts to fetch data from APIs
  transformation/       # dbt project (staging + marts)
orchestration/          # Dagster assets and jobs
models/                 # ML models (training scripts + sweep configs)
exploratory/            # Notebooks
```

## Data Sources

| Source | Data | API |
|--------|------|-----|
| USGS NWIS | Streamflow (15-min) + site metadata | dataretrieval |
| Open-Meteo | Hourly weather forcing | open-meteo.com |
| GAGES-II | Static watershed attributes | ScienceBase |
| NLDI | Basin characteristics | pygeohydro |

## Creating New Models

1. Copy `models/test_model.py` and `models/test_model.yml`
2. Update the model architecture and sweep parameters
3. Run with `just experiment <name>` or `just sweep <name>`

See [models/README.md](models/README.md) for details.

## Tech Stack

- **Data Processing**: Polars, DuckDB
- **ML Frameworks**: PyTorch, scikit-learn
- **Experiment Tracking**: Weights & Biases
- **Orchestration**: Dagster
- **Transformation**: dbt
- **Linting**: ruff (Python), sqlfluff (SQL)
