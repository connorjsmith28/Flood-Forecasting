# Flood Forecasting

ML flood forecasting for the Missouri River Basin (HUC 10). Extracts hydrological and meteorological data from public APIs, transforms it with dbt, and trains deep learning models tracked with Weights & Biases.

Implements LSTM, GRU, hybrid LSTM-Transformer, Temporal Fusion Transformer (TFT), and Graph Neural Network (GNN) models for multi-site streamflow prediction.

https://dashboard.waterdata.usgs.gov/app/nwd/en/

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

This creates `data/database/database.duckdb` with streamflow observations joined to weather forcing data.

### 3.5 Authenticate with NASA Earthdata (one-time)

Required for NLDAS-2 watershed-averaged forcing data. Create a free account at https://urs.earthdata.nasa.gov/, then add credentials to `~/.netrc`:

```bash
echo -e "machine urs.earthdata.nasa.gov\n  login YOUR_USERNAME\n  password YOUR_PASSWORD" >> ~/.netrc
chmod 600 ~/.netrc
```

Then authorize the GES DISC data archive at https://disc.gsfc.nasa.gov/earthdata-login

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
| `just extract-fresh` | Clear HTTP cache and re-extract |
| `just transform` | Run dbt build (run + test) |
| `just experiment <model>` | Run single training experiment |
| `just sweep <model> [n]` | Run hyperparameter sweep (default 5 runs) |
| `just dagster` | Launch Dagster UI |
| `just db` | Launch DuckDB UI (read-only) |
| `just db-write` | Launch DuckDB UI (write access) |
| `just download-wandb` | Download dataset artifact from W&B |
| `just lint` | Lint Python and SQL |
| `just dbt-docs` | Generate and serve dbt docs |

## Project Structure

```
data/
  database/             # DuckDB database
  duckdb/queries/       # Saved SQL queries
  dagster/orchestration/ # Dagster assets and jobs
src/
  elt/
    extraction/         # Python scripts to fetch data from APIs
    transformation/     # dbt project (staging + marts)
  models/               # Model definitions (LSTM, GRU, Hybrid, TFT, GNN, Transformer)
  preprocessing/        # Data preprocessing and GNN graph preprocessing
  postprocessing/       # Inference, evaluation, and SHAP explainability
  utils/                # Helper functions
  static/               # Static assets (flood quantile thresholds)
notebooks/
  model_training/       # Training scripts + sweep configs (lstm/, gru/, hybrid/, tft/, gnn/)
  model_exploration/    # Post-training analysis with SHAP (lstm/, gru/, hybrid/)
  data_exploration/     # Data analysis notebooks
demo/                   # Standalone demo (train.ipynb + inference.ipynb)
models/                 # Saved model weights (.keras)
artifacts/              # W&B dataset artifacts
brew/                   # Brewfile for macOS dependencies
```

## Data Sources

### Extracted Data

| Source | Data | API |
|--------|------|-----|
| USGS NWIS | Streamflow + site metadata | dataretrieval |
| Open-Meteo | Hourly weather forcing | open-meteo.com |

**USGS Site Coverage (Missouri Basin / HUC 10):**
- ~13,000 total stream gage sites in USGS database
- ~3,400 sites have discharge (water level) data
- ~1,300 of those have instantaneous values (IV, 15-min resolution)
- ~2,700 have daily values (includes sites without IV)

We extract both IV and daily data. Sites are flagged with `has_iv` and `has_daily` in the metadata.

### Seed Data (Static Attributes)

Seed data comes from the [CAMELS dataset](https://www.osti.gov/pages/servlets/purl/2574906) and provides static watershed characteristics:

| Source | Data | Description |
|--------|------|-------------|
| GAGES-II | Watershed attributes | Land cover, geology, soils, climate indices |
| HydroATLAS | Catchment attributes | 195+ hydrological/environmental variables |
| NLDAS-2 | Climate indices | Aridity index, precipitation seasonality |

## Creating New Models

1. Copy `notebooks/model_training/lstm/series/lstm_model.ipynb` or any existing model notebook as a starting point
2. Add a new `.py` training script and `.yml` sweep config under `notebooks/model_training/`
3. Implement the model class in `src/models/` extending `BaseModel`
4. Run with `just experiment <name>` or `just sweep <name>`

See [notebooks/model_training/README.md](notebooks/model_training/README.md) for details.

## Model Architectures

| Model | Description |
|-------|-------------|
| LSTM | Two-layer LSTM with optional asymmetric MSE loss (penalises under-prediction) |
| GRU | Gated Recurrent Unit — lighter alternative to LSTM |
| Hybrid | LSTM + Transformer encoder for sequence-to-scalar prediction |
| TFT | Temporal Fusion Transformer via Darts library with attention explainability |
| GNN | Spatio-temporal GNN: per-node LSTM → GATv2 message passing over river network |

SHAP explainability analysis is available for LSTM, GRU, and Hybrid models via `src/postprocessing/postprocessing.py` and the `notebooks/model_exploration/` notebooks.

## Tech Stack

- **Data Processing**: Polars, DuckDB
- **ML Frameworks**: TensorFlow/Keras (LSTM, GRU, Hybrid), PyTorch + PyTorch Geometric (GNN), Darts (TFT)
- **Explainability**: SHAP
- **Experiment Tracking**: Weights & Biases
- **Orchestration**: Dagster
- **Transformation**: dbt
- **Linting**: ruff (Python), sqlfluff (SQL)
