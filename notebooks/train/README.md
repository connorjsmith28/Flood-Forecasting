# Models

ML models for flood forecasting, tracked with [Weights & Biases](https://wandb.ai).

## Prerequisites

```bash
uv run wandb login  # Authenticate (one-time, get API key from wandb.ai/authorize)
just extract    # Load data from APIs
just transform  # Build dbt tables
```

## Team Setup

To collaborate, ask the project owner to invite you to the W&B team. Then runs automatically sync to the shared project.

## Usage

```bash
just experiment <model>      # Single run
just sweep <model>           # Hyperparameter sweep (5 runs)
just sweep <model> 20        # Sweep with 20 runs
```

## Creating a New Model

1. Copy `test_model.py` → `{name}.py`
2. Copy `test_model.yml` → `{name}.yml`
3. Update the model code and sweep parameters

## Starting a Model from Scratch

To create and run a new model from the source code in `src/`:

1. **Set up the environment:**
   ```bash
   uv sync
   ```

2. **Prepare the data:**
   ```bash
   just setup  # This runs data extraction and transformation
   ```

3. **Create a training script or notebook:**
   - Start with an existing example from `notebooks/train/` (e.g., copy `gru/series/gru_model.ipynb`).
   - Import model classes from `src/models/` (e.g., `from src.models.gru import GRUModel`).
   - Use preprocessing utilities from `src/preprocessing/` and helpers from `src/utils/`.
   - Configure the model parameters, features, and training settings.

4. **Run the training:**
   - Execute the notebook in VS Code or Jupyter.
   - Alternatively, convert to a Python script and run with `uv run python your_script.py`.
   - For hyperparameter sweeps, create a YAML config file and use `just sweep <model>`.

5. **Monitor and log with W&B:**
   - Ensure you have run `uv run wandb login`.
   - Training runs will automatically log metrics, models, and artifacts to Weights & Biases.

- **Runs are CLI-only**: W&B doesn't execute code. You run experiments locally (or on a server), and W&B logs the results.
- **Sweeps are hyperparameter tuning**: A sweep defines a search space and runs your model with different parameter combinations to find the best config.
- **Always set default values**: Your model should work standalone (`just experiment`) without a sweep. Define all hyperparameters with defaults in `wandb.init(config={...})`, then sweeps override them.
- **Data tracking**: Don't store datasets in wandb. Just update `data_description` when data changes.
- **Sweep methods**: Use `bayes` (default) to find good params faster, `grid` for exhaustive search.
- **Local `wandb/` folder**: Safe to gitignore/delete. It's just cache.
