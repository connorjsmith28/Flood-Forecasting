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

## Tips

- **Runs are CLI-only**: W&B doesn't execute code. You run experiments locally (or on a server), and W&B logs the results.
- **Sweeps are hyperparameter tuning**: A sweep defines a search space and runs your model with different parameter combinations to find the best config.
- **Always set default values**: Your model should work standalone (`just experiment`) without a sweep. Define all hyperparameters with defaults in `wandb.init(config={...})`, then sweeps override them.
- **Data tracking**: Don't store datasets in wandb. Just update `data_description` when data changes.
- **Sweep methods**: Use `bayes` (default) to find good params faster, `grid` for exhaustive search.
- **Local `wandb/` folder**: Safe to gitignore/delete. It's just cache.
