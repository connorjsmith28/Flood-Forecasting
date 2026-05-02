# brew/

This directory contains configuration for installing project dependencies using Homebrew (macOS/Linux package manager).

## Brewfile

The `Brewfile` defines the system-level dependencies required for the flood forecasting project:

- **uv**: Fast Python package installer and resolver (replaces pip, virtualenv, etc.)
- **just**: Command runner for executing project tasks (similar to make)
- **duckdb**: Embedded analytical database CLI, used for local data querying and UI

## Installation

If you're on macOS or Linux and have Homebrew installed:

1. Install Homebrew (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. Install dependencies from the Brewfile:
   ```bash
   brew bundle --file=brew/Brewfile
   ```

3. Set up the Python environment:
   ```bash
   uv sync
   ```

## Windows Users

On Windows, install the equivalent tools manually:

- **uv**: Download from [astral.sh/uv](https://astral.sh/uv) or use `pip install uv`
- **just**: Download from [github.com/casey/just/releases](https://github.com/casey/just/releases)
- **duckdb**: Download from [duckdb.org](https://duckdb.org/) or use `pip install duckdb`

Then proceed with `uv sync` to set up the virtual environment.

## Usage

After installation, use `just` commands for common tasks:

- `just setup` — Extract and transform data
- `just experiment <model>` — Run a single model training experiment
- `just sweep <model>` — Run hyperparameter sweeps

Use DuckDB for querying the local database:

- `just db` — Open DuckDB UI in browser
- `duckdb data/database/database.duckdb` — CLI access