# Cross-platform shell configuration
# Unix (macOS/Linux) uses sh by default
# Windows automatically uses PowerShell via windows-shell setting
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Clean and recreate virtual environment (fixes corrupted packages)
[unix]
reset-venv:
    rm -rf .venv
    uv sync

[windows]
reset-venv:
    if (Test-Path .venv) { Remove-Item -Recurse -Force .venv }
    uv sync

# Clean leftover Dagster temp directories (fixes permission errors)
[unix]
clean-dagster:
    rm -rf .tmp_dagster_home_*

[windows]
clean-dagster:
    Get-ChildItem -Force -Filter ".tmp_dagster_home_*" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Run full pipeline setup (extract + transform)
setup: extract transform

# Format and fix all code
lint:
    uv run ruff check --fix .
    uv run ruff format .
    uv run sqlfluff fix elt/transformation/

# Launch Dagster UI
dagster:
    uv run python -c "import threading, webbrowser, time; threading.Timer(3, lambda: webbrowser.open('http://localhost:3000')).start()"
    uv run dagster dev -m orchestration.definitions

# Launch DuckDB UI (opens in browser, read-only to allow concurrent Dagster runs)
db:
    duckdb -readonly -ui flood_forecasting.duckdb

# Launch DuckDB UI with write access (closes lock on exit)
db-write:
    duckdb -ui flood_forecasting.duckdb

# Run full extraction job (USGS, NLDAS, GAGES-II)
extract:
    uv run dagster job execute -m orchestration.definitions -j extraction_job

# Run full extraction job with fresh data (clears HTTP cache first)
extract-fresh:
    uv run python -c "import shutil; shutil.rmtree('cache', ignore_errors=True)"
    uv run dagster job execute -m orchestration.definitions -j extraction_job

# dbt project paths
dbt_project := "elt/transformation"
dbt_args := "--project-dir " + dbt_project + " --profiles-dir " + dbt_project

# DuckDB path (must match orchestration/definitions.py)
export DUCKDB_PATH := justfile_directory() / "flood_forecasting.duckdb"

# Run full transformation (dbt build = run + test)
transform:
    uv run dbt build {{dbt_args}}

# Parse dbt project and generate manifest (required before running Dagster with dbt)
dbt-parse:
    uv run dbt parse {{dbt_args}}

# Run dbt models
dbt-run:
    uv run dbt run {{dbt_args}}

# Test dbt models
dbt-test:
    uv run dbt test {{dbt_args}}

# Build dbt (run + test)
dbt-build:
    uv run dbt build {{dbt_args}}

# Generate dbt docs
dbt-docs:
    uv run dbt docs generate {{dbt_args}}
    uv run dbt docs serve {{dbt_args}}

# Run ML experiment (logs to wandb)
experiment model:
    uv run python models/{{model}}.py


# Create and run a wandb sweep
sweep model count="5":
    uv run python -c "import subprocess, re, sys; output = subprocess.run(['uv', 'run', 'wandb', 'sweep', 'models/{{model}}.yml', '--project', 'flood-forecasting'], capture_output=True, text=True); print(output.stdout, end=''); print(output.stderr, end='', file=sys.stderr); match = re.search(r'wandb agent ([^\s]+)', output.stdout + output.stderr); sweep_id = match.group(1) if match else None; sys.exit(1) if not sweep_id else subprocess.run(['uv', 'run', 'wandb', 'agent', sweep_id, '--count', '{{count}}'])"

# Full pipeline: extract, transform, upload to W&B (incremental)
sync-wandb:
    uv run dagster job execute -m orchestration.definitions -j sync_job

# Full pipeline with fresh data (clears cache, replaces W&B data)
sync-wandb-fresh:
    uv run python -c "import shutil; shutil.rmtree('cache', ignore_errors=True)"
    uv run dagster job execute -m orchestration.definitions -j sync_job --config '{"ops": {"wandb_dataset": {"config": {"full_refresh": true}}}}'
