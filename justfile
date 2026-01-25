# Format and lint all code
lint:
    uv run ruff format .
    uv run ruff check --fix .
    uv run sqlfluff fix elt/transformation/

# Launch Dagster UI
dagster:
    (sleep 3 && open http://localhost:3000) & uv run dagster dev -m orchestration.definitions

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
    rm -rf cache/
    uv run dagster job execute -m orchestration.definitions -j extraction_job

# Run extraction with full refresh (drops and recreates tables)
# Usage: just extract-refresh local  OR  just extract-refresh prod
extract-refresh env:
    #!/usr/bin/env bash
    if [ "{{env}}" = "local" ]; then
        uv run dagster job execute -m orchestration.definitions -j extraction_job \
            --config <(echo 'resources: {duckdb: {config: {full_refresh: true}}}')
    elif [ "{{env}}" = "prod" ]; then
        uv run dagster job execute -m orchestration.definitions -j extraction_job \
            --config <(echo 'resources: {duckdb: {config: {full_refresh: true, is_production: true}}}')
    else
        echo "Usage: just extract-refresh [local|prod]"
        exit 1
    fi

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

# Run ML experiment (logs to wandb)
experiment model:
    uv run python models/{{model}}.py

# Create and run a wandb sweep
sweep model count="5":
    #!/usr/bin/env bash
    output=$(uv run wandb sweep models/{{model}}.yml --project flood-forecasting 2>&1)
    echo "$output"
    agent_cmd=$(echo "$output" | grep -oE 'wandb agent [^ ]+')
    uv run $agent_cmd --count {{count}}
