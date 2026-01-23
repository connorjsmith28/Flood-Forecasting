# Run full pipeline setup (extract + transform)
setup: extract transform

# Lint all code
lint:
    ruff check .
    sqlfluff lint elt/transformation/

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
    uv run dbt docs generate {{dbt_args}} && uv run dbt docs serve {{dbt_args}}
