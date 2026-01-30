"""dbt transformation asset for flood forecasting."""

from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult,
    MetadataValue,
)
from dagster_dbt import DbtCliResource

from orchestration.utils import DBT_PROJECT_DIR


@asset(
    group_name="transformation",
    description="Run dbt transformations (staging + marts)",
    compute_kind="dbt",
    deps=[
        "usgs_site_metadata",
        "usgs_streamflow_15min",
        "usgs_streamflow_daily",
        "weather_forcing_raw",
    ],
)
def dbt_flood_forecasting(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
) -> MaterializeResult:
    """Run dbt models for flood forecasting transformations.

    These models transform raw extracted data into analytics-ready tables:
    - staging models: clean and standardize raw data
    - mart models: join and aggregate for ML/analytics

    This asset explicitly depends on all extraction assets to ensure raw data
    tables exist before dbt tries to read from them.
    """
    context.log.info(f"Running dbt build in {DBT_PROJECT_DIR}")

    # Run dbt build and wait for completion
    # Note: Don't pass context= since this is a regular @asset, not @dbt_assets
    # This will raise DagsterDbtCliRuntimeError if dbt fails
    dbt_invocation = dbt.cli(["build"]).wait()

    # Get run results for metadata
    run_results = dbt_invocation.get_artifact("run_results.json")

    # Build metadata from results
    results = run_results.get("results", [])
    metadata = {
        "total_models": len(results),
        "successful": sum(1 for r in results if r.get("status") == "success"),
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
    }

    # List model names
    model_names = [r.get("unique_id", "").split(".")[-1] for r in results]
    metadata["models"] = MetadataValue.json(model_names[:20])

    context.log.info(f"dbt build completed: {metadata}")

    return MaterializeResult(metadata=metadata)
