# W&B MLOps Roadmap: Model Evaluation, Promotion, and Deployment

This document outlines the complete workflow for using Weights & Biases (W&B) to evaluate models, automatically promote the best model to production, and serve predictions via an API endpoint.

---

## Overview: What W&B Does vs. What You Build

### What W&B Provides

1. **Experiment Tracking** - Logs metrics, hyperparameters, and model artifacts from each training run
2. **Model Registry** - Versions models with aliases (`production`, `staging`, `latest`)
3. **Comparison UI** - Visual interface to compare runs and identify best models
4. **API Access** - Programmatic access to query models, metrics, and update aliases
5. **Automations** (Pro/Enterprise) - Webhooks and notifications when models are promoted

### What You Need to Build

1. **Standardized Evaluation** - Consistent metrics calculation across all models
2. **Promotion Logic** - Script that compares models and updates the `production` alias
3. **API Endpoint** - FastAPI service that polls W&B and auto-reloads the production model
4. **Prediction Pipeline** - Process that sends data to API and stores predictions
5. **Dashboard** - Visualization of predictions for end users

---

## Complete Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DAILY MODEL TRAINING WORKFLOW                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Day 1-10: Developers train models                                     │
│  ┌──────────────┐                                                       │
│  │ Training     │ ──► [W&B Run] ──► Logs metrics, saves model artifact │
│  │ Script       │                                                       │
│  └──────────────┘                                                       │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ STANDARDIZED EVALUATION (Your Custom Script)               │       │
│  │                                                              │       │
│  │ 1. Load model from W&B artifact                            │       │
│  │ 2. Run on held-out test set                                 │       │
│  │ 3. Calculate standard metrics (MAE, R², etc.)               │       │
│  │ 4. Log metrics to W&B run                                   │       │
│  └──────────────────────────────────────────────────────────────┘       │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ MODEL PROMOTION LOGIC (Your Custom Script)                   │       │
│  │                                                              │       │
│  │ 1. Query W&B API: "Get best model by metric X"             │       │
│  │ 2. Compare to current production model                       │       │
│  │ 3. If better:                                                │       │
│  │    - Add "production" alias to new model in W&B              │       │
│  │    - Trigger webhook to deployment service (optional)         │       │
│  └──────────────────────────────────────────────────────────────┘       │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ API ENDPOINT (FastAPI/Flask)                                │       │
│  │                                                              │       │
│  │ 1. Polls W&B for model with "production" alias              │       │
│  │ 2. Downloads and loads model                                │       │
│  │ 3. Serves predictions via /predict endpoint                 │       │
│  │ 4. No code changes needed - just reloads model              │       │
│  └──────────────────────────────────────────────────────────────┘       │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ PREDICTION PROCESS                                           │       │
│  │                                                              │       │
│  │ 1. Continuously receives new data                            │       │
│  │ 2. Calls API /predict endpoint                               │       │
│  │ 3. Gets predictions                                          │       │
│  └──────────────────────────────────────────────────────────────┘       │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ DASHBOARD (Streamlit/Grafana)                                │       │
│  │                                                              │       │
│  │ 1. Queries predictions from database                         │       │
│  │ 2. Displays visualizations                                  │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Roadmap

### Phase 1: Standardized Model Evaluation ✅

**Goal**: Ensure all training scripts evaluate models consistently.

**Implementation**:

1. Create a shared evaluation function that all models use
2. Standardize on key metrics (e.g., `test_mae`, `test_r2`, `test_rmse`)
3. Ensure all models log artifacts to W&B with consistent naming

**Example**:

```python
# models/utils/evaluation.py
import wandb
import pickle
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
import numpy as np

def evaluate_and_log(model, X_test, y_test, artifact_name="flood-model"):
    """
    Standardized evaluation function for all models.
    
    Args:
        model: Trained model
        X_test: Test features
        y_test: Test targets
        artifact_name: Name for W&B artifact
    """
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    # Log to W&B
    wandb.log({
        "test_mae": mae,
        "test_r2": r2,
        "test_rmse": rmse,
    })
    
    # Save model as artifact
    artifact = wandb.Artifact(artifact_name, type="model")
    with artifact.new_file("model.pkl", mode="wb") as f:
        pickle.dump(model, f)
    
    wandb.log_artifact(artifact)
    
    return {"mae": mae, "r2": r2, "rmse": rmse}
```

**Update existing models** to use this function:

```python
# models/test_model.py
from models.utils.evaluation import evaluate_and_log

# ... training code ...

# Replace manual evaluation with standardized function
metrics = evaluate_and_log(model, X_test, y_test)
```

---

### Phase 2: Model Promotion Script 🔄

**Goal**: Automatically compare new models to production and promote if better.

**Implementation**:

1. Create a script that queries W&B for recent runs
2. Compare metrics to current production model
3. Update `production` alias if new model is better
4. Schedule to run daily (via cron or GitHub Actions)

**Script**: `scripts/promote_model.py`

```python
#!/usr/bin/env python3
"""
Model Promotion Script

Compares recent model runs to the current production model.
If a new model performs better (by a threshold), promotes it to production.
"""

import wandb
from wandb.apis.public import Api
import sys
from datetime import datetime, timedelta

# Configuration
PROJECT_NAME = "flood-forecasting"
ARTIFACT_NAME = "flood-model"
METRIC_NAME = "test_mae"  # Lower is better
IMPROVEMENT_THRESHOLD = 0.05  # 5% improvement required
LOOKBACK_DAYS = 7  # Check models from last 7 days

def get_current_production_model(api):
    """Get the current production model and its metric."""
    try:
        artifact = api.artifact(f"{PROJECT_NAME}/{ARTIFACT_NAME}:production")
        run = artifact.logged_by()
        metric = run.summary.get(METRIC_NAME)
        return artifact, run, metric
    except Exception as e:
        print(f"No production model found: {e}")
        return None, None, float("inf")

def find_best_recent_model(api):
    """Find the best model from recent runs."""
    cutoff_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    
    runs = api.runs(
        PROJECT_NAME,
        filters={
            "created_at": {"$gte": cutoff_date.isoformat()},
            "state": "finished",
        },
        order=f"-summary_metrics.{METRIC_NAME}",  # Best metric first
    )
    
    for run in runs:
        metric = run.summary.get(METRIC_NAME)
        if metric is not None:
            # Get model artifact from this run
            artifacts = run.logged_artifacts()
            model_artifacts = [a for a in artifacts if a.name == ARTIFACT_NAME]
            if model_artifacts:
                return model_artifacts[0], run, metric
    
    return None, None, float("inf")

def promote_model(artifact, run):
    """Add production alias to model artifact."""
    artifact.aliases.append("production")
    artifact.save()
    print(f"✅ Promoted model from run {run.id} to production")
    print(f"   Metric ({METRIC_NAME}): {run.summary.get(METRIC_NAME)}")
    print(f"   Run URL: {run.url}")

def main():
    api = Api()
    
    # Get current production model
    prod_artifact, prod_run, prod_metric = get_current_production_model(api)
    
    if prod_artifact is None:
        print("⚠️  No production model found. Promoting first available model.")
        best_artifact, best_run, best_metric = find_best_recent_model(api)
        if best_artifact:
            promote_model(best_artifact, best_run)
            return
        else:
            print("❌ No models found to promote")
            sys.exit(1)
    
    print(f"Current production model:")
    print(f"  Run ID: {prod_run.id}")
    print(f"  Metric ({METRIC_NAME}): {prod_metric}")
    
    # Find best recent model
    best_artifact, best_run, best_metric = find_best_recent_model(api)
    
    if best_artifact is None:
        print("❌ No recent models found")
        sys.exit(0)
    
    print(f"\nBest recent model:")
    print(f"  Run ID: {best_run.id}")
    print(f"  Metric ({METRIC_NAME}): {best_metric}")
    
    # Check if better (for MAE, lower is better)
    improvement = (prod_metric - best_metric) / prod_metric
    
    if improvement >= IMPROVEMENT_THRESHOLD:
        print(f"\n✅ New model is {improvement*100:.1f}% better (threshold: {IMPROVEMENT_THRESHOLD*100}%)")
        
        # Remove production alias from old model
        prod_artifact.aliases = [a for a in prod_artifact.aliases if a != "production"]
        prod_artifact.save()
        
        # Promote new model
        promote_model(best_artifact, best_run)
    else:
        print(f"\n❌ New model not better enough ({improvement*100:.1f}% improvement, need {IMPROVEMENT_THRESHOLD*100}%)")
        print("   Keeping current production model")

if __name__ == "__main__":
    main()
```

**Schedule with GitHub Actions**:

```yaml
# .github/workflows/promote-model.yml
name: Promote Best Model

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily
  workflow_dispatch:  # Manual trigger

jobs:
  promote:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install wandb scikit-learn
      
      - name: Promote model
        env:
          WANDB_API_KEY: ${{ secrets.WANDB_API_KEY }}
        run: |
          python scripts/promote_model.py
```

---

### Phase 3: API Endpoint with Auto-Reload 🔄

**Goal**: Create an API that automatically loads the production model from W&B and reloads when it changes.

**Implementation**:

1. Create FastAPI service
2. Background thread that polls W&B for production model
3. Reload model when version changes
4. Serve predictions via `/predict` endpoint

**Script**: `api/inference_server.py`

```python
"""
Flood Forecasting Inference API

Automatically loads production model from W&B and serves predictions.
Reloads model when production alias changes (no code changes needed).
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import wandb
import pickle
import time
import os
from threading import Thread
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Flood Forecasting API")

# Global state
current_model = None
current_model_version = None
model_metadata = {}

class PredictionRequest(BaseModel):
    """Request format for predictions."""
    precipitation_mm: float
    temperature_c: float
    wind_speed_ms: float
    humidity_pct: float
    # Add other features as needed

class PredictionResponse(BaseModel):
    """Response format for predictions."""
    prediction: float
    model_version: str
    model_run_id: str
    model_metric: dict

def load_production_model():
    """Load model with 'production' alias from W&B."""
    try:
        api = wandb.Api()
        artifact = api.artifact("flood-forecasting/flood-model:production")
        
        # Download artifact
        artifact_dir = artifact.download()
        
        # Load model
        model_path = Path(artifact_dir) / "model.pkl"
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        
        # Get metadata
        run = artifact.logged_by()
        version = artifact.version
        
        return model, version, run
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        raise

def reload_model_loop():
    """Check for new production model every 5 minutes."""
    global current_model, current_model_version, model_metadata
    
    # Initial load
    try:
        model, version, run = load_production_model()
        current_model = model
        current_model_version = version
        model_metadata = {
            "run_id": run.id,
            "run_url": run.url,
            "metrics": run.summary,
        }
        logger.info(f"✅ Loaded production model version {version} (run {run.id})")
    except Exception as e:
        logger.error(f"Failed to load initial model: {e}")
        return
    
    # Poll for updates
    while True:
        try:
            time.sleep(300)  # Check every 5 minutes
            
            model, version, run = load_production_model()
            
            if version != current_model_version:
                logger.info(f"🔄 New production model detected: {version}")
                current_model = model
                current_model_version = version
                model_metadata = {
                    "run_id": run.id,
                    "run_url": run.url,
                    "metrics": run.summary,
                }
                logger.info(f"✅ Reloaded model version {version} (run {run.id})")
        except Exception as e:
            logger.error(f"Error in reload loop: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

@app.on_event("startup")
async def startup_event():
    """Start background thread on API startup."""
    thread = Thread(target=reload_model_loop, daemon=True)
    thread.start()
    logger.info("🚀 Inference API started, loading production model...")

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "model_loaded": current_model is not None,
        "model_version": current_model_version,
    }

@app.get("/model/info")
async def model_info():
    """Get information about current production model."""
    if current_model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    return {
        "version": current_model_version,
        "run_id": model_metadata.get("run_id"),
        "run_url": model_metadata.get("run_url"),
        "metrics": model_metadata.get("metrics", {}),
    }

@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """Make a prediction using the production model."""
    if current_model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    # Convert request to feature array
    features = [
        request.precipitation_mm,
        request.temperature_c,
        request.wind_speed_ms,
        request.humidity_pct,
    ]
    
    # Make prediction
    prediction = current_model.predict([features])[0]
    
    return PredictionResponse(
        prediction=float(prediction),
        model_version=current_model_version,
        model_run_id=model_metadata.get("run_id", "unknown"),
        model_metric=model_metadata.get("metrics", {}),
    )

@app.post("/predict/batch")
async def predict_batch(requests: list[PredictionRequest]):
    """Make batch predictions."""
    if current_model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    features_list = [
        [
            r.precipitation_mm,
            r.temperature_c,
            r.wind_speed_ms,
            r.humidity_pct,
        ]
        for r in requests
    ]
    
    predictions = current_model.predict(features_list).tolist()
    
    return {
        "predictions": predictions,
        "model_version": current_model_version,
        "count": len(predictions),
    }
```

**Deploy**:

```bash
# Install dependencies
pip install fastapi uvicorn wandb scikit-learn

# Run locally
uvicorn api.inference_server:app --reload --port 8000

# Or deploy to Railway/Render/HuggingFace Spaces
```

---

### Phase 4: Prediction Pipeline 🔄

**Goal**: Continuously send new data to API and store predictions.

**Implementation**:

1. Dagster asset that queries latest data
2. Calls API `/predict` endpoint
3. Stores predictions in DuckDB
4. Runs on schedule (every 15 minutes)

**Script**: `orchestration/assets/predictions.py`

```python
"""
Dagster asset for generating predictions using production model.
"""

from dagster import asset, AssetExecutionContext
import requests
import duckdb
from datetime import datetime

API_URL = "http://localhost:8000"  # Or your deployed URL

@asset(
    group_name="ml",
    description="Generate flood predictions using production model",
)
def flood_predictions(context: AssetExecutionContext):
    """Query latest data and generate predictions."""
    
    # Load latest features from DuckDB
    con = duckdb.connect("flood_forecasting.duckdb", read_only=True)
    
    # Get latest hourly data for each site
    df = con.execute("""
        SELECT 
            site_id,
            timestamp,
            precipitation_mm,
            temperature_c,
            wind_speed_ms,
            humidity_pct
        FROM marts.fct_streamflow_hourly
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
          AND precipitation_mm IS NOT NULL
        ORDER BY timestamp DESC
    """).df()
    
    con.close()
    
    if df.empty:
        context.log.info("No new data to predict")
        return []
    
    # Call API for each row
    predictions = []
    for _, row in df.iterrows():
        payload = {
            "precipitation_mm": row["precipitation_mm"],
            "temperature_c": row["temperature_c"],
            "wind_speed_ms": row["wind_speed_ms"],
            "humidity_pct": row["humidity_pct"],
        }
        
        try:
            response = requests.post(f"{API_URL}/predict", json=payload)
            response.raise_for_status()
            result = response.json()
            
            predictions.append({
                "site_id": row["site_id"],
                "timestamp": row["timestamp"],
                "prediction": result["prediction"],
                "model_version": result["model_version"],
                "predicted_at": datetime.now(),
            })
        except Exception as e:
            context.log.error(f"Error predicting for {row['site_id']}: {e}")
    
    # Store predictions in DuckDB
    if predictions:
        con = duckdb.connect("flood_forecasting.duckdb")
        con.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                site_id VARCHAR,
                timestamp TIMESTAMP,
                prediction DOUBLE,
                model_version VARCHAR,
                predicted_at TIMESTAMP
            )
        """)
        
        con.executemany(
            """
            INSERT INTO predictions 
            (site_id, timestamp, prediction, model_version, predicted_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    p["site_id"],
                    p["timestamp"],
                    p["prediction"],
                    p["model_version"],
                    p["predicted_at"],
                )
                for p in predictions
            ],
        )
        con.close()
        
        context.log.info(f"Stored {len(predictions)} predictions")
    
    return predictions
```

**Schedule in Dagster**:

```python
# orchestration/jobs.py
from dagster import define_asset_job, ScheduleDefinition
from orchestration.assets.predictions import flood_predictions

prediction_job = define_asset_job(
    name="generate_predictions",
    selection=[flood_predictions],
)

prediction_schedule = ScheduleDefinition(
    job=prediction_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
)
```

---

### Phase 5: Dashboard 📊

**Goal**: Visualize predictions for end users.

**Implementation**:

1. Streamlit app that queries predictions from DuckDB
2. Displays time series, maps, alerts
3. Shows current model version and metrics

**Script**: `dashboard/app.py`

```python
"""
Streamlit dashboard for flood predictions.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Flood Forecasting Dashboard", layout="wide")

# Connect to database
con = duckdb.connect("flood_forecasting.duckdb", read_only=True)

# Header
st.title("🌊 Flood Forecasting Dashboard")

# Get current model info
model_info = con.execute("""
    SELECT DISTINCT model_version
    FROM predictions
    ORDER BY predicted_at DESC
    LIMIT 1
""").df()

if not model_info.empty:
    st.sidebar.metric("Model Version", model_info.iloc[0]["model_version"])

# Get recent predictions
df = con.execute("""
    SELECT 
        site_id,
        timestamp,
        prediction,
        model_version,
        predicted_at
    FROM predictions
    WHERE predicted_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    ORDER BY predicted_at DESC
""").df()

if df.empty:
    st.warning("No predictions available")
else:
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Predictions", len(df))
    col2.metric("Sites Monitored", df["site_id"].nunique())
    col3.metric("Latest Prediction", df["predicted_at"].max())
    
    # Time series plot
    st.subheader("Prediction Time Series")
    
    fig = px.line(
        df,
        x="timestamp",
        y="prediction",
        color="site_id",
        title="Flood Predictions Over Time",
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Recent predictions table
    st.subheader("Recent Predictions")
    st.dataframe(df.head(100))

con.close()
```

**Deploy**:

```bash
# Run locally
streamlit run dashboard/app.py

# Or deploy to Streamlit Cloud
```

---

## Key Points: Automatic Model Promotion

### How It Works

1. **Daily Training**: Developers train models, W&B tracks metrics
2. **Daily Promotion Check**: Script compares new models to production
3. **Alias Update**: If better, script adds `production` alias to new model
4. **Auto-Reload**: API polls W&B every 5 minutes, reloads when alias changes
5. **Zero Downtime**: Old model serves requests while new model loads

### No Code Changes Required

- ✅ API automatically detects new production model
- ✅ API reloads model in background thread
- ✅ Predictions continue during reload (uses old model until new one ready)
- ✅ Dashboard automatically shows predictions from new model

### Promotion Criteria

Currently configured:
- **Metric**: `test_mae` (lower is better)
- **Threshold**: 5% improvement required
- **Lookback**: Last 7 days of models

Customize in `scripts/promote_model.py`:
- Change `METRIC_NAME` for different metrics
- Change `IMPROVEMENT_THRESHOLD` for different thresholds
- Change `LOOKBACK_DAYS` for different time windows

---

## Testing the Workflow

### 1. Train a Model

```bash
just experiment test_model
```

### 2. Manually Promote First Model

```python
import wandb
api = wandb.Api()
artifact = api.artifact("flood-forecasting/flood-model:latest")
artifact.aliases.append("production")
artifact.save()
```

### 3. Start API

```bash
uvicorn api.inference_server:app --reload
```

### 4. Test Prediction

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "precipitation_mm": 10.5,
    "temperature_c": 15.0,
    "wind_speed_ms": 5.2,
    "humidity_pct": 65.0
  }'
```

### 5. Train Better Model

```bash
just experiment test_model  # With improved hyperparameters
```

### 6. Run Promotion Script

```bash
python scripts/promote_model.py
```

### 7. Verify API Reloaded

```bash
curl http://localhost:8000/model/info
```

---

## Next Steps

1. ✅ **Phase 1**: Implement standardized evaluation function
2. ✅ **Phase 2**: Create model promotion script
3. ✅ **Phase 3**: Build API with auto-reload
4. ✅ **Phase 4**: Create prediction pipeline
5. ✅ **Phase 5**: Build dashboard

**Optional Enhancements**:

- Add A/B testing (serve multiple models, compare performance)
- Add model monitoring (track prediction drift)
- Add alerting (notify when model performance degrades)
- Add canary deployments (gradually roll out new models)
- Integrate with W&B Automations (Pro tier) for webhooks

---

## FAQ

**Q: Does W&B automatically promote models?**  
A: No. W&B provides the registry and API, but you build the promotion logic.

**Q: Do I need to redeploy the API when a new model is promoted?**  
A: No. The API polls W&B and automatically reloads the production model.

**Q: What happens if the API can't reach W&B?**  
A: The API continues serving with the last loaded model. Logs errors and retries.

**Q: Can I promote models manually?**  
A: Yes. Use W&B UI to add `production` alias, or run the promotion script manually.

**Q: What if multiple models are better than production?**  
A: The promotion script picks the best one (lowest MAE). You can customize this logic.

**Q: How do I roll back to a previous model?**  
A: Manually add `production` alias to the previous model artifact in W&B UI.
