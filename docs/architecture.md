# Flood Forecasting - System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA SOURCES                                          │
│                                                                                         │
│    [USGS Water Data]     [NOAA Weather]     [Historical Records]     [Other APIs]      │
│           │                    │                    │                     │             │
└───────────┴────────────────────┴────────────────────┴─────────────────────┴─────────────┘
                                            │
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              SCHEDULED DATA PIPELINE                                    │
│                              GitHub Actions → Dagster                                   │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│    GitHub Actions (cron trigger)                                                       │
│    │                                                                                    │
│    │  schedule: '*/15 * * * *' or '*/30 * * * *'                                       │
│    │                                                                                    │
│    ▼                                                                                    │
│    ┌──────────────────────────────────────────────────────────────────────────────┐    │
│    │                         DAGSTER JOB                                          │    │
│    │                         (runs in GitHub Actions runner)                      │    │
│    │                                                                              │    │
│    │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐  │    │
│    │   │   Extract    │    │  Transform   │    │   Feature    │    │  Log to  │  │    │
│    │   │   (Python)   │───▶│   (dbt)      │───▶│   Engineer   │───▶│  W&B     │  │    │
│    │   │              │    │              │    │   (Polars)   │    │          │  │    │
│    │   └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘  │    │
│    │                                                                              │    │
│    │   Extract:  Fetch from USGS, NOAA APIs                                      │    │
│    │   dbt:      SQL transforms, staging → marts, schema tests                   │    │
│    │   Polars:   ML feature engineering (windowing, aggregations, etc.)          │    │
│    │                                                                              │    │
│    │   dagster job execute -j flood_data_pipeline                                │    │
│    │                                                                              │    │
│    └──────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                         │
│    Note: Dagster provides the pipeline DAG, retries, and asset tracking.               │
│          GitHub Actions provides the scheduling (since Dagster isn't hosted).          │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              WEIGHTS & BIASES (Academic 100GB)                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│    │                              ARTIFACTS                                          │  │
│    ├─────────────────────────────────────────────────────────────────────────────────┤  │
│    │                                                                                 │  │
│    │   flood-raw-data:v12        flood-features:v8        flood-model:v3            │  │
│    │   ├── 2024/*.parquet        ├── daily_agg.parquet    ├── model.pt              │  │
│    │   ├── 2023/*.parquet        ├── hourly_agg.parquet   ├── config.yaml           │  │
│    │   └── ...                   └── metadata.json        └── requirements.txt      │  │
│    │                                                                                 │  │
│    │   ~10GB                     ~2GB                      ~500MB                    │  │
│    │                                                                                 │  │
│    └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│    │                           EXPERIMENT TRACKING                                   │  │
│    ├─────────────────────────────────────────────────────────────────────────────────┤  │
│    │                                                                                 │  │
│    │   Run: train-2024-01-15-abc123                                                 │  │
│    │   ├── Params: lr=0.001, epochs=100, batch_size=256                            │  │
│    │   ├── Metrics: rmse=0.12, mae=0.08, f1=0.89                                   │  │
│    │   ├── Input: flood-features:v8                                                 │  │
│    │   └── Output: flood-model:v3                                                   │  │
│    │                                                                                 │  │
│    └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                  │                                                │
                  │                                                │
                  ▼                                                ▼
┌───────────────────────┐    ┌─────────────────────────────────────────────────────────────┐
│   LOCAL DEVELOPMENT   │    │                 TRAINING (W&B Sweeps + Your Compute)        │
│                       │    │                                                             │
├───────────────────────┤    ├─────────────────────────────────────────────────────────────┤
│                       │    │                                                             │
│  Dagster              │    │   W&B Sweep Controller              Your Compute            │
│  ├── Asset DAGs       │    │   (in W&B Cloud)                    (runs the actual work) │
│  ├── Testing          │    │   ┌──────────────────┐              ┌──────────────────┐   │
│  └── Debugging        │    │   │                  │  "try lr=.01"│                  │   │
│                       │    │   │  Sweep Config:   │─────────────▶│  Agent 1         │   │
│  DuckDB               │    │   │  - lr: [.001,.1] │              │  (your laptop)   │   │
│  └── Local queries    │    │   │  - epochs: [50]  │◀─────────────│                  │   │
│                       │    │   │  - batch: [64]   │  logs metrics└──────────────────┘   │
│  Polars               │    │   │                  │                                      │
│  └── Data exploration │    │   │  Decides next    │              ┌──────────────────┐   │
│                       │    │   │  hyperparams     │─────────────▶│  Agent 2         │   │
│  Team members work    │    │   │  using Bayesian  │              │  (univ cluster)  │   │
│  independently here   │    │   │  optimization    │◀─────────────│  (GPU node)      │   │
│                       │    │   │                  │  logs metrics└──────────────────┘   │
│                       │    │   └──────────────────┘                                      │
│                       │    │                                      ┌──────────────────┐   │
│                       │    │   Best run selected ───────────────▶│  Agent 3         │   │
│                       │    │   Model logged to W&B Artifacts     │  (Google Colab)  │   │
│                       │    │                                      └──────────────────┘   │
│                       │    │                                                             │
└───────────────────────┘    └─────────────────────────────────────────────────────────────┘
                                                          │
                                                          │ Best model promoted
                                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              INFERENCE API                                              │
│                              HuggingFace Spaces (always-on, free)                       │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│    │                          FastAPI / Gradio                                       │  │
│    │                                                                                 │  │
│    │    POST /predict                                                                │  │
│    │    │                                                                            │  │
│    │    ├── 1. Load model from W&B Artifacts (flood-model:latest)                   │  │
│    │    ├── 2. Load latest features from W&B Artifacts                              │  │
│    │    ├── 3. Run inference (PyTorch)                                              │  │
│    │    └── 4. Return flood risk prediction                                         │  │
│    │                                                                                 │  │
│    │    Data freshness: 15-30 minutes (matches pipeline schedule)                   │  │
│    │                                                                                 │  │
│    └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                                                         │
                                                                         │
                                                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              VISUALIZATION LAYER                                        │
│                              Streamlit Cloud (free)                                     │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│    │                                                                                 │  │
│    │   ┌─────────────┐   ┌─────────────────────────────────┐   ┌─────────────────┐  │  │
│    │   │             │   │                                 │   │                 │  │  │
│    │   │  Regional   │   │        Flood Risk Map           │   │   Historical    │  │  │
│    │   │  Selector   │   │        (Interactive)            │   │   Trends        │  │  │
│    │   │             │   │                                 │   │                 │  │  │
│    │   └─────────────┘   └─────────────────────────────────┘   └─────────────────┘  │  │
│    │                                                                                 │  │
│    │   ┌─────────────────────────────────────────┐   ┌───────────────────────────┐  │  │
│    │   │                                         │   │                           │  │  │
│    │   │      Current Predictions Table          │   │    Model Confidence       │  │  │
│    │   │      (refreshes every 15-30 min)        │   │    Metrics                │  │  │
│    │   │                                         │   │                           │  │  │
│    │   └─────────────────────────────────────────┘   └───────────────────────────┘  │  │
│    │                                                                                 │  │
│    └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│    Calls inference API on user interaction or auto-refresh                             │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SUMMARY                                              │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   Component              Service                    Cost        Role                    │
│   ─────────────────────────────────────────────────────────────────────────────────────│
│   Pipeline Scheduling    GitHub Actions             Free        Cron trigger (15-30m)  │
│   Pipeline Logic         Dagster                    Free        DAG, retries, assets    │
│   Storage & Tracking     W&B Academic               Free        Datasets, experiments   │
│   Training Orchestration W&B Sweeps                 Free        Hyperparameter tuning   │
│   Training Compute       Local / Cluster / Colab    Free        Runs training agents    │
│   Inference API          HuggingFace Spaces         Free        Serves predictions      │
│   Visualization          Streamlit Cloud            Free        User-facing dashboard   │
│   Local Dev              DuckDB + Polars            Free        Testing & debugging     │
│                                                                                         │
│   Total Monthly Cost: $0                                                               │
│   Data Freshness: 15-30 minutes                                                        │
│   Services to Manage: 4 (GitHub, W&B, HuggingFace, Streamlit)                          │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Summary

```
1. TRIGGER      GitHub Actions cron fires every 15-30 min
                                │
                                ▼
2. EXECUTE      Dagster job runs in GitHub Actions runner
                (provides DAG, retries, asset tracking)
                                │
                                ▼
3. INGEST       Extract assets fetch from APIs (USGS, NOAA, etc.)
                                │
                                ▼
4. TRANSFORM    dbt models clean and structure data (staging → marts)
                                │
                                ▼
5. ENGINEER     Polars builds ML features (windowing, lags, aggregations)
                                │
                                ▼
6. STORE        W&B Artifacts stores versioned datasets
                                │
                                ▼
7. ORCHESTRATE  W&B Sweeps defines hyperparameter search space
                                │
                                ▼
8. TRAIN        Agents on your compute (laptop/cluster/Colab) run training
                Each agent pulls features from W&B, logs metrics back
                                │
                                ▼
9. SELECT       W&B picks best model based on metrics, logs to Artifacts
                                │
                                ▼
10. DEPLOY      Best model artifact deployed to HuggingFace Spaces
                                │
                                ▼
11. SERVE       Inference API serves predictions on demand
                                │
                                ▼
12. VISUALIZE   Streamlit dashboard displays flood risk to users
```
