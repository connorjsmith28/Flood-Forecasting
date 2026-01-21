# MLOps Guide for Flood Forecasting

## What is MLOps?

MLOps is the practice of taking ML models from notebooks to production. It covers:

1. **Data Management** - Storage, versioning, quality checks
2. **Feature Engineering** - Transforming raw data into model inputs
3. **Experiment Tracking** - Logging hyperparameters, metrics, artifacts
4. **Model Training** - Running training jobs at scale
5. **Model Registry** - Versioning and staging trained models
6. **Deployment** - Serving predictions via API or batch
7. **Monitoring** - Tracking model performance and data drift

---

## Your Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LAYER                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [Raw Data Sources]  ──►  [Data Warehouse]  ──►  [Feature Store/Tables]   │
│   (APIs, CSVs, etc.)       (DuckDB/Postgres)      (Transformed via dbt)    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXPERIMENTATION LAYER                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [Feature Tables]  ──►  [Training Script]  ──►  [Experiment Tracker]      │
│                          (PyTorch/sklearn)       (MLflow/W&B)              │
│                                │                                            │
│                                ▼                                            │
│                          [Model Registry]                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SERVING LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [Model Artifact]  ──►  [Inference Service]  ──►  [Visualization]         │
│   (from registry)        (FastAPI/BentoML)         (Streamlit/Grafana)     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Breakdown & Free Options

### 1. Data Storage (Data Warehouse)

You need somewhere to store your flood data that can be queried.

| Option | Free Tier | Pros | Cons |
|--------|-----------|------|------|
| **DuckDB** | Completely free | Local, fast, SQL, works with Polars | Not cloud-hosted |
| **Supabase (Postgres)** | 500MB, 2 projects | Real Postgres, hosted | Limited storage |
| **Motherduck** | 10GB | DuckDB in cloud, shareable | Newer, less mature |
| **Snowflake** | $400 credits (trial) | Enterprise grade | Credits expire |
| **Databricks Community** | 15GB, limited clusters | Full platform | Clusters time out |
| **Azure Free** | $200 credits (30 days) | Full Azure access | Short trial |

**Recommendation**: Start with **DuckDB locally** or **Motherduck** for cloud. Both work seamlessly with Polars. Snowflake/Databricks trials are good for learning but will expire before your 4-month timeline.

---

### 2. Data Transformation

You mentioned dbt experience. Good news: dbt works with most warehouses.

| Option | Works With | Notes |
|--------|------------|-------|
| **dbt-core** | Postgres, DuckDB, Snowflake, etc. | Free, local execution |
| **dbt Cloud** | Same | Free tier for 1 developer |
| **Polars directly** | Any data source | Python-native, very fast |

**Recommendation**: Use **dbt-duckdb** adapter for transformations that live in SQL, and **Polars** for complex feature engineering in Python. They complement each other well.

```python
# Example: Polars reading from DuckDB
import polars as pl
import duckdb

conn = duckdb.connect("flood_data.db")
df = pl.read_database("SELECT * FROM flood_events", conn)
```

---

### 3. Experiment Tracking

This is where you log hyperparameters, metrics, and model artifacts.

| Option | Free Tier | Pros | Cons |
|--------|-----------|------|------|
| **MLflow** | Completely free (self-hosted) | Industry standard, full-featured | Need to host yourself |
| **Weights & Biases** | Free for individuals | Beautiful UI, easy setup | Cloud-only free tier |
| **Neptune.ai** | Free for individuals | Good UI | Limited runs |
| **DVC** | Free | Git-based versioning | Steeper learning curve |

**Recommendation**: **MLflow** self-hosted (can run locally or on university cluster) or **Weights & Biases** free tier. MLflow integrates with everything and you own your data.

```python
# Example: MLflow tracking
import mlflow

mlflow.set_experiment("flood-prediction-v1")

with mlflow.start_run():
    mlflow.log_param("learning_rate", 0.001)
    mlflow.log_param("epochs", 100)

    # ... train model ...

    mlflow.log_metric("rmse", 0.15)
    mlflow.pytorch.log_model(model, "model")
```

---

### 4. Model Training Infrastructure

Where the actual compute happens.

| Option | Free Tier | Pros | Cons |
|--------|-----------|------|------|
| **Local machine** | Free | Full control | Limited by your hardware |
| **University cluster** | Free (for you) | Likely GPUs available | May have software restrictions |
| **Google Colab** | Free tier with GPU | Easy, no setup | Session timeouts, limited |
| **Kaggle Notebooks** | 30 hrs GPU/week | Free GPUs | Must use their environment |
| **Lightning.ai** | Free tier | PyTorch native, easy scaling | Limited free compute |

**Recommendation**: Develop locally, then move training to **university cluster** or **Google Colab** for larger jobs. Package your training code properly so it runs anywhere.

#### University Cluster Considerations

Most university clusters run:
- **SLURM** - job scheduler (you submit batch jobs)
- **Singularity/Apptainer** - containers (like Docker but for HPC)
- **Environment modules** - pre-installed software

You'll typically:
1. SSH into the cluster
2. Create a conda/virtualenv environment
3. Submit jobs via SLURM scripts

```bash
#!/bin/bash
#SBATCH --job-name=flood-train
#SBATCH --gres=gpu:1
#SBATCH --time=04:00:00

source activate flood-env
python train.py --config config.yaml
```

---

### 5. Model Registry

Where trained models are stored and versioned.

| Option | Notes |
|--------|-------|
| **MLflow Model Registry** | Built into MLflow, free |
| **DVC** | Git-based model versioning |
| **Weights & Biases Artifacts** | Part of W&B free tier |

**Recommendation**: Use **MLflow Model Registry** - it's included with MLflow and handles model staging (dev → staging → production).

---

### 6. Model Deployment / Serving

How predictions get made available.

| Option | Free Tier | Use Case |
|--------|-----------|----------|
| **FastAPI + Uvicorn** | Free (self-hosted) | Custom REST API |
| **BentoML** | Free (self-hosted) | ML-specific serving framework |
| **MLflow Serve** | Free | Quick deployment from registry |
| **HuggingFace Spaces** | Free tier | Hosted demos |
| **Streamlit Cloud** | Free for public apps | Interactive dashboards |
| **Railway/Render** | Free tiers | Hosted containers |

**Recommendation**:
- For API endpoint: **FastAPI** (lightweight, fast, well-documented)
- For visualization: **Streamlit** (Python-native, easy to build dashboards)

```python
# Example: FastAPI inference endpoint
from fastapi import FastAPI
import mlflow

app = FastAPI()
model = mlflow.pytorch.load_model("models:/flood-predictor/Production")

@app.post("/predict")
async def predict(features: dict):
    prediction = model.predict(features)
    return {"flood_risk": prediction}
```

---

### 7. Orchestration

Coordinating the full pipeline (extract → transform → train → deploy).

| Option | Free Tier | Pros | Cons |
|--------|-----------|------|------|
| **Dagster** | Free (self-hosted) | Modern, great for data/ML | Needs 24/7 hosting |
| **Dagster Cloud** | Limited free | Managed scheduling | Very limited free tier |
| **Prefect Cloud** | 3 users free | Hybrid model, generous | Less ML-focused |
| **GitHub Actions** | 2000 min/month | No server needed | Not a full orchestrator |
| **Airflow** | Free (self-hosted) | Industry standard | Complex, needs hosting |

#### The Dagster Hosting Problem

Dagster needs a daemon running continuously to execute schedules. Options:

| Hosting Option | Cost | Notes |
|----------------|------|-------|
| **Dagster Cloud** | Limited free | Only 1 user, limited runs |
| **Railway** | $5 credit/month | Sleeps after inactivity |
| **Render** | Free tier | Sleeps after 15 min |
| **Fly.io** | Free tier | Can keep alive, some limits |
| **University server** | Free | If they allow persistent processes |
| **Old laptop/Raspberry Pi** | ~$0 | Runs at home |

#### Alternative: GitHub Actions for Scheduling

For a 4-month project, **GitHub Actions** might be simpler than hosting Dagster:

```yaml
# .github/workflows/nightly-pipeline.yml
name: Nightly Data Pipeline

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily
  workflow_dispatch:  # Manual trigger

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run extraction
        run: python elt/extraction/fetch_flood_data.py

      - name: Run dbt
        run: |
          cd elt/transformation
          dbt run

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: pipeline-output
          path: data/output/
```

**Pros**: Free (2000 min/month for private repos, unlimited for public), no server to maintain
**Cons**: Not a real orchestrator (no DAG visualization, limited retry logic)

#### Hybrid Approach

Use **Dagster locally for development** (testing pipelines, debugging) and **GitHub Actions for scheduled runs**:

```
Development:     dagster dev → test pipelines locally
Production:      GitHub Actions → runs on schedule, triggers pipeline scripts
```

This gives you the best of both: Dagster's nice dev experience without paying for hosting.

**Recommendation**: For a 4-month student project, **GitHub Actions** for scheduling + **Dagster local** for development. If you get access to an always-on university server, then self-host Dagster there.

---

## Team vs Solo: What Needs Hosting?

For a team project with a production API, most components need to be shared:

| Component | Local OK? | Team Requirement |
|-----------|-----------|------------------|
| Data warehouse | No | Everyone queries same data |
| Experiment tracking | No | Team sees each other's runs |
| Model registry | No | Shared model versions |
| Training compute | Yes | Can use personal machines/cluster |
| Inference API | No | Must be reachable 24/7 |
| Visualization | No | End users access it |

**Bottom line**: Training can be local/on-cluster. Everything else needs cloud hosting for team collaboration and production use.

---

## Recommended Stack (Free Tier - Team)

Given your constraints (team, 4 months, near real-time API, no budget):

```
┌────────────────────────────────────────────────────────────────┐
│                 RECOMMENDED STACK (TEAM)                       │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Data Storage:      Motherduck (10GB free, shared DuckDB)     │
│                     OR Supabase Postgres (500MB free)          │
│                                                                │
│  Transformation:    dbt Cloud (free tier, 1 dev seat)          │
│                     + Polars in Python scripts                 │
│                                                                │
│  Scheduling:        GitHub Actions (cron triggers)             │
│  Pipeline Dev:      Dagster (local development/testing)        │
│                                                                │
│  Experiment Track:  Weights & Biases (free for teams)         │
│                                                                │
│  Training:          Local machines → University Cluster        │
│                                                                │
│  Model Registry:    W&B Artifacts OR HuggingFace Hub (free)   │
│                                                                │
│  Inference API:     Railway (free tier)                        │
│                     OR Render (free, sleeps after 15 min)      │
│                     OR HuggingFace Spaces (free, always on)    │
│                                                                │
│  Visualization:     Streamlit Cloud (free for public apps)     │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Free Tier Reality Check

| Service | Free Tier | Sufficient for 4 months? |
|---------|-----------|--------------------------|
| **Motherduck** | 10GB storage | Yes, with compression |
| **Supabase** | 500MB, 2 projects | Tight, but works for features |
| **dbt Cloud** | 1 developer seat | Need to share login or use dbt-core |
| **W&B** | Unlimited for individuals/academics | Yes |
| **GitHub Actions** | 2000 min/month (private) | Yes |
| **Railway** | $5 credit/month | Yes, for small API |
| **HuggingFace Spaces** | Free CPU instances | Yes, always on |
| **Streamlit Cloud** | Free for public | Yes |

### Hosting the Inference API

For "near real-time" inference, you need an always-on endpoint. Best free options:

#### Option 1: HuggingFace Spaces (Recommended)
- Free, doesn't sleep
- Supports FastAPI via Docker
- Built-in model hosting integration
- Academic/research friendly

```python
# app.py for HuggingFace Spaces
import gradio as gr  # or FastAPI
from huggingface_hub import hf_hub_download

model = load_model(hf_hub_download("your-org/flood-model", "model.pt"))

def predict(features):
    return model.predict(features)

demo = gr.Interface(fn=predict, inputs="text", outputs="text")
demo.launch()
```

#### Option 2: Railway
- $5 free credit/month
- Real containers, full control
- Stays awake if there's traffic

#### Option 3: Render
- Free tier sleeps after 15 min inactivity
- Fine if visualization pings it regularly
- Can use a cron job to keep alive

### Why This Stack?

1. **Team-friendly** - Everyone accesses same data, experiments, models
2. **Actually free** - All services have sufficient free tiers for 4 months
3. **Production-ready** - Real API endpoint, real database, not toys
4. **Polars-friendly** - Motherduck is DuckDB (Arrow-native like Polars)
5. **Portable** - Patterns translate to enterprise tools later

---

## Simplified Stack: W&B Academic (Recommended)

If you can get W&B's academic tier (100GB storage), you can consolidate most of the stack into one platform.

**Apply here**: https://wandb.ai/site/research

W&B is generous with student/research projects. Mention your university affiliation and that this is flood forecasting research (socially relevant).

### What W&B Replaces

| Without W&B Academic | With W&B Academic |
|----------------------|-------------------|
| Motherduck (data storage) | W&B Artifacts |
| Separate model registry | W&B Artifacts |
| MLflow tracking | W&B Experiments |
| S3/R2 for raw data | W&B Artifacts |

### W&B Academic Stack

```
┌────────────────────────────────────────────────────────────────┐
│              SIMPLIFIED STACK (W&B Academic)                   │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  W&B (100GB academic tier):                                   │
│  ├── Raw datasets (Parquet)       ~10GB                       │
│  ├── Feature datasets (versioned) ~2GB                        │
│  ├── Training subsets             ~500MB                      │
│  ├── Model artifacts              ~2GB                        │
│  ├── Experiment tracking          ~100MB                      │
│  └── Remaining headroom           ~85GB                       │
│                                                                │
│  DuckDB (local, each team member):                            │
│  └── Downloaded artifacts for SQL queries                     │
│                                                                │
│  Inference API:     HuggingFace Spaces (free, always-on)      │
│  Visualization:     Streamlit Cloud (free)                    │
│  Scheduling:        GitHub Actions (cron triggers)            │
│  Pipeline Dev:      Dagster (local)                           │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### W&B Artifacts for Datasets

W&B Artifacts handles dataset versioning with automatic lineage tracking:

```python
import wandb

# === UPLOAD A DATASET ===
run = wandb.init(project="flood-forecasting", job_type="data-pipeline")

artifact = wandb.Artifact(
    name="flood-features",
    type="dataset",
    description="Daily aggregated flood features, all regions"
)
artifact.add_dir("data/features/")  # Add entire directory
run.log_artifact(artifact)
run.finish()

# === USE DATASET IN TRAINING ===
run = wandb.init(project="flood-forecasting", job_type="training")

# Pull specific version (or "latest")
artifact = run.use_artifact("flood-features:v3")
data_path = artifact.download()  # Cached locally

# Load with Polars
import polars as pl
df = pl.read_parquet(f"{data_path}/*.parquet")

# Train model...
# W&B automatically tracks: this run used flood-features:v3
```

### Dataset Versioning Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                  DATASET VERSIONING FLOW                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Raw data updated (GitHub Actions schedule)                 │
│                    │                                            │
│                    ▼                                            │
│  2. Transform script runs (dbt or Polars)                      │
│                    │                                            │
│                    ▼                                            │
│  3. Log new artifact version to W&B                            │
│     wandb.Artifact("flood-features") → v4                      │
│                    │                                            │
│                    ▼                                            │
│  4. Training runs reference artifact                            │
│     run.use_artifact("flood-features:v4")                      │
│                    │                                            │
│                    ▼                                            │
│  5. Model logged with automatic lineage                         │
│     W&B knows: model-v7 was trained on flood-features:v4       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Do You Still Need Motherduck?

**Maybe not.** With W&B storing datasets:

| Use Case | Without Motherduck |
|----------|-------------------|
| Team SQL queries | Each person downloads artifact → queries locally with DuckDB |
| Shared dashboards | Streamlit pulls from W&B artifacts |
| Data exploration | W&B Tables for visualization, or local DuckDB |

**Keep Motherduck if**: Your team wants a shared SQL query interface without downloading files locally each time.

**Skip Motherduck if**: Everyone's comfortable pulling artifacts and querying with Polars/DuckDB locally.

### GitHub Actions + W&B Integration

```yaml
# .github/workflows/data-pipeline.yml
name: Daily Data Pipeline

on:
  schedule:
    - cron: '0 2 * * *'

jobs:
  update-data:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run pipeline and log to W&B
        env:
          WANDB_API_KEY: ${{ secrets.WANDB_API_KEY }}
        run: |
          python elt/extraction/fetch_data.py
          python elt/transformation/build_features.py
          python scripts/log_dataset_to_wandb.py
```

### Summary: W&B Academic Simplifies Everything

| Component | Service | Cost |
|-----------|---------|------|
| Dataset storage & versioning | W&B Artifacts | Free (academic) |
| Experiment tracking | W&B Experiments | Free (academic) |
| Model registry | W&B Artifacts | Free (academic) |
| Local queries | DuckDB | Free |
| Scheduling | GitHub Actions | Free |
| Inference API | HuggingFace Spaces | Free |
| Visualization | Streamlit Cloud | Free |

**Total services to manage**: 4 (W&B, GitHub, HuggingFace, Streamlit)
**Total cost**: $0

---

## Real-Time vs Batch Inference

You mentioned "close to real-time" inference. There are two patterns:

### Batch Inference
- Run predictions on a schedule (every X minutes)
- Store predictions in a table
- Visualization tool queries the table

```
[New Data] → [Dagster Job] → [Model Predict] → [Predictions Table] → [Dashboard]
```

**Pros**: Simple, efficient, easy to debug
**Cons**: Not truly real-time

### Online Inference
- API endpoint receives requests
- Model makes prediction on-demand
- Returns result immediately

```
[Request] → [FastAPI] → [Model Predict] → [Response]
```

**Pros**: True real-time
**Cons**: Need to keep service running, handle scaling

**Recommendation for flood forecasting**: Start with **batch inference** (every 5-15 minutes). Flood conditions don't change by the second, and batch is much simpler to implement and debug.

---

## Learning Path

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up DuckDB with sample flood data
- [ ] Create dbt models for feature engineering
- [ ] Build training script that reads from DuckDB via Polars

### Phase 2: Experiment Tracking (Weeks 3-4)
- [ ] Set up MLflow locally
- [ ] Instrument training script with MLflow logging
- [ ] Run experiments, compare in MLflow UI

### Phase 3: Production Training (Weeks 5-6)
- [ ] Package training code properly (requirements.txt, config files)
- [ ] Get access to university cluster
- [ ] Run training job on cluster

### Phase 4: Deployment (Weeks 7-8)
- [ ] Set up MLflow Model Registry
- [ ] Build FastAPI inference endpoint
- [ ] Create Streamlit dashboard

### Phase 5: Orchestration (Weeks 9-10)
- [ ] Create Dagster pipeline for data refresh
- [ ] Add scheduled model inference job
- [ ] Set up monitoring/alerting

---

## Comparison: Cloud Platforms

Since you mentioned Snowflake, AWS, Databricks, and Azure:

| Platform | ML Offering | Free Tier Reality | Best For |
|----------|-------------|-------------------|----------|
| **Snowflake** | Snowpark ML | $400 trial credits | SQL-heavy orgs |
| **AWS** | SageMaker | 2 months, limited | Full AWS shops |
| **Databricks** | MLflow built-in | Community edition limited | Spark workloads |
| **Azure** | Azure ML | $200/30 days | Microsoft shops |

**Reality check**: None of these have free tiers that last 4 months for active development. They're designed to get you hooked, then charge.

**My advice**: Learn the concepts with free tools (MLflow, DuckDB, etc.), then you can easily translate to any platform later. The patterns are the same - only the specific APIs differ.

---

## Handling Large Datasets (40-50GB+)

Your 50GB dataset is manageable but requires some strategy.

### Storage & Compression

```
Raw CSV:     50 GB
Parquet:     ~5-10 GB  (columnar + compression)
DuckDB:      ~8-12 GB  (compressed, indexed)
```

**Action**: Convert to Parquet immediately. Polars and DuckDB both read Parquet natively.

```python
import polars as pl

# Convert CSV to Parquet (one-time)
pl.scan_csv("flood_data.csv").sink_parquet("flood_data.parquet")

# Now queries only read columns they need
df = pl.scan_parquet("flood_data.parquet").select(["timestamp", "water_level", "region"])
```

### Sampling Strategies for Training

You don't need 50GB to train a good model. Consider:

#### 1. Temporal Sampling
```python
# Last 2 years instead of full history
df = pl.scan_parquet("flood_data.parquet").filter(
    pl.col("timestamp") > datetime(2024, 1, 1)
)
```

#### 2. Stratified Sampling (Preserve Rare Events)
```python
# Keep all flood events, sample 10% of normal conditions
flood_events = df.filter(pl.col("flood_occurred") == True)
normal_sample = df.filter(pl.col("flood_occurred") == False).sample(fraction=0.1)
training_data = pl.concat([flood_events, normal_sample])
```

#### 3. Feature Aggregation (Reduce Row Count)
```python
# Aggregate 1-minute readings to hourly
hourly = df.group_by_dynamic("timestamp", every="1h").agg([
    pl.col("water_level").mean().alias("water_level_mean"),
    pl.col("water_level").max().alias("water_level_max"),
    pl.col("water_level").std().alias("water_level_std"),
])
```

This can reduce row count by 60x while preserving signal.

### DuckDB for Large Data

DuckDB excels at querying data larger than RAM:

```python
import duckdb

# Query 50GB directly - DuckDB streams through it
conn = duckdb.connect()
result = conn.execute("""
    SELECT region, date_trunc('day', timestamp) as day,
           AVG(water_level) as avg_level,
           MAX(water_level) as max_level
    FROM read_parquet('flood_data/*.parquet')
    GROUP BY 1, 2
""").pl()  # Returns Polars DataFrame
```

### Recommended Data Tiers

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA TIERS                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Raw Data]           50 GB    Parquet, partitioned by     │
│                                date/region, archived        │
│                                                             │
│  [Feature Tables]     2-5 GB   Pre-aggregated, indexed,    │
│                                used for training            │
│                                                             │
│  [Training Subset]    <1 GB    Sampled, fits in memory,    │
│                                versioned with DVC           │
│                                                             │
│  [Inference Window]   <100 MB  Last N hours, used for      │
│                                real-time predictions        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Partitioning Strategy

Partition your Parquet files for efficient querying:

```
flood_data/
├── region=northeast/
│   ├── year=2023/
│   │   ├── month=01.parquet
│   │   ├── month=02.parquet
│   │   └── ...
│   └── year=2024/
│       └── ...
├── region=southeast/
│   └── ...
```

```python
# Write partitioned
df.write_parquet(
    "flood_data",
    partition_by=["region", "year"],
)

# Read only what you need - Polars/DuckDB skip irrelevant partitions
df = pl.scan_parquet("flood_data/region=northeast/year=2024/*.parquet")
```

### Memory-Efficient Training with PyTorch

For large datasets, use a DataLoader that streams from disk:

```python
import torch
from torch.utils.data import Dataset, DataLoader
import polars as pl

class FloodDataset(Dataset):
    def __init__(self, parquet_path):
        # Only load metadata, not full data
        self.df = pl.scan_parquet(parquet_path)
        self.length = self.df.select(pl.count()).collect().item()

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        # Load single row on demand
        row = self.df.slice(idx, 1).collect()
        features = row.select(pl.exclude("target")).to_numpy()
        target = row.select("target").to_numpy()
        return torch.tensor(features), torch.tensor(target)

# Streams from disk, never loads full dataset
loader = DataLoader(FloodDataset("features.parquet"), batch_size=256)
```

---

## Next Steps

1. **Explore your university cluster** - Email IT/research computing to learn what's available
2. **Set up DuckDB** - Start loading your flood data
3. **Install MLflow** - Run `pip install mlflow` and start the UI with `mlflow ui`
4. **Convert a notebook to a script** - Take your existing model code and make it a proper Python module

Would you like me to create starter code for any of these components?
