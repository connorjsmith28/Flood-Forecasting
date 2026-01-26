import duckdb
import wandb
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Config
DB_PATH = "flood_forecasting.duckdb"
TARGET = "streamflow_cfs_mean"
FEATURES = ["precipitation_mm", "temperature_c", "wind_speed_ms", "humidity_pct"]

# Init wandb first (sweep injects params here)
wandb.init(
    project="flood-forecasting",
    config={
        "data_description": "Initial baseline model on fct_streamflow_hourly",
        "data_columns": FEATURES + [TARGET],
        "model": "RandomForest",
        "n_estimators": 100,
        "max_depth": None,
        "min_samples_split": 2,
    },
)
config = wandb.config

# Load data
con = duckdb.connect(DB_PATH, read_only=True)
df = con.execute(f"""
    SELECT {TARGET}, {", ".join(FEATURES)}
    FROM main_marts.fct_streamflow_hourly
    WHERE {TARGET} IS NOT NULL
      AND precipitation_mm IS NOT NULL
""").df()
con.close()

X = df[FEATURES]
y = df[TARGET]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train (params from config, overridden by sweep if running)
model = RandomForestRegressor(
    n_estimators=config.n_estimators,
    max_depth=config.max_depth,
    min_samples_split=config.min_samples_split,
    random_state=42,
)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Log metrics
wandb.log(
    {
        "mae": mean_absolute_error(y_test, y_pred),
        "r2": r2_score(y_test, y_pred),
    }
)
wandb.finish()
