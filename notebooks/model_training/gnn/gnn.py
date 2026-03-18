"""
GNN Flood Forecasting — CLI Training Script (wandb integrated)

Run a single experiment:
    just experiment gnn

Run a hyperparameter sweep (20 trials):
    just sweep gnn 20
"""

import torch
import torch.nn as nn
import numpy as np
import polars as pl
from sklearn.neighbors import NearestNeighbors
from torch_geometric.nn import GCNConv

import wandb
from src.preprocessing.preprocessing import processor

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# ── Feature definitions (same as LSTM for comparability) ──────────────────────

STATIC_FEATURES = [
    "longitude", "latitude", "DRAIN_SQKM", "artificial_path_pct",
    "wb5100_ann_mm", "snw_pc_syr", "snow_ice_nlcd06", "barren_nlcd06",
    "mains100_plant", "hga", "hgc", "bulk_density_avg", "elev_max_m", "aspect_deg",
]

DYNAMIC_FEATURES = [
    "streamflow_cfs_mean", "streamflow_cfs_max", "streamflow_cfs_min",
    "gage_height_ft_mean",
    "precipitation_mm",
    "temperature_c",
    "potential_evaporation_mm",
    "specific_humidity_kgkg",
    "shortwave_radiation_wm2",
    "longwave_radiation_wm2",
    "wind_speed_ms",
    "surface_pressure_pa",
    "cape_jkg",
    "convective_precip_fraction",
]

# ── wandb init (sweep overrides these defaults) ──────────────────────────────

wandb.init(
    project="flood-forecasting",
    group="gnn",
    tags=["gnn", "gcn-gru"],
    config={
        "model": "GCN-GRU",
        "data_description": "Top-30 flood-severity sites, hourly, 14 dynamic + 14 static features",
        "window_size": 72,
        "gcn_hidden": 32,
        "gru_hidden": 64,
        "n_gcn_layers": 2,
        "dropout": 0.2,
        "k_neighbors": 3,
        "lr": 1e-3,
        "epochs": 50,
        "batch_size": 32,
        "patience": 8,
        "under_predict_penalty": 2.0,
    },
)
cfg = wandb.config

# ── Data loading & preprocessing ─────────────────────────────────────────────

data_config = {
    "input_cols": DYNAMIC_FEATURES + STATIC_FEATURES,
    "static_cols": STATIC_FEATURES,
    "target": "streamflow_cfs_target_24h",
    "train_split": 0.8,
    "val_split": 0.9,
    "file_path": "flood-dataset-top30",
    "file_name": "flood_model_top30",
    "table": "wandb.flood_model_top30",
    "lag_window": 1,
    "frequency": "hourly",
    "split_time_days": 30,
    "site_scaling": False,
}

pcr = processor(data_config)
pcr.pull_wandb()

train_X, val_X, test_X, train_y, val_y, test_y = pcr.return_outputs()

# ── Align sites so every split has identical timestamps across all nodes ──────

def align_sites_by_date(
    X: pl.DataFrame, y: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    sites = X["site_id"].unique().to_list()
    common_dates = None
    for site in sites:
        dates = set(X.filter(pl.col("site_id") == site)["observation_hour"].to_list())
        common_dates = dates if common_dates is None else common_dates & dates
    mask = X["observation_hour"].is_in(list(common_dates))
    print(f"  {len(common_dates)} common timestamps across {len(sites)} sites")
    return X.filter(mask), y.filter(mask)


print("Aligning train...")
train_X, train_y = align_sites_by_date(train_X, train_y)
print("Aligning val...")
val_X, val_y = align_sites_by_date(val_X, val_y)
print("Aligning test...")
test_X, test_y = align_sites_by_date(test_X, test_y)

# ── Build graph edges (KNN proximity fallback) ───────────────────────────────

site_meta = (
    pcr.df
    .select(["site_id", "latitude", "longitude"])
    .unique("site_id")
    .sort("site_id")
)
sites_ordered = site_meta["site_id"].to_list()
site_to_idx = {s: i for i, s in enumerate(sites_ordered)}
num_nodes = len(sites_ordered)
coords = site_meta.select(["latitude", "longitude"]).to_numpy()

print(f"Nodes: {num_nodes}")


def build_knn_edges(coords: np.ndarray, k: int) -> torch.Tensor:
    """Connect each node to its k nearest geographic neighbors (undirected)."""
    nbrs = NearestNeighbors(n_neighbors=k + 1, metric="haversine").fit(
        np.deg2rad(coords)
    )
    _, indices = nbrs.kneighbors(np.deg2rad(coords))

    src, dst = [], []
    for i, neighbors in enumerate(indices):
        for j in neighbors[1:]:
            src.append(i)
            dst.append(j)
            src.append(j)
            dst.append(i)

    edge_index = torch.tensor([src, dst], dtype=torch.long)
    edge_index = torch.unique(edge_index, dim=1)
    return edge_index


edge_index = build_knn_edges(coords, k=cfg.k_neighbors)
print(f"Edges: {edge_index.shape[1]} (undirected, k={cfg.k_neighbors})")

# ── Build temporal graph sequences ───────────────────────────────────────────

def build_graph_sequences(
    X: pl.DataFrame,
    y: pl.DataFrame,
    site_to_idx: dict,
    window_size: int,
    drop_cols: list[str] | None = None,
) -> tuple[torch.Tensor, torch.Tensor]:
    if drop_cols is None:
        drop_cols = ["site_id", "observation_hour"]

    sites_ordered = sorted(site_to_idx, key=site_to_idx.get)

    site_X, site_y = {}, {}
    for site in sites_ordered:
        mask = X["site_id"] == site
        site_X[site] = torch.tensor(
            X.filter(mask).drop(drop_cols).to_numpy(), dtype=torch.float32
        )
        site_y[site] = torch.tensor(
            y.filter(mask).to_numpy().flatten(), dtype=torch.float32
        )

    X_stacked = torch.stack([site_X[s] for s in sites_ordered], dim=0)
    y_stacked = torch.stack([site_y[s] for s in sites_ordered], dim=0)

    T = X_stacked.shape[1]
    num_windows = T - window_size
    if num_windows <= 0:
        raise ValueError(f"Not enough timesteps ({T}) for window_size={window_size}")

    X_windows = torch.stack(
        [X_stacked[:, t : t + window_size, :] for t in range(num_windows)],
        dim=0,
    ).permute(0, 2, 1, 3)

    y_windows = y_stacked[:, window_size:].T

    nan_x = X_windows.isnan().any(dim=(1, 2, 3))
    nan_y = y_windows.isnan().any(dim=1)
    keep = ~(nan_x | nan_y)
    print(f"  Dropped {(~keep).sum().item()} NaN windows, kept {keep.sum().item()}")
    return X_windows[keep], y_windows[keep]


WINDOW_SIZE = cfg.window_size

print("Building train sequences...")
X_train, y_train = build_graph_sequences(train_X, train_y, site_to_idx, WINDOW_SIZE)
print("Building val sequences...")
X_val, y_val = build_graph_sequences(val_X, val_y, site_to_idx, WINDOW_SIZE)
print("Building test sequences...")
X_test, y_test = build_graph_sequences(test_X, test_y, site_to_idx, WINDOW_SIZE)

print(f"\nX_train: {X_train.shape}  y_train: {y_train.shape}")

# ── Model definition ─────────────────────────────────────────────────────────

class GCNGRU(nn.Module):
    def __init__(
        self,
        in_features: int,
        gcn_hidden: int,
        gru_hidden: int,
        n_gcn_layers: int = 2,
        dropout: float = 0.2,
    ):
        super().__init__()

        gcn_dims = [in_features] + [gcn_hidden] * n_gcn_layers
        self.gcn_layers = nn.ModuleList(
            [GCNConv(gcn_dims[i], gcn_dims[i + 1]) for i in range(n_gcn_layers)]
        )
        self.dropout = nn.Dropout(dropout)
        self.relu = nn.ReLU()

        self.gru = nn.GRU(
            input_size=gcn_hidden,
            hidden_size=gru_hidden,
            batch_first=True,
        )

        self.head = nn.Linear(gru_hidden, 1)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        batch_size, window_size, num_nodes, _ = x.shape

        gcn_out_list = []
        for t in range(window_size):
            x_t = x[:, t, :, :]
            h_t_list = []
            for b in range(batch_size):
                h = x_t[b]
                for gcn in self.gcn_layers:
                    h = self.relu(gcn(h, edge_index))
                    h = self.dropout(h)
                h_t_list.append(h)
            gcn_out_list.append(torch.stack(h_t_list, dim=0))

        gcn_out = torch.stack(gcn_out_list, dim=1)

        gcn_out = gcn_out.permute(0, 2, 1, 3)
        gcn_flat = gcn_out.reshape(batch_size * num_nodes, window_size, -1)

        _, h_n = self.gru(gcn_flat)
        h_n = h_n.squeeze(0)

        out = self.head(h_n)
        out = out.reshape(batch_size, num_nodes)
        return out


# ── Instantiate model ────────────────────────────────────────────────────────

in_features = X_train.shape[-1]
model = GCNGRU(
    in_features=in_features,
    gcn_hidden=cfg.gcn_hidden,
    gru_hidden=cfg.gru_hidden,
    n_gcn_layers=cfg.n_gcn_layers,
    dropout=cfg.dropout,
).to(DEVICE)

total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
print(f"\n{model}")
print(f"Total trainable parameters: {total_params:,}")
wandb.log({"total_params": total_params})

# ── Training ─────────────────────────────────────────────────────────────────

def asymmetric_mse(y_pred: torch.Tensor, y_true: torch.Tensor) -> torch.Tensor:
    error = y_true - y_pred
    weight = torch.where(error > 0, cfg.under_predict_penalty, 1.0)
    return (weight * error ** 2).mean()


optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)
edge_index_dev = edge_index.to(DEVICE)

X_train_dev = X_train.to(DEVICE)
y_train_dev = y_train.to(DEVICE)
X_val_dev = X_val.to(DEVICE)
y_val_dev = y_val.to(DEVICE)

best_val_loss = float("inf")
patience_counter = 0
best_weights = None

BATCH = cfg.batch_size
n_train = X_train_dev.shape[0]

for epoch in range(cfg.epochs):
    # Train
    model.train()
    epoch_loss = 0.0
    perm = torch.randperm(n_train)
    for start in range(0, n_train, BATCH):
        idx = perm[start : start + BATCH]
        xb = X_train_dev[idx]
        yb = y_train_dev[idx]

        optimizer.zero_grad()
        pred = model(xb, edge_index_dev)
        loss = asymmetric_mse(pred, yb)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item() * len(idx)

    train_loss = epoch_loss / n_train

    # Validate
    model.eval()
    with torch.no_grad():
        val_pred = model(X_val_dev, edge_index_dev)
        val_loss = asymmetric_mse(val_pred, y_val_dev).item()

    wandb.log({
        "epoch": epoch + 1,
        "train_loss": train_loss,
        "val_loss": val_loss,
    })

    if (epoch + 1) % 5 == 0:
        print(f"Epoch {epoch+1:3d} | train={train_loss:.4f} | val={val_loss:.4f}")

    # Early stopping
    if val_loss < best_val_loss:
        best_val_loss = val_loss
        best_weights = {k: v.clone() for k, v in model.state_dict().items()}
        patience_counter = 0
    else:
        patience_counter += 1
        if patience_counter >= cfg.patience:
            print(f"Early stop at epoch {epoch+1}")
            break

model.load_state_dict(best_weights)
print(f"\nBest val loss: {best_val_loss:.4f}")
wandb.log({"best_val_loss": best_val_loss})

# ── Test evaluation ──────────────────────────────────────────────────────────

model.eval()
with torch.no_grad():
    pred_test_scaled = model(X_test.to(DEVICE), edge_index_dev).cpu()

pred_flat = pred_test_scaled.reshape(-1, 1).to(torch.float64)
actual_flat = y_test.reshape(-1, 1).to(torch.float64)

pred_real = pcr.target_scaler.inverse_transform(pred_flat).numpy().reshape(pred_test_scaled.shape)
actual_real = pcr.target_scaler.inverse_transform(actual_flat).numpy().reshape(y_test.shape)

overall_mae = float(np.abs(actual_real - pred_real).mean())
wandb.log({"test_mae_cfs": overall_mae})

print(f"\n{'Site':<12} {'MAE (CFS)':>12}")
print("-" * 26)
for i, site in enumerate(sites_ordered):
    site_mae = float(np.abs(actual_real[:, i] - pred_real[:, i]).mean())
    print(f"{site:<12} {site_mae:>10.1f}")
    wandb.log({f"test_mae/{site}": site_mae})

print(f"\nOverall test MAE: {overall_mae:.1f} CFS")

wandb.finish()
