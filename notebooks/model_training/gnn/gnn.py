"""
GNN Flood Forecasting — CLI Training Script (wandb integrated)

Run a single experiment:
    just experiment gnn

Run a hyperparameter sweep (20 trials):
    just sweep gnn 20
"""

import time
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
        "epochs": 20,
        "batch_size": 32,
        "stride": 6,
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
# Uses union alignment: each site is padded with NaN for timestamps it lacks.
# build_graph_sequences already drops windows containing any NaN.

def align_sites_by_date(
    X: pl.DataFrame, y: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    target_col = y.columns[0]
    combined = X.with_columns(y[target_col])
    sites = sorted(combined["site_id"].unique().to_list())
    all_dates = combined["observation_hour"].unique().sort()
    n_dates = len(all_dates)
    fill_cols = [c for c in combined.columns if c not in ("site_id", "observation_hour")]
    print(f"  {n_dates} total timestamps across {len(sites)} sites (union, forward-filled)")

    padded_parts = []
    for site in sites:
        spine = pl.DataFrame({
            "site_id": pl.Series([site] * n_dates),
            "observation_hour": all_dates,
        })
        site_data = combined.filter(pl.col("site_id") == site)
        aligned = spine.join(site_data, on=["site_id", "observation_hour"], how="left")
        # Fill gaps within the site's time series; backward_fill covers any leading NaN.
        aligned = aligned.with_columns([
            pl.col(c).forward_fill().backward_fill() for c in fill_cols
        ])
        padded_parts.append(aligned)

    result = pl.concat(padded_parts).sort(["observation_hour", "site_id"])
    return result.drop(target_col), result.select(target_col)


print("Aligning train...")
train_X, train_y = align_sites_by_date(train_X, train_y)
print("Aligning val...")
val_X, val_y = align_sites_by_date(val_X, val_y)
print("Aligning test...")
test_X, test_y = align_sites_by_date(test_X, test_y)

# ── Build graph edges (KNN proximity fallback) ───────────────────────────────
# Use only sites present in the processed splits (pcr.df may have more sites
# than survived lag/null removal during preprocessing).

actual_sites = (
    set(train_X["site_id"].unique())
    | set(val_X["site_id"].unique())
    | set(test_X["site_id"].unique())
)
site_meta = (
    pcr.df
    .filter(pl.col("site_id").is_in(actual_sites))
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
# Materialising all windows at once (~8 GB for train) causes OOM.
# Instead, keep compact [num_nodes, T, features] stacked tensors and slice
# windows lazily in a Dataset.__getitem__.

def build_stacked(
    X: pl.DataFrame,
    y: pl.DataFrame,
    site_to_idx: dict,
    drop_cols: list[str] | None = None,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Stack per-site data → [num_nodes, T, features] and [num_nodes, T]."""
    if drop_cols is None:
        drop_cols = ["site_id", "observation_hour"]
    target_col = y.columns[0]
    sites_ordered = sorted(site_to_idx, key=site_to_idx.get)

    combined = X.with_columns(y[target_col])
    X_list, y_list = [], []
    for site in sites_ordered:
        site_df = combined.filter(pl.col("site_id") == site).sort("observation_hour")
        X_list.append(torch.tensor(
            site_df.drop(drop_cols + [target_col]).to_numpy(), dtype=torch.float32
        ))
        y_list.append(torch.tensor(
            site_df[target_col].to_numpy(), dtype=torch.float32
        ))

    return torch.stack(X_list, dim=0), torch.stack(y_list, dim=0)


class GraphWindowDataset(torch.utils.data.Dataset):
    """Lazy sliding-window dataset over [num_nodes, T, features] tensors."""

    def __init__(self, X_stacked: torch.Tensor, y_stacked: torch.Tensor, window_size: int, stride: int = 1):
        # Compute valid window start indices without allocating all windows.
        valid_t = ~X_stacked.isnan().any(dim=(0, 2))   # [T]
        valid_y = ~y_stacked.isnan().any(dim=0)         # [T]
        window_valid = valid_t.float().unfold(0, window_size, 1).bool().all(dim=1)[:-1]
        target_valid = valid_y[window_size:]
        keep = window_valid & target_valid

        self.valid_indices = torch.where(keep)[0][::stride]
        self.X_stacked = X_stacked   # [nodes, T, features]
        self.y_stacked = y_stacked   # [nodes, T]
        self.window_size = window_size
        print(f"  Dropped {(~keep).sum().item()} NaN windows, kept {keep.sum().item()} (stride={stride} → {len(self.valid_indices)} used)")

    def __len__(self) -> int:
        return len(self.valid_indices)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        t = self.valid_indices[idx].item()
        x = self.X_stacked[:, t : t + self.window_size, :].permute(1, 0, 2)  # [window, nodes, feat]
        y = self.y_stacked[:, t + self.window_size]                            # [nodes]
        return x, y


WINDOW_SIZE = cfg.window_size
BATCH = cfg.batch_size

print("Building train dataset...")
X_train_s, y_train_s = build_stacked(train_X, train_y, site_to_idx)
print("Building val dataset...")
X_val_s, y_val_s = build_stacked(val_X, val_y, site_to_idx)
print("Building test dataset...")
X_test_s, y_test_s = build_stacked(test_X, test_y, site_to_idx)

print(f"\nStacked train: {X_train_s.shape}  target: {y_train_s.shape}")

train_dataset = GraphWindowDataset(X_train_s, y_train_s, WINDOW_SIZE, stride=cfg.stride)
val_dataset   = GraphWindowDataset(X_val_s,   y_val_s,   WINDOW_SIZE, stride=cfg.stride)
test_dataset  = GraphWindowDataset(X_test_s,  y_test_s,  WINDOW_SIZE, stride=cfg.stride)

train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=BATCH, shuffle=True)
val_loader   = torch.utils.data.DataLoader(val_dataset,   batch_size=BATCH, shuffle=False)
test_loader  = torch.utils.data.DataLoader(test_dataset,  batch_size=BATCH, shuffle=False)

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
        batch_size, window_size, num_nodes, in_features = x.shape
        BT = batch_size * window_size

        # Build one block-diagonal batched graph for all (batch × timestep) copies.
        # Each copy i gets edge_index + i * num_nodes, so nodes don't overlap.
        offsets = torch.arange(BT, device=edge_index.device) * num_nodes   # [BT]
        edge_index_bt = (
            edge_index.repeat(1, BT)                                          # [2, BT*E]
            + offsets.repeat_interleave(edge_index.shape[1]).unsqueeze(0)     # [1, BT*E]
        )

        # Single GCN pass over all BT * num_nodes virtual nodes.
        h = x.reshape(BT * num_nodes, in_features)
        for gcn in self.gcn_layers:
            h = self.relu(gcn(h, edge_index_bt))
            h = self.dropout(h)

        # h: [BT * num_nodes, gcn_hidden] → [batch, nodes, window, gcn_hidden]
        gcn_out = h.reshape(batch_size, window_size, num_nodes, -1).permute(0, 2, 1, 3)

        # GRU over time for each node: [batch * nodes, window, gcn_hidden]
        gcn_flat = gcn_out.reshape(batch_size * num_nodes, window_size, -1)
        _, h_n = self.gru(gcn_flat)
        h_n = h_n.squeeze(0)   # [batch * nodes, gru_hidden]

        out = self.head(h_n)
        return out.reshape(batch_size, num_nodes)


# ── Instantiate model ────────────────────────────────────────────────────────

in_features = X_train_s.shape[-1]
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

best_val_loss = float("inf")
patience_counter = 0
best_weights = None

n_batches = len(train_loader)
LOG_EVERY = max(1, n_batches // 10)  # print ~10 updates per epoch

for epoch in range(cfg.epochs):
    # Train
    model.train()
    epoch_loss = 0.0
    n_train = 0
    epoch_start = time.time()

    for batch_idx, (xb, yb) in enumerate(train_loader):
        xb, yb = xb.to(DEVICE), yb.to(DEVICE)
        optimizer.zero_grad()
        pred = model(xb, edge_index_dev)
        loss = asymmetric_mse(pred, yb)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item() * len(xb)
        n_train += len(xb)

        if batch_idx == 0 and epoch == 0:
            batch_secs = time.time() - epoch_start
            est_epoch = batch_secs * n_batches
            est_total = est_epoch * cfg.epochs
            print(f"  1 batch in {batch_secs:.1f}s → ~{est_epoch/60:.1f} min/epoch"
                  f" → ~{est_total/3600:.1f} hr total ({cfg.epochs} epochs)")

        if (batch_idx + 1) % LOG_EVERY == 0 or (batch_idx + 1) == n_batches:
            print(f"  Epoch {epoch+1}/{cfg.epochs} "
                  f"batch {batch_idx+1}/{n_batches} "
                  f"loss={epoch_loss/n_train:.4f}", flush=True)

    train_loss = epoch_loss / n_train
    epoch_secs = time.time() - epoch_start

    # Validate
    model.eval()
    val_loss_sum = 0.0
    n_val = 0
    with torch.no_grad():
        for xb, yb in val_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            val_loss_sum += asymmetric_mse(model(xb, edge_index_dev), yb).item() * len(xb)
            n_val += len(xb)
    val_loss = val_loss_sum / n_val

    wandb.log({
        "epoch": epoch + 1,
        "train_loss": train_loss,
        "val_loss": val_loss,
        "epoch_secs": epoch_secs,
    })

    print(f"Epoch {epoch+1:3d}/{cfg.epochs} | train={train_loss:.4f} | val={val_loss:.4f}"
          f" | {epoch_secs:.0f}s")

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
all_preds, all_actuals = [], []
with torch.no_grad():
    for xb, yb in test_loader:
        all_preds.append(model(xb.to(DEVICE), edge_index_dev).cpu())
        all_actuals.append(yb)

pred_test_scaled = torch.cat(all_preds, dim=0)
y_test_tensor    = torch.cat(all_actuals, dim=0)

pred_flat   = pred_test_scaled.reshape(-1, 1).to(torch.float64)
actual_flat = y_test_tensor.reshape(-1, 1).to(torch.float64)

pred_real   = pcr.target_scaler.inverse_transform(pred_flat).numpy().reshape(pred_test_scaled.shape)
actual_real = pcr.target_scaler.inverse_transform(actual_flat).numpy().reshape(y_test_tensor.shape)

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
