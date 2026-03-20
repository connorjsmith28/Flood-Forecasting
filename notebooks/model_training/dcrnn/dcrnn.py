"""
DCRNN Flood Forecasting — CLI Training Script (wandb integrated)

Diffusion Convolutional Recurrent Neural Network:
  - Diffusion graph convolution replaces linear transforms inside each GRU gate,
    coupling spatial message-passing and temporal dynamics at every timestep.
  - Supports undirected KNN graphs (default) and directed graphs (set directed=True
    when NHDPlus river topology is available).

Run a single experiment:
    just experiment dcrnn

Run a hyperparameter sweep:
    just sweep dcrnn 20
"""

import time
import torch
import torch.nn as nn
import numpy as np
import polars as pl

import wandb
from src.preprocessing.preprocessing import processor
from src.models.graph_utils import (
    align_sites_by_date,
    asymmetric_mse,
    build_knn_edges,
    build_stacked,
    compute_diffusion_supports,
    GraphWindowDataset,
)

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# ── Feature definitions (same as GNN for comparability) ──────────────────────

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

# ── wandb init (sweep overrides these defaults) ───────────────────────────────

wandb.init(
    project="flood-forecasting",
    group="dcrnn",
    tags=["dcrnn", "diffusion-gru"],
    config={
        "model": "DCRNN",
        "data_description": "Top-30 flood-severity sites, hourly, 14 dynamic + 14 static features",
        "window_size": 48,
        "rnn_hidden": 32,
        "n_rnn_layers": 1,
        "K": 2,
        "directed": False,
        "dropout": 0.4,
        "k_neighbors": 3,
        "lr": 5e-4,
        "weight_decay": 1e-3,
        "epochs": 20,
        "batch_size": 32,
        "stride": 6,
        "patience": 8,
        "under_predict_penalty": 2.0,
    },
)
cfg = wandb.config

# ── Data loading & preprocessing ──────────────────────────────────────────────

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

# ── Align sites so every split has identical timestamps across all nodes ───────

print("Aligning train...")
train_X, train_y = align_sites_by_date(train_X, train_y)
print("Aligning val...")
val_X, val_y = align_sites_by_date(val_X, val_y)
print("Aligning test...")
test_X, test_y = align_sites_by_date(test_X, test_y)

# ── Build graph ───────────────────────────────────────────────────────────────
# Uses KNN proximity by default. Replace with NHDPlus river network topology
# and set directed=True for physically-grounded message passing.

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

edge_index = build_knn_edges(coords, k=cfg.k_neighbors)
print(f"Edges: {edge_index.shape[1]} (undirected, k={cfg.k_neighbors})")

supports = [
    S.to(DEVICE)
    for S in compute_diffusion_supports(
        edge_index, num_nodes, K=cfg.K, directed=cfg.directed
    )
]
print(f"Diffusion supports: {len(supports)} (K={cfg.K}, directed={cfg.directed})")

# ── Build temporal graph sequences ────────────────────────────────────────────

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

# ── Model definition ──────────────────────────────────────────────────────────

class DiffConv(nn.Module):
    """Project node features through K-hop diffusion supports then linearly mix.

    For each support matrix S_k (a [nodes, nodes] random-walk transition), computes
    S_k @ x and concatenates all results before applying a shared linear layer.
    This is the graph-domain equivalent of a 1D convolution over hops.
    """

    def __init__(self, in_features: int, out_features: int, n_supports: int):
        super().__init__()
        self.linear = nn.Linear(in_features * n_supports, out_features, bias=False)

    def forward(self, x: torch.Tensor, supports: list[torch.Tensor]) -> torch.Tensor:
        # x: [batch, nodes, in_features]
        # supports[k]: [nodes, nodes]
        parts = [torch.einsum("ij,bjf->bif", S, x) for S in supports]
        return self.linear(torch.cat(parts, dim=-1))  # [batch, nodes, out_features]


class DCGRUCell(nn.Module):
    """GRU cell where all linear transforms are replaced by diffusion convolutions.

    This couples spatial message-passing directly into the recurrent dynamics:
    at every timestep, each node's gate values are informed by its graph neighbours.
    """

    def __init__(self, in_features: int, hidden_size: int, n_supports: int):
        super().__init__()
        self.hidden_size = hidden_size
        # Reset and update gates operate on concatenated [x, h]
        self.gate_conv = DiffConv(in_features + hidden_size, 2 * hidden_size, n_supports)
        # Candidate state operates on concatenated [x, r * h]
        self.cand_conv = DiffConv(in_features + hidden_size, hidden_size, n_supports)
        
        # Add normalization
        self.norm = nn.LayerNorm(hidden_size)

    def forward(
        self,
        x: torch.Tensor,
        h: torch.Tensor,
        supports: list[torch.Tensor],
    ) -> torch.Tensor:
        # x: [batch, nodes, in_features]
        # h: [batch, nodes, hidden_size]
        xh = torch.cat([x, h], dim=-1)
        gates = torch.sigmoid(self.gate_conv(xh, supports))   # [B, N, 2*hidden]
        r, u = gates.chunk(2, dim=-1)

        xrh = torch.cat([x, r * h], dim=-1)
        c = torch.tanh(self.cand_conv(xrh, supports))         # [B, N, hidden]

        new_h = u * h + (1.0 - u) * c                         # [B, N, hidden]
        return self.norm(new_h)


class DCRNN(nn.Module):
    """Stacked DCRNN encoder for graph-structured sequence forecasting.

    Runs a sequence of DCGRUCells over the temporal window, with each cell
    performing diffusion-aware message passing at every timestep. The final
    hidden state of the last layer is projected to a scalar prediction per node.

    Args:
        in_features:  Number of input features per node.
        hidden_size:  Hidden state size (same across all layers).
        n_layers:     Number of stacked DCGRUCell layers.
        n_supports:   Number of diffusion support matrices (K+1 or 2*(K+1)).
        dropout:      Dropout applied between layers.
    """

    def __init__(
        self,
        in_features: int,
        hidden_size: int,
        n_layers: int,
        n_supports: int,
        dropout: float = 0.0,
    ):
        super().__init__()
        self.hidden_size = hidden_size
        self.n_layers = n_layers

        self.cells = nn.ModuleList()
        for i in range(n_layers):
            in_dim = in_features if i == 0 else hidden_size
            self.cells.append(DCGRUCell(in_dim, hidden_size, n_supports))

        self.dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden_size, 1)

    def forward(
        self, x: torch.Tensor, supports: list[torch.Tensor]
    ) -> torch.Tensor:
        # x: [batch, window, nodes, features]
        B, T, N, _ = x.shape

        h = [torch.zeros(B, N, self.hidden_size, device=x.device) for _ in self.cells]

        for t in range(T):
            x_t = x[:, t, :, :]           # [B, N, features]
            for i, cell in enumerate(self.cells):
                h[i] = cell(x_t, h[i], supports)
                x_t = self.dropout(h[i])

        out = self.head(x_t)               # [B, N, 1] (x_t is already self.dropout(h[-1]))
        return out.squeeze(-1)             # [B, N]


# ── Instantiate model ─────────────────────────────────────────────────────────

in_features = X_train_s.shape[-1]
n_supports = len(supports)

model = DCRNN(
    in_features=in_features,
    hidden_size=cfg.rnn_hidden,
    n_layers=cfg.n_rnn_layers,
    n_supports=n_supports,
    dropout=cfg.dropout,
).to(DEVICE)

total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
print(f"\n{model}")
print(f"Total trainable parameters: {total_params:,}")
wandb.log({"total_params": total_params})

# ── Training ──────────────────────────────────────────────────────────────────

optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)

best_val_loss = float("inf")
patience_counter = 0
best_weights = None

n_batches = len(train_loader)
LOG_EVERY = max(1, n_batches // 10)

for epoch in range(cfg.epochs):
    model.train()
    epoch_loss = 0.0
    n_train = 0
    epoch_start = time.time()

    for batch_idx, (xb, yb) in enumerate(train_loader):
        xb, yb = xb.to(DEVICE), yb.to(DEVICE)
        optimizer.zero_grad()
        pred = model(xb, supports)
        loss = asymmetric_mse(pred, yb, cfg.under_predict_penalty)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item() * len(xb)
        n_train += len(xb)

        if batch_idx == 0 and epoch == 0:
            batch_secs = time.time() - epoch_start
            est_epoch = batch_secs * n_batches
            est_total = est_epoch * cfg.epochs
            print(
                f"  1 batch in {batch_secs:.1f}s → ~{est_epoch/60:.1f} min/epoch"
                f" → ~{est_total/3600:.1f} hr total ({cfg.epochs} epochs)"
            )

        if (batch_idx + 1) % LOG_EVERY == 0 or (batch_idx + 1) == n_batches:
            print(
                f"  Epoch {epoch+1}/{cfg.epochs} "
                f"batch {batch_idx+1}/{n_batches} "
                f"loss={epoch_loss/n_train:.4f}",
                flush=True,
            )

    train_loss = epoch_loss / n_train
    epoch_secs = time.time() - epoch_start

    model.eval()
    val_loss_sum = 0.0
    n_val = 0
    with torch.no_grad():
        for xb, yb in val_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            val_loss_sum += (
                asymmetric_mse(model(xb, supports), yb, cfg.under_predict_penalty).item()
                * len(xb)
            )
            n_val += len(xb)
    val_loss = val_loss_sum / n_val

    wandb.log({
        "epoch": epoch + 1,
        "train_loss": train_loss,
        "val_loss": val_loss,
        "epoch_secs": epoch_secs,
    })

    print(
        f"Epoch {epoch+1:3d}/{cfg.epochs} | train={train_loss:.4f} | val={val_loss:.4f}"
        f" | {epoch_secs:.0f}s"
    )

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

# ── Test evaluation ───────────────────────────────────────────────────────────

model.eval()
all_preds, all_actuals = [], []
with torch.no_grad():
    for xb, yb in test_loader:
        all_preds.append(model(xb.to(DEVICE), supports).cpu())
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
