"""Shared graph utilities for GNN-based flood forecasting models."""

import numpy as np
import polars as pl
import torch
import torch.nn as nn
from sklearn.neighbors import NearestNeighbors


# ── Graph construction ────────────────────────────────────────────────────────

def build_knn_edges(coords: np.ndarray, k: int) -> torch.Tensor:
    """Connect each node to its k nearest geographic neighbors (undirected).

    Args:
        coords: [num_nodes, 2] array of (latitude, longitude) in degrees.
        k:      Number of neighbors per node.

    Returns:
        edge_index: [2, num_edges] long tensor (undirected, deduplicated).
    """
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
    return torch.unique(edge_index, dim=1)


def compute_diffusion_supports(
    edge_index: torch.Tensor,
    num_nodes: int,
    K: int,
    directed: bool = False,
) -> list[torch.Tensor]:
    """Compute K-hop random-walk diffusion supports from an edge_index.

    For undirected graphs returns K+1 matrices (identity + K forward hops).
    For directed graphs returns 2*(K+1) matrices (forward + backward).

    Args:
        edge_index: [2, num_edges] long tensor.
        num_nodes:  Total number of nodes.
        K:          Number of diffusion hops.
        directed:   If True, also compute backward (transpose) supports.

    Returns:
        List of [num_nodes, num_nodes] float tensors on CPU.
    """
    A = torch.zeros(num_nodes, num_nodes)
    A[edge_index[0], edge_index[1]] = 1.0

    def _rw_supports(A_mat: torch.Tensor) -> list[torch.Tensor]:
        D = A_mat.sum(dim=1, keepdim=True).clamp(min=1.0)
        P = A_mat / D  # row-normalised transition matrix
        supports = [torch.eye(num_nodes)]
        Pk = P.clone()
        for _ in range(K):
            supports.append(Pk)
            Pk = Pk @ P
        return supports

    fwd = _rw_supports(A)
    if directed:
        return fwd + _rw_supports(A.T)
    return fwd


# ── Data alignment ────────────────────────────────────────────────────────────

def align_sites_by_date(
    X: pl.DataFrame, y: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Align all sites to a shared timestamp spine via union + forward-fill.

    Sites that are missing a timestamp are padded with NaN then forward/backward
    filled. Windows containing any remaining NaN are dropped by GraphWindowDataset.

    Args:
        X: Feature DataFrame with 'site_id' and 'observation_hour' columns.
        y: Single-column target DataFrame aligned row-for-row with X.

    Returns:
        (X_aligned, y_aligned) with identical timestamp coverage across all sites.
    """
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
        aligned = aligned.with_columns([
            pl.col(c).forward_fill().backward_fill() for c in fill_cols
        ])
        padded_parts.append(aligned)

    result = pl.concat(padded_parts).sort(["observation_hour", "site_id"])
    return result.drop(target_col), result.select(target_col)


# ── Tensor stacking ───────────────────────────────────────────────────────────

def build_stacked(
    X: pl.DataFrame,
    y: pl.DataFrame,
    site_to_idx: dict,
    drop_cols: list[str] | None = None,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Stack per-site data into [num_nodes, T, features] and [num_nodes, T] tensors.

    Args:
        X:           Feature DataFrame (includes 'site_id' and 'observation_hour').
        y:           Single-column target DataFrame.
        site_to_idx: Mapping from site ID string to integer index.
        drop_cols:   Columns to exclude from features. Defaults to ['site_id', 'observation_hour'].

    Returns:
        X_stacked: float32 tensor of shape [num_nodes, T, features].
        y_stacked: float32 tensor of shape [num_nodes, T].
    """
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


# ── Dataset ───────────────────────────────────────────────────────────────────

class GraphWindowDataset(torch.utils.data.Dataset):
    """Lazy sliding-window dataset over [num_nodes, T, features] tensors.

    Computes valid window indices once at construction; __getitem__ slices
    on-the-fly to avoid materialising all windows in memory.
    """

    def __init__(
        self,
        X_stacked: torch.Tensor,
        y_stacked: torch.Tensor,
        window_size: int,
        stride: int = 1,
    ):
        valid_t = ~X_stacked.isnan().any(dim=(0, 2))   # [T]
        valid_y = ~y_stacked.isnan().any(dim=0)         # [T]
        window_valid = valid_t.float().unfold(0, window_size, 1).bool().all(dim=1)[:-1]
        target_valid = valid_y[window_size:]
        keep = window_valid & target_valid

        self.valid_indices = torch.where(keep)[0][::stride]
        self.X_stacked = X_stacked   # [nodes, T, features]
        self.y_stacked = y_stacked   # [nodes, T]
        self.window_size = window_size
        print(
            f"  Dropped {(~keep).sum().item()} NaN windows, "
            f"kept {keep.sum().item()} (stride={stride} → {len(self.valid_indices)} used)"
        )

    def __len__(self) -> int:
        return len(self.valid_indices)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        t = self.valid_indices[idx].item()
        x = self.X_stacked[:, t : t + self.window_size, :].permute(1, 0, 2)  # [window, nodes, feat]
        y = self.y_stacked[:, t + self.window_size]                            # [nodes]
        return x, y


# ── Loss ──────────────────────────────────────────────────────────────────────

def asymmetric_mse(
    y_pred: torch.Tensor, y_true: torch.Tensor, under_predict_penalty: float
) -> torch.Tensor:
    """MSE with a higher penalty for under-predictions (missing a flood event).

    Args:
        y_pred:                Model predictions.
        y_true:                Ground-truth targets.
        under_predict_penalty: Multiplier applied when y_true > y_pred.
    """
    error = y_true - y_pred
    weight = torch.where(error > 0, under_predict_penalty, 1.0)
    return (weight * error ** 2).mean()
