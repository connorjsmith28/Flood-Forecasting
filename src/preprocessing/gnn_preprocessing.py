# =============================================================================
# gnn_preprocessing.py — GNN Preprocessing for Flood Forecasting
# =============================================================================
#
# Extends preprocessing.py to support all 49 gauge sites (37 primary + 12
# upstream-only) and builds the graph structure required by PyTorch Geometric.
#
# EDGE ATTRIBUTES (per upstream→downstream pair)
# -----------------------------------------------
#   lag_hours   : flood wave travel time between the gauge pair (hours)
#   scale_m     : linear scale factor (primary ≈ m * upstream + b)
#   intercept_b : baseflow / tributary contribution between gauges (cfs)
#   r_squared   : goodness-of-fit of the linear relationship [0, 1]
#
# These are computed once from the raw 15-min parquet using
# analyze_gauge_relationship() and stored as edge_attr [num_edges, 4].
# Edges for null upstream pairs are simply omitted — those nodes have
# no incoming edges and rely entirely on the LSTM branch.
#
# GRAPH CONVENTIONS
# -----------------
#   Edges are directed UPSTREAM → DOWNSTREAM.
#   edge_index[0] = source (upstream) node index
#   edge_index[1] = target (downstream) node index
#   edge_attr[i]  = [lag_hours, scale_m, intercept_b, r_squared] for edge i
#
#   primary_mask[i] = True  →  node i is a prediction target (37 nodes)
#   primary_mask[i] = False →  node i is an input-only upstream node (12 nodes)
# =============================================================================

import numpy as np
import polars as pl
import torch
from torch_geometric.data import Data
from scipy.signal import correlate
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression

from src.preprocessing.preprocessing import processor, TorchStandardScaler


# ---------------------------------------------------------------------------
# Constants — import NUM_EDGE_FEATURES in gnn_model.py
# ---------------------------------------------------------------------------

EDGE_ATTR_COLS    = ["lag_hours", "scale_m", "intercept_b", "r_squared"]
NUM_EDGE_FEATURES = len(EDGE_ATTR_COLS)   # 4


# ---------------------------------------------------------------------------
# Edge attribute computation
# ---------------------------------------------------------------------------

def analyze_gauge_relationship(
    parquet_path: str,
    primary_site_id: str,
    upstream_site_id: str,
    max_lag_hours: float = 72.0,
    timestep_minutes: int = 15,
    plot: bool = False,
) -> dict:
    """
    Compute physical edge attributes for one upstream→downstream gauge pair.
    Identical logic to top_37_upstream_pair.ipynb; plot=False by default
    since we call this in a loop during graph construction.

    Returns
    -------
    dict with keys: lag_hours, scale_m, intercept_b, r_squared,
                    lag_steps, pearson_r, n_primary, n_upstream, n_overlap
    """
    timesteps_per_hour = 60 // timestep_minutes
    max_lag_steps = int(max_lag_hours * timesteps_per_hour)

    df = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("site_id").is_in([primary_site_id, upstream_site_id]))
        .select(["site_id", "datetime", "streamflow_cfs"])
        .collect()
    )

    df_pivot = (
        df
        .pivot(index="datetime", on="site_id", values="streamflow_cfs")
        .sort("datetime")
        .rename({primary_site_id: "primary", upstream_site_id: "upstream"})
    )

    SENTINEL = -999_999
    df_pivot = df_pivot.with_columns([
        pl.when(pl.col("primary")  == SENTINEL).then(None).otherwise(pl.col("primary") ).alias("primary"),
        pl.when(pl.col("upstream") == SENTINEL).then(None).otherwise(pl.col("upstream")).alias("upstream"),
    ])

    df_overlap = df_pivot.filter(
        pl.col("primary").is_not_null() & pl.col("upstream").is_not_null()
    )

    n_primary  = df_pivot["primary"].drop_nulls().len()
    n_upstream = df_pivot["upstream"].drop_nulls().len()
    n_overlap  = df_overlap.height

    if n_overlap < 2:
        raise ValueError(
            f"Not enough overlapping observations for pair "
            f"{primary_site_id} ← {upstream_site_id} (n={n_overlap})."
        )

    primary_vals  = df_overlap["primary"].to_numpy()
    upstream_vals = df_overlap["upstream"].to_numpy()

    # Cross-correlation to find lag
    p_centered = primary_vals  - primary_vals.mean()
    u_centered = upstream_vals - upstream_vals.mean()
    full_corr  = correlate(p_centered, u_centered, mode="full")
    lags       = np.arange(-len(u_centered) + 1, len(p_centered))

    valid_mask     = (lags >= 0) & (lags <= max_lag_steps)
    valid_lags     = lags[valid_mask]
    valid_corr     = full_corr[valid_mask]
    best_lag_steps = int(valid_lags[np.argmax(valid_corr)])
    best_lag_hours = best_lag_steps / timesteps_per_hour

    # Align series by lag then fit linear model
    if best_lag_steps > 0:
        upstream_shifted = upstream_vals[:-best_lag_steps]
        primary_aligned  = primary_vals[best_lag_steps:]
    else:
        upstream_shifted = upstream_vals
        primary_aligned  = primary_vals

    reg         = LinearRegression().fit(upstream_shifted.reshape(-1, 1), primary_aligned)
    scale_m     = float(reg.coef_[0])
    intercept_b = float(reg.intercept_)
    r_squared   = float(reg.score(upstream_shifted.reshape(-1, 1), primary_aligned))
    pearson_r, _ = pearsonr(upstream_shifted, primary_aligned)

    return {
        "lag_hours":   best_lag_hours,
        "lag_steps":   best_lag_steps,
        "scale_m":     scale_m,
        "intercept_b": intercept_b,
        "r_squared":   r_squared,
        "pearson_r":   pearson_r,
        "n_primary":   n_primary,
        "n_upstream":  n_upstream,
        "n_overlap":   n_overlap,
    }


def build_edge_attributes(
    upstream_pair_dict: dict,
    parquet_path: str,
    max_lag_hours: float = 72.0,
    timestep_minutes: int = 15,
) -> dict[tuple[str, str], dict]:
    """
    Run analyze_gauge_relationship for every valid pair and return a dict
    keyed by (primary_id, upstream_id) → results dict.

    Parameters
    ----------
    upstream_pair_dict : {primary_site_id: upstream_site_id | None}
    parquet_path       : glob path to raw 15-min parquet files
    max_lag_hours      : passed to analyze_gauge_relationship
    timestep_minutes   : passed to analyze_gauge_relationship

    Returns
    -------
    edge_results : {(primary_id, upstream_id): results_dict}
    """
    edge_results = {}
    valid_pairs = [(p, u) for p, u in upstream_pair_dict.items() if u is not None]
    print(f"Computing edge attributes for {len(valid_pairs)} upstream pairs...\n")
    print(f"{'Upstream':<12} → {'Primary':<12}  {'lag_h':>6}  {'R²':>6}")
    print("-" * 48)

    for primary_id, upstream_id in valid_pairs:
        try:
            results = analyze_gauge_relationship(
                parquet_path     = parquet_path,
                primary_site_id  = primary_id,
                upstream_site_id = upstream_id,
                max_lag_hours    = max_lag_hours,
                timestep_minutes = timestep_minutes,
                plot             = False,
            )
            edge_results[(primary_id, upstream_id)] = results
            print(f"{upstream_id:<12} → {primary_id:<12}  "
                  f"{results['lag_hours']:>6.2f}  {results['r_squared']:>6.4f}")
        except ValueError as e:
            print(f"{upstream_id:<12} → {primary_id:<12}  SKIPPED — {e}")

    print(f"\nEdge attributes ready for {len(edge_results)}/{len(valid_pairs)} pairs.")
    return edge_results


# ---------------------------------------------------------------------------
# Helper: derive full 49-site list from upstream_pair_dict
# ---------------------------------------------------------------------------

def get_all_sites(upstream_pair_dict: dict) -> tuple[list[str], list[str], list[str]]:
    """
    Derive the complete site list from the upstream_pair_dict.

    Returns
    -------
    all_sites      : sorted list of all unique site IDs (primary + upstream-only)
    primary_sites  : the 37 primary site IDs (keys of the dict)
    upstream_only  : site IDs that appear only as upstream values (12 sites)
    """
    primary_sites   = list(upstream_pair_dict.keys())
    upstream_values = {v for v in upstream_pair_dict.values() if v is not None}
    upstream_only   = sorted(upstream_values - set(primary_sites))
    all_sites       = sorted(set(primary_sites) | upstream_values)
    return all_sites, primary_sites, upstream_only


# ---------------------------------------------------------------------------
# Helper: build graph tensors
# ---------------------------------------------------------------------------

def build_graph(
    upstream_pair_dict: dict,
    node_id_to_index: dict[str, int],
    edge_results: dict[tuple[str, str], dict],
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Build edge_index, edge_attr, and primary_mask tensors.

    Parameters
    ----------
    upstream_pair_dict  : {primary_site_id: upstream_site_id | None}
    node_id_to_index    : {site_id: int} for all nodes
    edge_results        : {(primary_id, upstream_id): results_dict}

    Returns
    -------
    edge_index   : LongTensor  [2, num_edges]
    edge_attr    : FloatTensor [num_edges, 4]  (lag_hours, scale_m, intercept_b, r_squared)
    primary_mask : BoolTensor  [num_nodes]
    """
    sources   = []
    targets   = []
    attr_rows = []

    for primary_id, upstream_id in upstream_pair_dict.items():
        if upstream_id is None:
            continue
        key = (primary_id, upstream_id)
        if key not in edge_results:
            print(f"  Warning: no edge attributes for {upstream_id} → {primary_id}, edge omitted.")
            continue

        r = edge_results[key]
        sources.append(node_id_to_index[upstream_id])
        targets.append(node_id_to_index[primary_id])
        attr_rows.append([
            r["lag_hours"],
            r["scale_m"],
            r["intercept_b"],
            r["r_squared"],
        ])

    edge_index = torch.tensor([sources, targets], dtype=torch.long)
    edge_attr  = torch.tensor(attr_rows, dtype=torch.float32)   # [E, 4]

    num_nodes    = len(node_id_to_index)
    primary_ids  = set(upstream_pair_dict.keys())
    primary_mask = torch.zeros(num_nodes, dtype=torch.bool)
    for site_id, idx in node_id_to_index.items():
        if site_id in primary_ids:
            primary_mask[idx] = True

    return edge_index, edge_attr, primary_mask


# ---------------------------------------------------------------------------
# GNN processor
# ---------------------------------------------------------------------------

class gnn_processor(processor):
    """
    Extends processor to load all 49 gauge sites and build a graph with
    physical edge attributes (lag_hours, scale_m, intercept_b, r_squared).

    Typical usage
    -------------
    gnn_pcr = gnn_processor(config)
    gnn_pcr.compute_edge_attributes(parquet_path)   # uses raw 15-min parquet
    gnn_pcr.pull_wandb()                            # loads hourly flood dataset
    graph = gnn_pcr.get_graph()                     # Data(edge_index, edge_attr, primary_mask)
    """

    def __init__(self, config: dict) -> None:
        if "upstream_pair_dict" not in config:
            raise ValueError("config must include 'upstream_pair_dict'.")

        self.upstream_pair_dict = config["upstream_pair_dict"]
        all_sites, primary_sites, upstream_only = get_all_sites(self.upstream_pair_dict)
        self.all_sites      = all_sites
        self.primary_sites  = primary_sites
        self.upstream_only  = upstream_only

        config = {**config, "sites": all_sites}
        super().__init__(config)

        self.node_id_to_index: dict[str, int] = {
            site: idx for idx, site in enumerate(sorted(all_sites))
        }
        self.ordered_sites: list[str] = sorted(all_sites)

        self._edge_results: dict | None = None
        self.graph: Data | None = None

        print(f"[gnn_processor] Total sites   : {len(all_sites)}")
        print(f"[gnn_processor] Primary       : {len(primary_sites)}")
        print(f"[gnn_processor] Upstream-only : {len(upstream_only)}")

    def compute_edge_attributes(
        self,
        parquet_path: str,
        max_lag_hours: float = 72.0,
        timestep_minutes: int = 15,
    ):
        """
        Run analyze_gauge_relationship for every valid upstream pair.
        Uses the raw 15-min parquet — independent of the hourly flood dataset.
        Call this before or after pull_wandb(); graph is built either way.

        Parameters
        ----------
        parquet_path     : glob path to raw-streamflow-15min parquet
        max_lag_hours    : max lag to search (default 72h)
        timestep_minutes : resolution of parquet (default 15 min)
        """
        self._edge_results = build_edge_attributes(
            upstream_pair_dict = self.upstream_pair_dict,
            parquet_path       = parquet_path,
            max_lag_hours      = max_lag_hours,
            timestep_minutes   = timestep_minutes,
        )
        if self.train_X_scaled is not None:
            self._build_graph_object()

    def preprocess(self):
        super().preprocess()
        if self._edge_results is not None:
            self._build_graph_object()
        else:
            print(
                "[gnn_processor] Warning: call compute_edge_attributes(parquet_path) "
                "to attach physical edge features before training."
            )

    def _build_graph_object(self):
        if self._edge_results is None:
            raise RuntimeError("Call compute_edge_attributes() first.")

        edge_index, edge_attr, primary_mask = build_graph(
            upstream_pair_dict = self.upstream_pair_dict,
            node_id_to_index   = self.node_id_to_index,
            edge_results       = self._edge_results,
        )

        self.graph = Data(
            edge_index   = edge_index,
            edge_attr    = edge_attr,
            primary_mask = primary_mask,
            num_nodes    = len(self.node_id_to_index),
        )

        print(
            f"[gnn_processor] Graph built: {self.graph.num_nodes} nodes, "
            f"{edge_index.shape[1]} edges, "
            f"edge_attr {list(edge_attr.shape)}  [lag_hours, scale_m, intercept_b, r_squared]"
        )

    def get_graph(self) -> Data:
        if self.graph is None:
            raise RuntimeError(
                "Graph not built. Ensure compute_edge_attributes() and "
                "pull_wandb() / pull_duckdb() have both been called."
            )
        return self.graph

    def edge_attr_summary(self):
        """Pretty-print edge attributes for all pairs."""
        if self.graph is None:
            print("No graph available yet.")
            return
        ea  = self.graph.edge_attr
        ei  = self.graph.edge_index
        idx_to_site = {v: k for k, v in self.node_id_to_index.items()}

        print(f"\n{'Upstream':<12} {'Primary':<12} {'lag_h':>7} {'scale_m':>9} {'intercept_b':>13} {'R²':>7}")
        print("-" * 65)
        for i in range(ea.shape[0]):
            up  = idx_to_site[ei[0, i].item()]
            dn  = idx_to_site[ei[1, i].item()]
            lag, m, b, r2 = ea[i].tolist()
            print(f"{up:<12} {dn:<12} {lag:>7.2f} {m:>9.4f} {b:>13.2f} {r2:>7.4f}")

    def build_gnn_tensors(
        self,
        split: str = "train",
        window_size: int = 72,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """
        Build sliding-window temporal tensors shaped for GNN input.

        Returns
        -------
        X : FloatTensor [num_windows, num_nodes, window_size, num_features]
        y : FloatTensor [num_windows, num_primary_nodes]
        """
        split_map = {
            "train": (self.train_X_scaled, self.train_y_scaled),
            "val":   (self.val_X_scaled,   self.val_y_scaled),
            "test":  (self.test_X_scaled,  self.test_y_scaled),
        }
        if split not in split_map:
            raise ValueError(f"split must be one of {list(split_map.keys())}")

        X_df, y_df    = split_map[split]
        feature_cols  = [c for c in X_df.columns if c not in ("site_id", "observation_hour")]
        target_col    = self.config["target"]
        primary_order = sorted(self.primary_sites)

        node_arrays:  dict[str, np.ndarray] = {}
        node_targets: dict[str, np.ndarray] = {}

        for site in self.ordered_sites:
            site_X = (
                X_df
                .filter(pl.col("site_id") == site)
                .sort("observation_hour")
                .select(feature_cols)
                .to_numpy()
            )
            node_arrays[site] = site_X

            if site in self.primary_sites:
                site_y = (
                    X_df
                    .filter(pl.col("site_id") == site)
                    .sort("observation_hour")
                    .join(
                        y_df.with_columns(
                            X_df.filter(pl.col("site_id") == site)
                            .sort("observation_hour")["observation_hour"]
                            .alias("observation_hour")
                        ),
                        on="observation_hour",
                        how="left",
                    )
                    [target_col]
                    .to_numpy()
                )
                node_targets[site] = site_y

        min_T        = min(arr.shape[0] for arr in node_arrays.values())
        node_arrays  = {s: arr[:min_T] for s, arr in node_arrays.items()}
        node_targets = {s: arr[:min_T] for s, arr in node_targets.items()}

        num_nodes    = len(self.ordered_sites)
        num_features = next(iter(node_arrays.values())).shape[1]

        node_stack   = np.stack([node_arrays[s]  for s in self.ordered_sites], axis=1)
        target_stack = np.stack([node_targets[s] for s in primary_order],      axis=1)

        num_windows = min_T - window_size
        X_out = np.zeros((num_windows, num_nodes, window_size, num_features), dtype=np.float32)
        y_out = np.zeros((num_windows, len(primary_order)), dtype=np.float32)

        for i in range(num_windows):
            X_out[i] = node_stack[i : i + window_size].transpose(1, 0, 2)
            y_out[i] = target_stack[i + window_size]

        return (
            torch.tensor(X_out, dtype=torch.float32),
            torch.tensor(y_out, dtype=torch.float32),
        )

    def summary(self):
        print("=" * 60)
        print("GNN Processor Summary")
        print("=" * 60)
        print(f"Total nodes      : {len(self.all_sites)}")
        print(f"Primary nodes    : {len(self.primary_sites)}")
        print(f"Upstream-only    : {len(self.upstream_only)}")
        if self.graph is not None:
            print(f"Graph edges      : {self.graph.edge_index.shape[1]}")
            print(f"Edge attr shape  : {list(self.graph.edge_attr.shape)}")
            print(f"Edge attr cols   : {EDGE_ATTR_COLS}")
        print("=" * 60)
