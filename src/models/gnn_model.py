# =============================================================================
# gnn_model.py — Spatio-Temporal GNN for Flood Forecasting
# =============================================================================
#
# Architecture: per-node LSTM (temporal) → GATv2 + edge_attr (spatial)
#               → learned gate fusion with LSTM → MLP decoder
#
# Edge attributes [lag_hours, scale_m, intercept_b, r_squared] are computed
# from analyze_gauge_relationship() and fed into each GATv2 layer via edge_dim.
# This lets attention weights vary based on the physical properties of each
# upstream→downstream connection.
#
# For nodes with no upstream edges (19 isolated sites), the GATv2 layer
# reduces to a self-loop. A learned per-node gate automatically down-weights
# the GNN branch for these sites and relies on the LSTM branch instead.
# =============================================================================

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATv2Conv
import matplotlib.pyplot as plt
import numpy as np

from src.preprocessing.gnn_preprocessing import NUM_EDGE_FEATURES   # 4


class StreamflowGNN(nn.Module):
    """
    Hybrid LSTM + GATv2 spatio-temporal GNN for multi-site streamflow prediction.

    Parameters
    ----------
    num_dynamic_features : int   Number of dynamic (time-varying) input features.
    num_static_features  : int   Number of static per-node attributes.
    hidden_dim           : int   LSTM hidden size and GATv2 channel width. Default 64.
    lstm_layers          : int   Number of stacked LSTM layers. Default 2.
    gat_heads            : int   GATv2 attention heads. Default 4.
    gat_layers           : int   Number of GATv2 message-passing layers. Default 2.
    dropout              : float Dropout rate. Default 0.2.
    """

    def __init__(
        self,
        num_dynamic_features: int,
        num_static_features:  int,
        hidden_dim:  int   = 64,
        lstm_layers: int   = 2,
        gat_heads:   int   = 4,
        gat_layers:  int   = 2,
        dropout:     float = 0.2,
    ) -> None:
        super().__init__()

        self.hidden_dim      = hidden_dim
        self.gat_layers_count = gat_layers

        # ------------------------------------------------------------------
        # 1. Temporal encoder: per-node LSTM
        #    Input:  [num_nodes, window_size, num_dynamic_features]
        #    Output: [num_nodes, hidden_dim]
        # ------------------------------------------------------------------
        self.lstm = nn.LSTM(
            input_size  = num_dynamic_features,
            hidden_size = hidden_dim,
            num_layers  = lstm_layers,
            batch_first = True,
            dropout     = dropout if lstm_layers > 1 else 0.0,
        )

        # Project static features and fuse with LSTM output
        self.static_proj = nn.Sequential(
            nn.Linear(num_static_features, hidden_dim),
            nn.ReLU(),
        )
        self.fusion = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
        )

        # ------------------------------------------------------------------
        # 2. Spatial encoder: stacked GATv2 layers with edge attributes
        #
        #    edge_dim=NUM_EDGE_FEATURES (4) tells GATv2Conv to incorporate
        #    [lag_hours, scale_m, intercept_b, r_squared] into the attention
        #    score for each edge. The attention coefficient for edge i→j becomes:
        #
        #      α_ij = softmax( LeakyReLU( a^T [W·h_i ‖ W·h_j ‖ W_e·e_ij] ) )
        #
        #    So an edge with high r_squared and appropriate lag_hours will
        #    naturally learn higher attention than a noisy low-R² edge.
        # ------------------------------------------------------------------
        self.gat_convs = nn.ModuleList()
        self.gat_norms = nn.ModuleList()

        for _ in range(gat_layers):
            self.gat_convs.append(
                GATv2Conv(
                    in_channels  = hidden_dim,
                    out_channels = hidden_dim,
                    heads        = gat_heads,
                    edge_dim     = NUM_EDGE_FEATURES,   # 4: lag_hours, scale_m, intercept_b, r_squared
                    concat       = False,               # average heads → stays at hidden_dim
                    dropout      = dropout,
                    add_self_loops = True,
                )
            )
            self.gat_norms.append(nn.LayerNorm(hidden_dim))

        # ------------------------------------------------------------------
        # 3. Learned per-node gate
        #
        #    gate[i] ∈ [0, 1] controls how much the GNN branch contributes
        #    vs. the LSTM branch for each node. Isolated nodes (no upstream
        #    edges) will learn gate ≈ 0 during training; well-connected nodes
        #    will learn gate > 0.
        #
        #    h_fused = gate * h_gnn + (1 - gate) * h_lstm
        # ------------------------------------------------------------------
        self.gate_fc = nn.Linear(hidden_dim, 1)

        # ------------------------------------------------------------------
        # 4. Decoder: MLP applied at primary nodes only
        # ------------------------------------------------------------------
        self.decoder = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout / 2),
            nn.Linear(hidden_dim // 2, 1),
        )

        # Training history
        self._train_losses: list[float] = []
        self._val_losses:   list[float] = []

    def forward(
        self,
        x_seq:        torch.Tensor,   # [num_nodes, window_size, num_dynamic_features]
        x_static:     torch.Tensor,   # [num_nodes, num_static_features]
        edge_index:   torch.Tensor,   # [2, num_edges]
        edge_attr:    torch.Tensor,   # [num_edges, 4]  ← lag_hours, scale_m, intercept_b, r_squared
        primary_mask: torch.Tensor,   # [num_nodes] bool
    ) -> torch.Tensor:
        """
        Returns
        -------
        predictions : FloatTensor [num_primary_nodes, 1]
        """
        # ---- 1. Temporal encoding ----
        lstm_out, _ = self.lstm(x_seq)       # [num_nodes, window, hidden_dim]
        h_lstm      = lstm_out[:, -1, :]     # [num_nodes, hidden_dim]

        # ---- 2. Static fusion ----
        h_static = self.static_proj(x_static)
        h        = self.fusion(torch.cat([h_lstm, h_static], dim=-1))   # [num_nodes, hidden_dim]

        # ---- 3. Spatial message passing with edge attributes ----
        h_gnn = h
        for conv, norm in zip(self.gat_convs, self.gat_norms):
            h_new = F.elu(conv(h_gnn, edge_index, edge_attr))   # edge_attr injected here
            h_gnn = norm(h_gnn + h_new)                         # residual + LayerNorm

        # ---- 4. Per-node gate: blend GNN and LSTM branches ----
        gate   = torch.sigmoid(self.gate_fc(h_lstm))            # [num_nodes, 1]
        h_fused = gate * h_gnn + (1 - gate) * h_lstm            # [num_nodes, hidden_dim]

        # ---- 5. Decode at primary nodes only ----
        h_primary   = h_fused[primary_mask]                     # [num_primary, hidden_dim]
        predictions = self.decoder(h_primary)                   # [num_primary, 1]
        return predictions

    # ------------------------------------------------------------------
    # Training loop
    # ------------------------------------------------------------------

    def fit(
        self,
        train_loader,
        val_loader,
        edge_index:      torch.Tensor,
        edge_attr:       torch.Tensor,
        primary_mask:    torch.Tensor,
        static_features: torch.Tensor,
        optimizer:       torch.optim.Optimizer,
        epochs:          int   = 50,
        patience:        int   = 5,
        device:          str   = "cuda" if torch.cuda.is_available() else "cpu",
    ):
        """
        Train with early stopping.

        DataLoader batches: (X_dyn, y)
            X_dyn : [batch, num_nodes, window_size, num_dynamic_features]
            y     : [batch, num_primary_nodes]
        """
        self.to(device)
        edge_index      = edge_index.to(device)
        edge_attr       = edge_attr.to(device)
        primary_mask    = primary_mask.to(device)
        static_features = static_features.to(device)

        best_val_loss    = float("inf")
        patience_counter = 0
        best_state_dict  = None

        for epoch in range(1, epochs + 1):
            # Train
            self.train()
            train_loss = 0.0
            for X_dyn, y_batch in train_loader:
                X_dyn   = X_dyn.to(device)
                y_batch = y_batch.to(device)

                batch_preds = []
                for b in range(X_dyn.shape[0]):
                    pred = self(
                        x_seq        = X_dyn[b],
                        x_static     = static_features,
                        edge_index   = edge_index,
                        edge_attr    = edge_attr,
                        primary_mask = primary_mask,
                    )
                    batch_preds.append(pred)

                preds = torch.stack(batch_preds, dim=0).squeeze(-1)   # [B, num_primary]
                loss  = F.mse_loss(preds, y_batch)
                optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.parameters(), max_norm=1.0)
                optimizer.step()
                train_loss += loss.item()

            train_loss /= len(train_loader)
            self._train_losses.append(train_loss)

            # Validate
            self.eval()
            val_loss = 0.0
            with torch.no_grad():
                for X_dyn, y_batch in val_loader:
                    X_dyn   = X_dyn.to(device)
                    y_batch = y_batch.to(device)
                    batch_preds = []
                    for b in range(X_dyn.shape[0]):
                        pred = self(
                            x_seq        = X_dyn[b],
                            x_static     = static_features,
                            edge_index   = edge_index,
                            edge_attr    = edge_attr,
                            primary_mask = primary_mask,
                        )
                        batch_preds.append(pred)
                    preds     = torch.stack(batch_preds, dim=0).squeeze(-1)
                    val_loss += F.mse_loss(preds, y_batch).item()

            val_loss /= len(val_loader)
            self._val_losses.append(val_loss)

            print(f"Epoch {epoch:>3}/{epochs}  train={train_loss:.6f}  val={val_loss:.6f}")

            if val_loss < best_val_loss:
                best_val_loss    = val_loss
                best_state_dict  = {k: v.clone() for k, v in self.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    print(f"\nEarly stopping (best val={best_val_loss:.6f})")
                    break

        if best_state_dict:
            self.load_state_dict(best_state_dict)
            print("Restored best weights.")

    # ------------------------------------------------------------------
    # Evaluation and diagnostics
    # ------------------------------------------------------------------

    def plot_training_history(self):
        if not self._train_losses:
            print("No training history.")
            return
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(self._train_losses, label="Train")
        ax.plot(self._val_losses,   label="Val")
        ax.set_xlabel("Epoch")
        ax.set_ylabel("MSE Loss (scaled)")
        ax.set_title("GNN Training History")
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.4)
        plt.tight_layout()
        plt.show()

    def get_edge_attention_weights(
        self,
        x_seq:        torch.Tensor,
        x_static:     torch.Tensor,
        edge_index:   torch.Tensor,
        edge_attr:    torch.Tensor,
        primary_mask: torch.Tensor,
        layer: int = 0,
    ) -> torch.Tensor:
        """
        Extract attention weights from a specific GATv2 layer for interpretability.
        Returns alpha [num_edges, num_heads] — higher weight = more influence.
        """
        self.eval()
        with torch.no_grad():
            lstm_out, _ = self.lstm(x_seq)
            h_lstm      = lstm_out[:, -1, :]
            h_static    = self.static_proj(x_static)
            h           = self.fusion(torch.cat([h_lstm, h_static], dim=-1))

            _, (_, alpha) = self.gat_convs[layer](
                h, edge_index, edge_attr, return_attention_weights=True
            )
        return alpha   # [num_edges, num_heads]

    def evaluate(
        self,
        test_loader,
        edge_index:        torch.Tensor,
        edge_attr:         torch.Tensor,
        primary_mask:      torch.Tensor,
        static_features:   torch.Tensor,
        target_scaler,
        primary_site_order: list[str],
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
    ) -> dict:
        """
        Evaluate on test set. Inverse-transforms predictions and prints
        per-site NSE and RMSE.
        """
        self.eval()
        self.to(device)
        edge_index      = edge_index.to(device)
        edge_attr       = edge_attr.to(device)
        primary_mask    = primary_mask.to(device)
        static_features = static_features.to(device)

        all_preds   = []
        all_targets = []

        with torch.no_grad():
            for X_dyn, y_batch in test_loader:
                X_dyn   = X_dyn.to(device)
                y_batch = y_batch.to(device)
                batch_preds = []
                for b in range(X_dyn.shape[0]):
                    pred = self(
                        x_seq        = X_dyn[b],
                        x_static     = static_features,
                        edge_index   = edge_index,
                        edge_attr    = edge_attr,
                        primary_mask = primary_mask,
                    )
                    batch_preds.append(pred)
                preds = torch.stack(batch_preds, dim=0).squeeze(-1)
                all_preds.append(preds.cpu())
                all_targets.append(y_batch.cpu())

        all_preds   = torch.cat(all_preds,   dim=0)
        all_targets = torch.cat(all_targets, dim=0)

        preds_inv   = target_scaler.inverse_transform(all_preds.unsqueeze(-1)).squeeze(-1)
        targets_inv = target_scaler.inverse_transform(all_targets.unsqueeze(-1)).squeeze(-1)

        metrics = {}
        print(f"\n{'Site':<12} {'NSE':>8} {'RMSE (cfs)':>12}")
        print("-" * 36)
        for i, site in enumerate(primary_site_order):
            obs  = targets_inv[:, i].numpy()
            pred = preds_inv[:, i].numpy()
            nse  = 1 - np.sum((obs - pred) ** 2) / (np.sum((obs - obs.mean()) ** 2) + 1e-8)
            rmse = np.sqrt(np.mean((obs - pred) ** 2))
            metrics[site] = {"nse": nse, "rmse": rmse}
            print(f"{site:<12} {nse:>8.4f} {rmse:>12.2f}")

        all_obs  = targets_inv.numpy().flatten()
        all_pred = preds_inv.numpy().flatten()
        agg_nse  = 1 - np.sum((all_obs - all_pred) ** 2) / (
            np.sum((all_obs - all_obs.mean()) ** 2) + 1e-8)
        agg_rmse = np.sqrt(np.mean((all_obs - all_pred) ** 2))
        print("-" * 36)
        print(f"{'AGGREGATE':<12} {agg_nse:>8.4f} {agg_rmse:>12.2f}")
        metrics["aggregate"] = {"nse": agg_nse, "rmse": agg_rmse}
        return metrics
