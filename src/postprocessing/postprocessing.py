"""Post-processing and analysis utilities for flood forecasting models."""

import numpy as np
import matplotlib.pyplot as plt
import shap


class PostProcessor:
    """Analysis and interpretation tools for trained flood forecasting models.

    Handles per-site evaluation, prediction plotting, persistence baseline
    comparison, quantile flood accuracy, and SHAP explainability.

    Args:
        model:         A trained BaseModel instance (LSTMModel, GRUModel, etc.).
        X_test:        Test inputs, shape (n_samples, timesteps, n_features).
        y_test:        Test targets, shape (n_samples,), in scaled units.
        site_ids:      Site ID array aligned with X_test/y_test, shape (n_samples,).
        target_scaler: Fitted scaler with an inverse_transform method.
        feature_names: List of feature names, length == n_features.
    """

    def __init__(
        self,
        model,
        X_test: np.ndarray,
        y_test: np.ndarray,
        site_ids: np.ndarray,
        target_scaler,
        feature_names: list[str],
    ) -> None:
        self.model = model
        self.X_test = X_test
        self.y_test = y_test
        self.site_ids = site_ids
        self.target_scaler = target_scaler
        self.feature_names = feature_names

        # Cache predictions so we don't recompute repeatedly
        self._preds_scaled: np.ndarray | None = None   # raw model output, scaled units
        self._preds_cfs: np.ndarray | None = None      # inverse transformed to CFS
        self._actuals_cfs: np.ndarray | None = None    # inverse transformed to CFS

    def _get_predictions_scaled(self) -> np.ndarray:
        """Return model predictions in scaled units, computing and caching on first call."""
        if self._preds_scaled is None:
            self._preds_scaled = self.model.predict(self.X_test).flatten()
        return self._preds_scaled

    def _get_predictions_cfs(self) -> tuple[np.ndarray, np.ndarray]:
        """Return (preds_cfs, actuals_cfs), computing and caching on first call."""
        import torch
        if self._preds_cfs is None:
            self._preds_cfs = self.target_scaler.inverse_transform(
                torch.tensor(self._get_predictions_scaled()).reshape(-1, 1)
            ).numpy().flatten()
            self._actuals_cfs = self.target_scaler.inverse_transform(
                torch.tensor(self.y_test).reshape(-1, 1)
            ).numpy().flatten()
        return self._preds_cfs, self._actuals_cfs

    def evaluate(self) -> None:
        """Print overall test loss and a per-site table of actual mean,
        predicted mean, and MAE in CFS. Also shows quantile flood accuracy
        per site if flood_quantiles are available on the model.
        """
        preds_cfs, actuals_cfs = self._get_predictions_cfs()

        overall_loss = self.model.model.evaluate(self.X_test, self.y_test, verbose=0)
        loss_name = self.model.model.loss if isinstance(self.model.model.loss, str) else "loss"
        print(f"Overall test {loss_name}: {overall_loss[0]:.4f}  |  MAE (scaled): {overall_loss[1]:.4f}\n")

        for site in np.unique(self.site_ids):
            mask = self.site_ids == site
            site_preds = preds_cfs[mask]
            site_actuals = actuals_cfs[mask]
            mae = np.mean(np.abs(site_actuals - site_preds))

            print(f"Site {site}:")
            print(f"  {'Split':<10} {'Actual Mean':>15} {'Predicted Mean':>15} {'MAE':>12}")
            print(f"  {'-'*54}")
            print(f"  {'Test':<10} {np.mean(site_actuals):>12.1f} CFS {np.mean(site_preds):>12.1f} CFS {mae:>8.1f} CFS")

            # Quantile flood accuracy if thresholds are available
            if (
                self.model.flood_quantiles is not None
                and str(site) in self.model.flood_quantiles
            ):
                self.quantile_accuracy(
                    preds=site_preds,
                    actuals=site_actuals,
                    thresholds=self.model.flood_quantiles[str(site)],
                )
            print()

    def plot_results(self, n_samples: int = 500, site_id: str | None = None) -> None:
        """Plot predicted vs actual streamflow in original CFS scale.

        Args:
            n_samples: Number of timesteps to plot. Default 500.
            site_id:   Optional site ID string for the plot title.
        """
        preds_cfs, actuals_cfs = self._get_predictions_cfs()

        title = f"Predictions vs Actual (first {n_samples} samples)"
        if site_id is not None:
            title = f"Site {site_id} — " + title

        plt.figure(figsize=(14, 5))
        plt.plot(actuals_cfs[:n_samples], label="Actual", color="steelblue")
        plt.plot(preds_cfs[:n_samples], label="Predicted", color="orange")
        plt.xlabel("Time step")
        plt.ylabel("Streamflow (CFS)")
        plt.title(title)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def persistence_baseline(self, streamflow_feature: str = "streamflow_cfs_mean") -> None:
        """Compare model performance against a naive persistence baseline.

        The persistence baseline predicts that the next streamflow value equals
        the most recently observed value (last timestep of each sequence).

        Args:
            streamflow_feature: Name of the streamflow feature to use as the
                                persistence prediction. Must be in feature_names.
        """
        if streamflow_feature not in self.feature_names:
            raise ValueError(f"'{streamflow_feature}' not found in feature_names.")

        streamflow_idx = self.feature_names.index(streamflow_feature)
        persistence_scaled = self.X_test[:, -1, streamflow_idx]
        model_scaled = self._get_predictions_scaled()

        def _metrics(preds, actuals):
            mae  = np.mean(np.abs(actuals - preds))
            rmse = np.sqrt(np.mean((actuals - preds) ** 2))
            nse  = 1 - (
                np.sum((actuals - preds) ** 2) /
                np.sum((actuals - actuals.mean()) ** 2)
            )
            return mae, rmse, nse

        mae_p, rmse_p, nse_p = _metrics(persistence_scaled, self.y_test)
        mae_m, rmse_m, nse_m = _metrics(model_scaled, self.y_test)

        print(f"{'Model':<20} {'MAE (scaled)':>15} {'RMSE (scaled)':>15} {'NSE':>10}")
        print(f"{'-'*62}")
        print(f"{'Persistence':<20} {mae_p:>15.4f} {rmse_p:>15.4f} {nse_p:>10.4f}")
        print(f"{'Model':<20} {mae_m:>15.4f} {rmse_m:>15.4f} {nse_m:>10.4f}")

    @staticmethod
    def quantile_accuracy(
        preds: np.ndarray,
        actuals: np.ndarray,
        thresholds: dict,
    ) -> list[float]:
        """Calculate flood prediction accuracy for multiple quantile thresholds.

        For each threshold, evaluates only on timesteps where actual streamflow
        exceeds the threshold (actual flood events), then measures what fraction
        of those events the model also predicted as floods.

        Args:
            preds:      1D array of predicted streamflow values (CFS).
            actuals:    1D array of actual streamflow values (CFS).
            thresholds: Dict mapping quantile name (e.g. 'Q2') to CFS threshold value.

        Returns:
            List of accuracy values (0–1), one per quantile.
        """
        quantile_names = list(thresholds.keys())
        accuracies = []

        for q in quantile_names:
            threshold = thresholds[q]
            flood_mask = actuals >= threshold
            if flood_mask.sum() == 0:
                accuracies.append(0.0)
                continue
            correct = (preds[flood_mask] >= threshold).sum()
            acc = correct / flood_mask.sum()
            accuracies.append(float(acc))

        try:
            plt.figure(figsize=(max(8, len(quantile_names) * 1.5), 4))
            plt.bar(quantile_names, accuracies, color="skyblue")
            plt.xlabel("Quantile")
            plt.ylabel("Accuracy")
            plt.title("Flood Prediction Accuracy by Quantile")
            plt.ylim(0, 1)
            plt.tight_layout()
            plt.show()
        except Exception:
            pass

        return accuracies

    def compute_shap(
        self,
        background_size: int = 100,
        sample_index: int = 0,
        max_explain_samples: int = 300,
        nsamples: int = 200,
    ) -> None:
        """Compute SHAP values via KernelSHAP and produce three plots:
          1. Beeswarm     — global feature importance (mean over timesteps)
          2. Waterfall    — single prediction breakdown
          3. Temporal heatmap — mean |SHAP| by timestep × feature

        Args:
            background_size:     Number of background samples for the explainer.
            sample_index:        Which test sample to use for the waterfall plot.
            max_explain_samples: Cap on samples passed to SHAP (performance guard).
            nsamples:            KernelSHAP estimation budget per sample.
                                 50 for quick exploration, 200 default, 500 for report.
        """
        X = self.X_test.copy()

        if len(X) > max_explain_samples:
            print(f"Subsampling X_test from {len(X)} to {max_explain_samples} samples for SHAP...")
            idx = np.random.choice(len(X), size=max_explain_samples, replace=False)
            X = X[idx]

        background = X[:background_size]
        n_samples, timesteps, n_features = X.shape

        print(f"Computing SHAP values via KernelSHAP (nsamples={nsamples})...")
        flat_background = background.reshape(background_size, -1)
        flat_X = X.reshape(n_samples, -1)

        def model_predict_flat(x_flat):
            return self.model.predict(
                x_flat.reshape(-1, timesteps, n_features), verbose=0
            ).flatten()

        explainer = shap.KernelExplainer(model_predict_flat, flat_background)
        shap_flat = explainer.shap_values(flat_X, nsamples=nsamples)
        shap_values = shap_flat.reshape(n_samples, timesteps, n_features)
        print("Done.")

        # Collapse time axis for beeswarm and waterfall
        shap_2d = shap_values.mean(axis=1)  # (n_samples, n_features)
        X_2d    = X.mean(axis=1)            # (n_samples, n_features)

        # 1. Beeswarm
        shap_exp = shap.Explanation(
            values=shap_2d,
            data=X_2d,
            feature_names=self.feature_names,
        )
        plt.figure()
        shap.plots.beeswarm(shap_exp, show=False)
        plt.title("SHAP — Global Feature Importance (mean over timesteps)")
        plt.tight_layout()
        plt.savefig("shap_beeswarm.png", dpi=150)
        plt.show()

        # 2. Waterfall
        base_val = explainer.expected_value
        if isinstance(base_val, (list, np.ndarray)):
            base_val = float(base_val[0])

        single_exp = shap.Explanation(
            values=shap_2d[sample_index],
            base_values=float(base_val),
            data=X_2d[sample_index],
            feature_names=self.feature_names,
        )
        plt.figure()
        shap.plots.waterfall(single_exp, show=False)
        plt.title(f"SHAP — Waterfall for sample {sample_index}")
        plt.tight_layout()
        plt.savefig("shap_waterfall.png", dpi=150)
        plt.show()

        # 3. Temporal heatmap
        mean_abs = np.abs(shap_values).mean(axis=0)  # (timesteps, n_features)

        fig, ax = plt.subplots(figsize=(max(8, len(self.feature_names) * 0.7), 5))
        im = ax.imshow(mean_abs.T, aspect="auto", cmap="YlOrRd")
        ax.set_xticks(range(timesteps))
        ax.set_xticklabels(
            [f"t-{timesteps - i}" for i in range(timesteps)],
            rotation=45, ha="right",
        )
        ax.set_yticks(range(len(self.feature_names)))
        ax.set_yticklabels(self.feature_names)
        ax.set_xlabel("Timestep (lag)")
        ax.set_ylabel("Feature")
        ax.set_title("SHAP — Mean |SHAP| by Timestep × Feature")
        plt.colorbar(im, ax=ax, label="Mean |SHAP|")
        plt.tight_layout()
        plt.savefig("shap_temporal_heatmap.png", dpi=150)
        plt.show()