"""Base model class for flood forecasting models."""

from abc import ABC, abstractmethod
from pathlib import Path
import pickle
import numpy as np
import tensorflow as tf
import torch
import shap
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


class BaseModel(ABC):
    """Abstract base for all flood forecasting models.

    Subclasses must implement `build` to return a compiled Keras model.
    Provides a unified `save` method that persists the model weights
    and (optionally) logs the artifact to W&B.
    """

    def __init__(self, under_predict_penalty: float = 2.0, learning_rate: float = 1e-3) -> None:
        self.model: tf.keras.Model | None = None
        self.history: dict | None = None
        self.dict_quantiles: dict | None = None
        self.under_predict_penalty = under_predict_penalty
        self.learning_rate = learning_rate

    def _loss(self):
        if self.under_predict_penalty == 1.0:
            return "mse"
        penalty = self.under_predict_penalty
        def asymmetric_mse(y_true, y_pred):
            error = y_true - y_pred
            weight = tf.where(error > 0, penalty, 1.0)
            return tf.reduce_mean(weight * tf.square(error))
        return asymmetric_mse

    @abstractmethod
    def build(self, input_shape: tuple[int, ...]) -> tf.keras.Model:
        """Construct and compile the Keras model."""

    def fit(self, train_ds, val_ds, epochs: int = 50, callbacks=None, **kwargs):
        """Train the model, delegating to the underlying Keras model."""
        if self.model is None:
            raise RuntimeError("Call build() before fit()")
        if isinstance(train_ds, tuple):
            X_train, y_train = train_ds
        else:
            X_train, y_train = train_ds, None

        result = self.model.fit(
            X_train,
            y_train,
            epochs=epochs,
            validation_data=val_ds,
            callbacks=callbacks,
            **kwargs,
        )
        self.history = result.history
        return result

    def predict(self, X, **kwargs) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Call build() before predict()")
        return self.model.predict(X, **kwargs)

    def classify(self, X, site: str, **kwargs) -> np.ndarray:
        """Predict streamflow then classify as flood (1) or not (0).

        Uses ``dict_quantiles[site]`` as the flood threshold.
        Returns a binary array the same shape as raw predictions.
        """
        if self.dict_quantiles is None:
            raise RuntimeError("Set dict_quantiles before calling classify()")
        prediction = self.predict(X, **kwargs)
        threshold = self.dict_quantiles[site]
        return (prediction >= threshold).astype(np.int32)

    def evaluate(
        self,
        X_test: np.ndarray,
        y_test: np.ndarray,
        site_ids: np.ndarray,
        target_scaler,
    ) -> None:
        """Print a per-site table of actual mean, predicted mean, and MAE in original CFS scale.

        Args:
            X_test:        3D array of shape (num_sequences, window_size, num_features).
            y_test:        1D array of shape (num_sequences,) in scaled units.
            site_ids:      1D array of site ID strings aligned with X_test/y_test sequences.
            target_scaler: Fitted TorchStandardScaler used to inverse transform predictions.
        """
        import torch

        preds_scaled = self.predict(X_test).flatten()
        
        # Inverse transform both predictions and actuals back to CFS
        preds_cfs = target_scaler.inverse_transform(
            torch.tensor(preds_scaled).reshape(-1, 1)
        ).numpy().flatten()
        
        actuals_cfs = target_scaler.inverse_transform(
            torch.tensor(y_test).reshape(-1, 1)
        ).numpy().flatten()

        overall_loss = self.model.evaluate(X_test, y_test, verbose=0)
        loss_name = self.model.loss if isinstance(self.model.loss, str) else "loss"
        print(f"Overall test {loss_name}: {overall_loss[0]:.4f}  |  MAE (scaled): {overall_loss[1]:.4f}\n")
        
        for site in np.unique(site_ids):
            mask = site_ids == site
            site_preds = preds_cfs[mask]
            site_actuals = actuals_cfs[mask]
            mae = np.mean(np.abs(site_actuals - site_preds))

            print(f"Site {site}:")
            print(f"  {'Split':<10} {'Actual Mean':>15} {'Predicted Mean':>15} {'MAE':>12}")
            print(f"  {'-'*54}")
            print(f"  {'Test':<10} {np.mean(site_actuals):>12.1f} CFS {np.mean(site_preds):>12.1f} CFS {mae:>8.1f} CFS")
            print()

    def plot_training_history(self):
        """Plot training/validation loss and MAE curves."""
        if self.history is None:
            raise RuntimeError("No history — call fit() first")

        loss = self.history["loss"]
        val_loss = self.history["val_loss"]
        mae = self.history["mae"]
        val_mae = self.history["val_mae"]
        epochs = range(1, len(loss) + 1)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        ax1.plot(epochs, loss, label="Train Loss")
        ax1.plot(epochs, val_loss, label="Val Loss")
        ax1.set_xlabel("Epoch")
        ax1.set_ylabel("Loss (MSE)")
        ax1.set_title("Loss vs Val Loss")
        ax1.legend()
        ax1.grid(True)
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))

        ax2.plot(epochs, mae, label="Train MAE")
        ax2.plot(epochs, val_mae, label="Val MAE")
        ax2.set_xlabel("Epoch")
        ax2.set_ylabel("MAE")
        ax2.set_title("MAE vs Val MAE")
        ax2.legend()
        ax2.grid(True)
        ax2.xaxis.set_major_locator(MaxNLocator(integer=True))

        plt.tight_layout()
        plt.show()

    def plot_results(
        self,
        X_test: np.ndarray,
        y_test: np.ndarray,
        target_scaler,
        n_samples: int = 500,
        site_id: str | None = None,
    ) -> None:
        """Plot predicted vs actual streamflow in original CFS scale.

        Args:
            X_test:        3D array of shape (num_sequences, window_size, num_features).
            y_test:        1D array of shape (num_sequences,) in scaled units.
            target_scaler: Fitted TorchStandardScaler to inverse transform back to CFS.
            n_samples:     Number of timesteps to plot. Default 500.
            site_id:       Optional site ID string for the plot title.
        """
        import torch
        import matplotlib.pyplot as plt

        preds_scaled = self.predict(X_test).flatten()

        preds_cfs = target_scaler.inverse_transform(
            torch.tensor(preds_scaled).reshape(-1, 1)
        ).numpy().flatten()

        actuals_cfs = target_scaler.inverse_transform(
            torch.tensor(y_test).reshape(-1, 1)
        ).numpy().flatten()

        # Slice to n_samples
        preds_cfs = preds_cfs[:n_samples]
        actuals_cfs = actuals_cfs[:n_samples]

        title = f"Predictions vs Actual (first {n_samples} samples)"
        if site_id is not None:
            title = f"Site {site_id} — " + title

        plt.figure(figsize=(14, 5))
        plt.plot(actuals_cfs, label="Actual", color="steelblue")
        plt.plot(preds_cfs, label="Predicted", color="orange")
        plt.xlabel("Time step")
        plt.ylabel("Streamflow (CFS)")
        plt.title(title)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def summary(self):
        if self.model is not None:
            self.model.summary()

    def save_model(self, path: str | Path, name: str = "model") -> Path:
        """Save the trained model to disk.

        Automatically detects Keras vs PyTorch and saves in the appropriate
        native format (.keras or .pt).
        Returns the path to the saved file.
        """
        if self.model is None:
            raise RuntimeError("No model to save — call build() and fit() first")
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        if isinstance(self.model, tf.keras.Model):
            out_path = path / f"{name}.keras"
            self.model.save(out_path)

        elif isinstance(self.model, torch.nn.Module):
            out_path = path / f"{name}.pt"
            torch.save(self.model, out_path)

        else:
            raise TypeError(f"Unsupported model type: {type(self.model)}")

        return out_path

    @classmethod
    def load_model(cls, path: str | Path) -> "BaseModel":
        """Reconstruct a model instance from a saved file.
        
        Usage:
            model = LSTMModel.load_model("models/lstm.keras")
            model.predict(X_test)
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"No file found at {path}")

        instance = cls() 

        suffix = path.suffix
        if suffix == ".keras":
            instance.model = tf.keras.models.load_model(path)
        elif suffix == ".pt":
            instance.model = torch.load(path, weights_only=True)
        else:
            raise ValueError(f"Unrecognized file extension: {suffix}")

        return instance

    
    def compute_shap(
        self,
        X_test: np.ndarray,
        feature_names: list[str],
        background_size: int = 100,
        sample_index: int = 0,
        max_explain_samples: int = 300,
        nsamples: int = 200,
    ) -> None:
        """
        Compute SHAP values and produce three plots:
        1. Beeswarm (global feature importance)
        2. Waterfall (single prediction breakdown)
        3. Temporal heatmap (timestep x feature SHAP values)

        Attempts DeepSHAP first; falls back to KernelSHAP automatically if
        DeepSHAP fails (e.g. due to unsupported layers in the Transformer).

        Args:
            X_test:          Test inputs, shape (n_samples, timesteps, n_features).
            feature_names:   List of feature names, length == n_features.
            background_size: Number of background samples for the explainer.
            sample_index:    Which test sample to use for the waterfall plot.
        """
        assert self.model is not None, "Call build() and fit() before compute_shap()."
        
        if len(X_test) > max_explain_samples:
            print(f"Subsampling X_test from {len(X_test)} to {max_explain_samples} samples for SHAP...")
            idx = np.random.choice(len(X_test), size=max_explain_samples, replace=False)
            X_test = X_test[idx]

        background = X_test[:background_size]
        n_samples, timesteps, n_features = X_test.shape

        print("Computing SHAP values via KernelSHAP...")
        flat_background = background.reshape(background_size, -1)
        flat_X = X_test.reshape(n_samples, -1)

        def model_predict_flat(x_flat):
            return self.model.predict(
                x_flat.reshape(-1, timesteps, n_features), verbose=0
            ).flatten()

        explainer = shap.KernelExplainer(model_predict_flat, flat_background)
        shap_flat = explainer.shap_values(flat_X, nsamples=nsamples)
        shap_values = shap_flat.reshape(n_samples, timesteps, n_features)
        print("Done.")



        shap_2d = shap_values.mean(axis=1)  
        X_2d    = X_test.mean(axis=1)       

        shap_exp = shap.Explanation(
            values=shap_2d,
            data=X_2d,
            feature_names=feature_names,
        )
        plt.figure()
        shap.plots.beeswarm(shap_exp, show=False)
        plt.title("SHAP — Global Feature Importance (mean over timesteps)")
        plt.tight_layout()
        plt.savefig("shap_beeswarm.png", dpi=150)
        plt.show()



        base_val = explainer.expected_value
        if isinstance(base_val, (list, np.ndarray)):
            base_val = float(base_val[0])

        single_exp = shap.Explanation(
            values=shap_2d[sample_index],
            base_values=float(base_val),
            data=X_2d[sample_index],
            feature_names=feature_names,
        )
        plt.figure()
        shap.plots.waterfall(single_exp, show=False)
        plt.title(f"SHAP — Waterfall for sample {sample_index}")
        plt.tight_layout()
        plt.savefig("shap_waterfall.png", dpi=150)
        plt.show()



        mean_abs = np.abs(shap_values).mean(axis=0) 

        fig, ax = plt.subplots(figsize=(max(8, len(feature_names) * 0.7), 5))
        im = ax.imshow(mean_abs.T, aspect="auto", cmap="YlOrRd")
        ax.set_xticks(range(timesteps))
        ax.set_xticklabels(
            [f"t-{timesteps - i}" for i in range(timesteps)],
            rotation=45, ha="right"
        )
        ax.set_yticks(range(len(feature_names)))
        ax.set_yticklabels(feature_names)
        ax.set_xlabel("Timestep (lag)")
        ax.set_ylabel("Feature")
        ax.set_title("SHAP — Mean |SHAP| by Timestep × Feature")
        plt.colorbar(im, ax=ax, label="Mean |SHAP|")
        plt.tight_layout()
        plt.savefig("shap_temporal_heatmap.png", dpi=150)
        plt.show()