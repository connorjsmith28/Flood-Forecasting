"""Base model class for flood forecasting models."""

from abc import ABC, abstractmethod
from pathlib import Path
import os
import json
import numpy as np
import tensorflow as tf
import torch
def flood_prediction_accuracy(preds, actual, thresholds):
    """
    Calculate flood prediction accuracy for multiple quantiles.
    Args:
        preds:      np.ndarray, shape (n_samples,) — 1D array of predicted streamflow values
        actual:     np.ndarray, shape (n_samples,) — 1D array of actual streamflow values
        thresholds: dict mapping quantile name to threshold value
    Returns:
        List of accuracy metrics, one per quantile.
    """
    quantile_names = list(thresholds.keys())
    accuracies = []
    n_quantiles = len(quantile_names)
    for q in quantile_names:
        threshold = thresholds[q]
        # Only evaluate on timesteps where actual streamflow exceeds the threshold (actual flood events)
        flood_mask = actual >= threshold
        if flood_mask.sum() == 0:
            accuracies.append(0.0)
            continue
        flood_preds = preds[flood_mask]
        flood_actuals = actual[flood_mask]
        # Of those actual flood events, how many did the model also predict as flood?
        correct = (flood_preds >= threshold).sum()
        total = flood_mask.sum()
        acc = correct / total
        accuracies.append(float(acc))
    try:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(max(8, n_quantiles * 1.5), 4))
        plt.bar(quantile_names, accuracies, color="skyblue")
        plt.xlabel("Quantile")
        plt.ylabel("Accuracy")
        plt.title("Flood Prediction Accuracy by Quantile")
        plt.ylim(0, 1)
        plt.tight_layout()
        plt.show()
    except ImportError:
        print("matplotlib is not installed; skipping plot.")

    return accuracies
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
        self.flood_quantiles: dict | None = None
        quantile_path = os.path.join(os.path.dirname(__file__), '../static/top_site_quantile_thesholds.json')
        quantile_path = os.path.abspath(quantile_path)
        if os.path.exists(quantile_path):
            try:
                with open(quantile_path, 'r') as f:
                    self.flood_quantiles = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load flood quantiles from {quantile_path}: {e}")
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
            flood_prediction_accuracy(site_preds, site_actuals, self.flood_quantiles[str(site)])
    def plot_training_history(self):
        """Plot training/validation loss and MAE curves."""
        if self.history is None:
            raise RuntimeError("No history — call fit() first")

        import matplotlib.pyplot as plt
        from matplotlib.ticker import MaxNLocator

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

        instance = cls.__new__(cls)  
        BaseModel.__init__(instance) 

        suffix = path.suffix
        if suffix == ".keras":
            instance.model = tf.keras.models.load_model(path)
        elif suffix == ".pt":
            instance.model = torch.load(path, weights_only=True)
        else:
            raise ValueError(f"Unrecognized file extension: {suffix}")

        return instance
    
