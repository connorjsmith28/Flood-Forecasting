"""Base model class for flood forecasting models."""

import os
import json
from abc import ABC, abstractmethod
from pathlib import Path
import numpy as np
import tensorflow as tf
import torch
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
            # error > 0 means y_true > y_pred, i.e. the model under-predicted actual streamflow
            # Apply the penalty there; over-predictions get weight=1 (standard MSE)
            weight = tf.where(error > 0, tf.cast(penalty, tf.float32), tf.cast(1.0, tf.float32))
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

    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> None:
        """Print overall test loss and MAE in scaled units.
        
        For full per-site analysis, inverse-transformed CFS metrics, and
        quantile accuracy, use PostProcessor.evaluate().
        """
        if self.model is None:
            raise RuntimeError("Call build() before evaluate()")
        results = self.model.evaluate(X_test, y_test, verbose=0)
        loss_name = self.model.loss if isinstance(self.model.loss, str) else "loss"
        print(f"Test {loss_name}: {results[0]:.4f}  |  MAE (scaled): {results[1]:.4f}")

    def summary(self):
        if self.model is not None:
            self.model.summary()

    def save_model(self, path: str | Path | None = None, name: str = "model") -> Path:
        """Save the trained model to disk.

        Automatically detects Keras vs PyTorch and saves in the appropriate
        native format (.keras or .pt). All paths are anchored to the repo root.
        Defaults to saving in <repo_root>/models/.

        Args:
            path: Optional subdirectory relative to repo root (e.g. "models/gru").
                Defaults to "models/" if not provided.
            name: Filename without extension (e.g. "gru_model").

        Returns:
            Path to the saved file.
        """
        if self.model is None:
            raise RuntimeError("No model to save — call build() and fit() first")

        repo_root = Path(__file__).resolve().parents[2]

        if path is None:
            directory = repo_root / "models"
        else:
            directory = repo_root / path

        directory.mkdir(parents=True, exist_ok=True)

        if isinstance(self.model, tf.keras.Model):
            out_path = directory / f"{name}.keras"
            self.model.save(out_path)
        elif isinstance(self.model, torch.nn.Module):
            out_path = directory / f"{name}.pt"
            torch.save(self.model, out_path)
        else:
            raise TypeError(f"Unsupported model type: {type(self.model)}")

        return out_path

    @classmethod
    def load_model(cls, name: str | Path, override_path: str | None = None, custom_objects: dict = None) -> "BaseModel":
        """Reconstruct a model instance from a saved file.
        
        Usage:
            model = LSTMModel.load_model("models/lstm.keras")
            model.predict(X_test)
        """

        repo_root = Path(__file__).resolve().parents[2]
        if override_path is None:
            path = repo_root / "models" / name
        else:
            path = repo_root / override_path / name

        if not path.exists():
            raise FileNotFoundError(f"No file found at {path}")

        instance = cls()

        suffix = path.suffix
        if suffix == ".keras":
            instance.model = tf.keras.models.load_model(path, custom_objects=custom_objects)
        elif suffix == ".pt":
            instance.model = torch.load(path, weights_only=True)
        else:
            raise ValueError(f"Unrecognized file extension: {suffix}")

        return instance
