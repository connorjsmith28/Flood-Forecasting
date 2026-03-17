"""Base model class for flood forecasting models."""

from abc import ABC, abstractmethod
from pathlib import Path
import pickle
import numpy as np
import tensorflow as tf


class BaseModel(ABC):
    """Abstract base for all flood forecasting models.

    Subclasses must implement `build` to return a compiled Keras model.
    Provides a unified `save` method that persists the model weights
    and (optionally) logs the artifact to W&B.
    """

    def __init__(self) -> None:
        self.model: tf.keras.Model | None = None
        self.history: dict | None = None
        self.dict_quantiles: dict | None = None

    @abstractmethod
    def build(self, input_shape: tuple[int, ...]) -> tf.keras.Model:
        """Construct and compile the Keras model."""

    def fit(self, train_ds, val_ds, epochs: int = 50, callbacks=None, **kwargs):
        """Train the model, delegating to the underlying Keras model."""
        if self.model is None:
            raise RuntimeError("Call build() before fit()")
        result = self.model.fit(
            train_ds,
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
    def summary(self):
        if self.model is not None:
            self.model.summary()
    def save_model(self, path: str | Path, name: str = "model") -> Path:
        """Save the trained Keras model to disk.

        Creates the directory if it doesn't exist and saves in the native
        Keras format (`.keras`).  Returns the path to the saved file.
        """
        if self.model is None:
            raise RuntimeError("No model to save — call build() and fit() first")
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        model_path = path / f"{name}.keras"
        self.model.save(model_path)
        return model_path