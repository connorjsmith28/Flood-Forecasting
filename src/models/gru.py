"""GRU flood forecasting model."""

import tensorflow as tf
from tensorflow.keras.layers import GRU, Dense, Dropout
from tensorflow.keras.models import Sequential

from src.models.base import BaseModel


class GRUModel(BaseModel):
    """Two-layer GRU with optional asymmetric MSE loss.

    Args:
        gru_units: Tuple of units for the two GRU layers.
        dense_units: Units in the hidden Dense layer before the output.
        dropout_rate: Dropout fraction after each GRU layer.
        output_size: Number of output neurons (1 for single-site prediction).
        under_predict_penalty: When > 1, under-predictions are penalised
            more heavily (asymmetric MSE).  Set to 1.0 for standard MSE.
        learning_rate: Adam learning rate.
    """

    def __init__(
        self,
        gru_units: tuple[int, int] = (32, 16),
        dense_units: int = 64,
        dropout_rate: float = 0.3,
        output_size: int = 1,
        under_predict_penalty: float = 2.0,
        learning_rate: float = 1e-3,
    ) -> None:
        super().__init__()
        self.gru_units = gru_units
        self.dense_units = dense_units
        self.dropout_rate = dropout_rate
        self.output_size = output_size
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

    def build(self, input_shape: tuple[int, ...] | None = None) -> tf.keras.Model:
        """Build and compile the GRU model.

        Args:
            input_shape: Optional ``(window_size, n_features)`` tuple.
                         If omitted the first layer defers shape inference.
        """
        layers = []

        if input_shape is not None:
            layers.append(GRU(self.gru_units[0], return_sequences=True, input_shape=input_shape))
        else:
            layers.append(GRU(self.gru_units[0], return_sequences=True))

        layers += [
            Dropout(self.dropout_rate),
            GRU(self.gru_units[1], return_sequences=False),
            Dropout(self.dropout_rate),
            Dense(self.dense_units),
            Dense(self.output_size),
        ]

        self.model = Sequential(layers)
        self.model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss=self._loss(),
            metrics=["mae"],
        )
        return self.model
