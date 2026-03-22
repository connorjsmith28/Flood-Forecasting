"""hybrid flood forecasting model."""

import tensorflow as tf
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.models import Sequential
from .transformer import TransformerBlock
from src.models.base import BaseModel


class LSTMTransformer(BaseModel):
    """Two-layer LSTM with optional asymmetric MSE loss.

    Args:
        lstm_units: Tuple of units for the two LSTM layers.
        dense_units: Units in the hidden Dense layer before the output.
        dropout_rate: Dropout fraction after each LSTM layer.
        output_size: Number of output neurons (1 for single-site prediction).
        under_predict_penalty: When > 1, under-predictions are penalised
            more heavily (asymmetric MSE). Set to 1.0 for standard MSE.
        learning_rate: Adam learning rate.
    """

    def __init__(
        self,
        lstm_units: tuple[int, int] = (32, 16),
        transformer_units: tuple[int, int] = (32, 16),
        dense_units: int = 64,
        dropout_rate: float = 0.3,
        output_size: int = 1,
        under_predict_penalty: float = 2.0,
        learning_rate: float = 1e-3,
    ) -> None:
        super().__init__(under_predict_penalty=under_predict_penalty, learning_rate=learning_rate)
        self.lstm_units = lstm_units
        self.dense_units = dense_units
        self.dropout_rate = dropout_rate
        self.output_size = output_size
        self.transformer_units = transformer_units

    def build(self, input_shape: tuple[int, ...] | None = None) -> tf.keras.Model:
        """Build and compile the LSTM model.

        Args:
            input_shape: Optional ``(window_size, n_features)`` tuple.
                         If omitted the first layer defers shape inference.
        """

        layers = []

        # First LSTM layer (returns sequences for transformer input)
        if input_shape is not None:
            layers.append(LSTM(self.lstm_units[0], return_sequences=True, input_shape=input_shape))
        else:
            layers.append(LSTM(self.lstm_units[0], return_sequences=True))
        layers.append(Dropout(self.dropout_rate))


        # Second LSTM layer (returns sequences for transformer input)
        layers.append(LSTM(self.lstm_units[1], return_sequences=True))
        layers.append(Dropout(self.dropout_rate))

        # Project to transformer d_model dimension
        layers.append(Dense(self.transformer_units[0]))

        # Add two TransformerBlock layers
        for _ in range(2):
            layers.append(
                TransformerBlock(
                    d_model=self.transformer_units[0],
                    num_heads=2,
                    ff_dim=self.transformer_units[1],
                    dropout=self.dropout_rate,
                )
            )

        # Pooling to flatten sequence for dense layers
        from tensorflow.keras.layers import GlobalAveragePooling1D
        layers.append(GlobalAveragePooling1D())

        # Dense layers
        layers.append(Dense(self.dense_units))
        layers.append(Dense(self.output_size))

        self.model = Sequential(layers)
        self.model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss=self._loss(),
            metrics=["mae"],
        )
        return self.model