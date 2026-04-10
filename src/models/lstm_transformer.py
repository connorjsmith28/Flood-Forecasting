"""Hybrid LSTM-Transformer flood forecasting model."""

import tensorflow as tf
from tensorflow.keras.layers import LSTM, Dense, Dropout, GlobalAveragePooling1D
from .transformer import TransformerBlock
from src.models.base import BaseModel


class LSTMTransformer(BaseModel):
    """LSTM encoder feeding a single Transformer block for flood forecasting.

    Args:
        lstm_units: Tuple of units for the two LSTM layers.
        d_model: Transformer embedding dimension (projection target).
        num_heads: Number of attention heads in the TransformerBlock.
        ff_dim: Feed-forward hidden dim inside the TransformerBlock.
        dense_units: Units in the hidden Dense layer before output.
        dropout_rate: Dropout fraction used throughout.
        output_size: Number of output neurons (1 for single-site prediction).
        under_predict_penalty: Asymmetric MSE weight (1.0 = standard MSE).
        learning_rate: Adam learning rate.
    """

    def __init__(
        self,
        lstm_units: tuple[int, int] = (64, 64),
        d_model: int = 32,
        num_heads: int = 2,
        ff_dim: int = 32,
        dense_units: int = 32,
        dropout_rate: float = 0.3,
        output_size: int = 1,
        under_predict_penalty: float = 2.0,
        learning_rate: float = 1e-3,
    ) -> None:
        super().__init__(under_predict_penalty=under_predict_penalty, learning_rate=learning_rate)
        self.lstm_units = lstm_units
        self.d_model = d_model
        self.num_heads = num_heads
        self.ff_dim = ff_dim
        self.dense_units = dense_units
        self.dropout_rate = dropout_rate
        self.output_size = output_size

    def build(self, input_shape: tuple[int, ...] | None = None) -> tf.keras.Model:
        """Build and compile the hybrid model.

        Args:
            input_shape: Optional ``(window_size, n_features)`` tuple.
        """
        inputs = tf.keras.Input(shape=input_shape)

        x = LSTM(
            self.lstm_units[0],
            return_sequences=True,
            dropout=self.dropout_rate,
            recurrent_dropout=0.1,
        )(inputs)

        x = LSTM(
            self.lstm_units[1],
            return_sequences=True,  # keep sequences for transformer
            dropout=self.dropout_rate,
            recurrent_dropout=0.0,
        )(x)

        x = Dense(self.d_model)(x)

        x = TransformerBlock(
            d_model=self.d_model,
            num_heads=self.num_heads,
            ff_dim=self.ff_dim,
            dropout=self.dropout_rate,
        )(x, training=None) 

        x = GlobalAveragePooling1D()(x)

        x = Dense(self.dense_units, activation="relu")(x)
        x = Dropout(self.dropout_rate)(x)
        outputs = Dense(self.output_size)(x)

        self.model = tf.keras.Model(inputs=inputs, outputs=outputs)
        self.model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss=self._loss(),
            metrics=["mae"],
        )
        return self.model