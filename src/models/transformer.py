
"""Transformer flood forecasting model."""

import tensorflow as tf
from tensorflow.keras.layers import (
    Dense,
    Dropout,
    GlobalAveragePooling1D,
    LayerNormalization,
    MultiHeadAttention,
)

from src.models.base import BaseModel


class TransformerBlock(tf.keras.layers.Layer):
    """Single Transformer encoder block: multi-head self-attention + feed-forward."""

    def __init__(self, d_model: int, num_heads: int, ff_dim: int, dropout: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.attn = MultiHeadAttention(num_heads=num_heads, key_dim=d_model)
        self.ffn = tf.keras.Sequential([
            Dense(ff_dim, activation="relu"),
            Dense(d_model),
        ])
        self.norm1 = LayerNormalization()
        self.norm2 = LayerNormalization()
        self.dropout1 = Dropout(dropout)
        self.dropout2 = Dropout(dropout)

    def call(self, x, training=False):
        attn_out = self.attn(x, x)
        x = self.norm1(x + self.dropout1(attn_out, training=training))
        ffn_out = self.ffn(x)
        return self.norm2(x + self.dropout2(ffn_out, training=training))


class TransformerModel(BaseModel):
    """Transformer encoder for time-series flood forecasting.

    Args:
        num_blocks: Number of stacked TransformerBlock layers.
        d_model: Embedding / projection dimension.
        num_heads: Number of attention heads per block.
        ff_dim: Hidden units in each block's feed-forward network.
        dense_units: Units in the dense head before the output layer.
        dropout_rate: Dropout fraction used throughout.
        output_size: Number of output neurons.
        under_predict_penalty: Asymmetric MSE weight (1.0 = standard MSE).
        learning_rate: Adam learning rate.
    """

    def __init__(
        self,
        num_blocks: int = 2,
        d_model: int = 64,
        num_heads: int = 4,
        ff_dim: int = 128,
        dense_units: int = 64,
        dropout_rate: float = 0.1,
        output_size: int = 1,
        under_predict_penalty: float = 2.0,
        learning_rate: float = 1e-3,
    ) -> None:
        super().__init__(under_predict_penalty=under_predict_penalty, learning_rate=learning_rate)
        self.num_blocks = num_blocks
        self.d_model = d_model
        self.num_heads = num_heads
        self.ff_dim = ff_dim
        self.dense_units = dense_units
        self.dropout_rate = dropout_rate
        self.output_size = output_size


    def build(self, input_shape: tuple[int, ...] | None = None) -> tf.keras.Model:
        """Build and compile the Transformer model.

        Args:
            input_shape: ``(window_size, n_features)`` tuple.
        """
        inputs = tf.keras.Input(shape=input_shape)
        x = Dense(self.d_model)(inputs)

        for _ in range(self.num_blocks):
            x = TransformerBlock(
                d_model=self.d_model,
                num_heads=self.num_heads,
                ff_dim=self.ff_dim,
                dropout=self.dropout_rate,
            )(x)

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