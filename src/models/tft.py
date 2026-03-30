"""Temporal Fusion Transformer flood forecasting model."""

import keras
from keras import ops
from keras.layers import (
    Dense,
    Dropout,
    GlobalAveragePooling1D,
    LayerNormalization,
    MultiHeadAttention,
    LSTM,
    Reshape,
)

from src.models.base import BaseModel


class GatedResidualNetwork(keras.layers.Layer):
    """Gated Residual Network: core TFT building block.

    Applies a two-layer dense network gated by a sigmoid and adds a
    residual skip connection with optional projection when input/output
    dimensions differ. LayerNorm is applied after the residual addition.
    Works on both 2-D ``(batch, features)`` and 3-D ``(batch, time, features)``
    tensors — Dense layers apply to the trailing dimension.

    Args:
        units: Output dimension.
        dropout: Dropout fraction applied after the first dense layer.
    """

    def __init__(self, units: int, dropout: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dense1 = Dense(units, activation="elu")
        self.dense2 = Dense(units)
        self.gate = Dense(units, activation="sigmoid")
        self.norm = LayerNormalization()
        self.dropout = Dropout(dropout)
        self._skip_proj: Dense | None = None

    def build(self, input_shape):
        if input_shape[-1] != self.units:
            self._skip_proj = Dense(self.units, use_bias=False)
        super().build(input_shape)

    def call(self, x, training=False):
        residual = self._skip_proj(x) if self._skip_proj is not None else x
        h = self.dense1(x)
        h = self.dropout(h, training=training)
        h = self.dense2(h)
        g = self.gate(h)
        return self.norm(g * h + residual)


class TemporalFusionTransformerModel(BaseModel):
    """Temporal Fusion Transformer for time-series flood forecasting.

    Treats the last ``num_static_features`` columns of the input as
    time-invariant site attributes; the remaining columns are dynamic
    (time-varying) features.  This matches the ordering produced by
    ``processor`` when ``static_cols`` are appended after ``input_cols``.

    Architecture overview
    ---------------------
    1. **Static covariate encoder** (GRN) — compresses static features into
       four context vectors used to condition the rest of the network.
    2. **Dynamic feature projection + GRN** — projects raw dynamic inputs to
       ``d_model`` and enriches them with the static selection context.
    3. **LSTM encoder** — processes the enriched sequence, initialised from
       the static initial-state context vectors.
    4. **Temporal self-attention** (multi-head) — attends over all timesteps
       with a residual connection and LayerNorm.
    5. **GRN enrichment** — a final gated refinement conditioned on the
       static enrichment context.
    6. **Mean pooling → Dense output** — collapses the time dimension and
       produces the scalar streamflow prediction.

    Args:
        num_static_features: Number of time-invariant input columns
            (trailing columns of the input tensor).
        d_model: Core hidden dimension used throughout the model.
        num_heads: Number of attention heads in the self-attention layer.
        lstm_units: Hidden units in the LSTM encoder.
        dropout_rate: Dropout fraction applied in GRNs and attention.
        output_size: Number of output neurons (1 for single-step regression).
        under_predict_penalty: Asymmetric MSE weight (>1 penalises under-predictions).
        learning_rate: Adam learning rate.
    """

    def __init__(
        self,
        num_static_features: int = 14,
        d_model: int = 64,
        num_heads: int = 4,
        lstm_units: int = 64,
        dropout_rate: float = 0.1,
        output_size: int = 1,
        under_predict_penalty: float = 2.0,
        learning_rate: float = 1e-3,
    ) -> None:
        super().__init__(under_predict_penalty=float(under_predict_penalty), learning_rate=learning_rate)
        self.num_static_features = num_static_features
        self.d_model = d_model
        self.num_heads = num_heads
        self.lstm_units = lstm_units
        self.dropout_rate = dropout_rate
        self.output_size = output_size

    def _loss(self):
        """Backend-agnostic asymmetric MSE using keras.ops."""
        if self.under_predict_penalty == 1.0:
            return "mse"
        penalty = self.under_predict_penalty

        def asymmetric_mse(y_true, y_pred):
            error = y_true - y_pred
            weight = ops.where(error > 0.0, penalty, 1.0)
            return ops.mean(weight * ops.square(error))

        return asymmetric_mse

    def build(self, input_shape: tuple[int, ...] | None = None) -> keras.Model:
        """Build and compile the TFT model.

        Args:
            input_shape: ``(window_size, n_features)`` where the last
                ``num_static_features`` columns are time-invariant.
        """
        _window_size, n_features = input_shape
        n_dynamic = n_features - self.num_static_features

        inputs = keras.Input(shape=input_shape, name="inputs")

        # ── Feature splitting ──────────────────────────────────────────────
        dynamic = inputs[:, :, :n_dynamic]     # (batch, time, n_dynamic)
        static_raw = inputs[:, 0, n_dynamic:]  # (batch, n_static) — same across time

        # ── Static covariate encoder ───────────────────────────────────────
        static_enc = GatedResidualNetwork(
            self.d_model, self.dropout_rate, name="static_enc"
        )(static_raw)

        # Four static context vectors condition different parts of the network
        c_selection = Dense(self.d_model, name="c_selection")(static_enc)
        c_enrichment = Dense(self.d_model, name="c_enrichment")(static_enc)
        c_h = Dense(self.lstm_units, name="c_h")(static_enc)
        c_c = Dense(self.lstm_units, name="c_c")(static_enc)

        # ── Dynamic feature projection + context injection ─────────────────
        # Dense applies to the trailing dim → shape: (batch, time, d_model)
        dynamic_proj = Dense(self.d_model, name="dynamic_proj")(dynamic)

        # Reshape to (batch, 1, d_model) so it broadcasts over the time axis
        c_sel_3d = Reshape((1, self.d_model), name="c_sel_expand")(c_selection)
        dynamic_ctx = dynamic_proj + c_sel_3d
        dynamic_selected = GatedResidualNetwork(
            self.d_model, self.dropout_rate, name="dynamic_grn"
        )(dynamic_ctx)

        # ── LSTM encoder conditioned on static initial state ───────────────
        lstm_out = LSTM(
            self.lstm_units,
            return_sequences=True,
            name="lstm_encoder",
        )(dynamic_selected, initial_state=[c_h, c_c])
        lstm_out = Dropout(self.dropout_rate, name="lstm_drop")(lstm_out)

        # Project to d_model for attention
        lstm_proj = Dense(self.d_model, name="lstm_proj")(lstm_out)

        # ── Temporal self-attention ────────────────────────────────────────
        attn_layer = MultiHeadAttention(
            num_heads=self.num_heads,
            key_dim=self.d_model // self.num_heads,
            dropout=self.dropout_rate,
            name="temporal_attn",
        )
        attn_out = attn_layer(lstm_proj, lstm_proj)
        attn_out = LayerNormalization(name="attn_norm")(lstm_proj + attn_out)

        # ── GRN enrichment with static context ────────────────────────────
        c_enr_3d = Reshape((1, self.d_model), name="c_enr_expand")(c_enrichment)
        enriched = GatedResidualNetwork(
            self.d_model, self.dropout_rate, name="enrichment_grn"
        )(attn_out + c_enr_3d)

        # ── Aggregate and predict ──────────────────────────────────────────
        pooled = GlobalAveragePooling1D(name="pooling")(enriched)  # (batch, d_model)
        outputs = Dense(self.output_size, name="output")(pooled)

        self.model = keras.Model(inputs=inputs, outputs=outputs, name="tft")
        self.model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss=self._loss(),
            metrics=["mae"],
        )
        return self.model
