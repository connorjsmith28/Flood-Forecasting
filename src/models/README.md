# src/models/

ML model definitions for flood streamflow forecasting. All models subclass `BaseModel` (base.py) and share a common interface: `build()`, `fit()`, `predict()`, `save_model()`, `load_model()`.

## Models

| File | Model | Framework | Notes |
|------|-------|-----------|-------|
| `base.py` | BaseModel | TF/Keras + PyTorch | Abstract base; asymmetric MSE loss, save/load, flood quantile classification |
| `lstm.py` | LSTMModel | TensorFlow/Keras | Two-layer LSTM with dropout |
| `gru.py` | GRUModel | TensorFlow/Keras | Two-layer GRU (lighter alternative to LSTM) |
| `transformer.py` | TransformerModel | TensorFlow/Keras | Stacked Transformer encoder blocks |
| `lstm_transformer.py` | LSTMTransformer | TensorFlow/Keras | LSTM encoder → single TransformerBlock |
| `tft.py` | TemporalFusionTransformerModel | Keras 3 | Full TFT with GRNs, static context, LSTM encoder, multi-head attention |

## Asymmetric MSE Loss

All models support an `under_predict_penalty` (default: 2.0) that penalises under-predictions more heavily than over-predictions. This is important for flood forecasting: missing a flood is more costly than a false alarm. Set to 1.0 for standard MSE.

## Usage

```python
from src.models.gru import GRUModel

model = GRUModel(gru_units=(64, 32), dropout_rate=0.3, under_predict_penalty=2.0)
model.build(input_shape=(72, 28))
model.fit(train_ds=(X_train, y_train), val_ds=(X_val, y_val), epochs=50)
model.save_model(name="gru_v1")
```

Loading a saved model requires passing the custom loss as a `custom_objects` entry:

```python
tmp = GRUModel()
model = GRUModel.load_model("gru_v1.keras", custom_objects={"asymmetric_mse": tmp._loss()})
```
