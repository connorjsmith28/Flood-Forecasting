# models/

Saved model weights produced by training notebooks.

## File formats

| Extension | Framework | Saved by |
|-----------|-----------|----------|
| `.keras` | TensorFlow/Keras | `BaseModel.save_model()` |
| `.pt` | PyTorch | `BaseModel.save_model()` |
| `preprocessors/*.pkl` | joblib | `processor.save()` |

## Loading a model

```python
from src.models.gru import GRUModel

tmp = GRUModel()
model = GRUModel.load_model("gru_model.keras", custom_objects={"asymmetric_mse": tmp._loss()})
```

Models trained with standard MSE (`under_predict_penalty=1.0`) do not need `custom_objects`.

## Loading a preprocessor

```python
from src.preprocessing.preprocessing import processor

pcr = processor.load("preprocessor_name")   # loads from models/preprocessors/
```

## Note

This folder is not version-controlled for large weight files. Models are tracked as W&B artifacts. Use `just dagster` and the `wandb_dataset` asset to upload/download.
