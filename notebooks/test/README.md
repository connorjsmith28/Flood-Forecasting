# notebooks/test/

Model evaluation notebooks. Each notebook loads a saved model, runs inference on the test set, and produces evaluation metrics and visualizations.

## Subfolders

| Folder | Description |
|--------|-------------|
| [gru/](gru/) | GRU evaluation: `running_gru.ipynb`, `running_gru_2xloss.ipynb` |
| [lstm/](lstm/) | LSTM evaluation: `running_lstm.ipynb`, `running_lstm_2xloss.ipynb`, `lstm_shap_by_site.ipynb` |

`running_hybrid.ipynb` (hybrid LSTM-Transformer) lives directly in this folder.

## Common pattern

All evaluation notebooks follow the same structure:
1. Load config and pull data from W&B
2. Run `processor` + `create_sequences` to produce `X_test`, `y_test`
3. Load saved model weights (`.keras` file from `models/`)
4. Instantiate `PostProcessor` and call `evaluate()`, `plot_results()`, `persistence_baseline()`, `compute_shap()`

## Tip: loading models with custom loss

Models trained with `under_predict_penalty > 1` use a custom `asymmetric_mse` loss. Pass it as a `custom_objects` entry when loading:

```python
tmp = GRUModel()
model = GRUModel.load_model("gru_model.keras", custom_objects={"asymmetric_mse": tmp._loss()})
```
