# src/postprocessing/

Post-training evaluation, interpretation, and inference utilities.

## Files

### `postprocessing.py` — `PostProcessor`

Per-site evaluation after training. Takes a trained model, test arrays, and a fitted scaler.

```python
from src.postprocessing.postprocessing import PostProcessor

pp = PostProcessor(
    model=model,
    X_test=X_test,           # (n_samples, timesteps, n_features)
    y_test=y_test,           # (n_samples,) scaled
    site_ids=test_site_ids,
    target_scaler=pcr.target_scaler,
    feature_names=feature_names,
)

pp.evaluate()            # per-site MAE + flood quantile accuracy
pp.plot_results()        # predicted vs actual CFS
pp.persistence_baseline()  # model vs naive persistence (NSE, RMSE, MAE)
pp.compute_shap()        # KernelSHAP: beeswarm, waterfall, temporal heatmap
pp.compute_shap_per_site()  # per-site SHAP importance table + heatmap
```

**Note on SHAP:** KernelSHAP is slow. Use `nsamples=50` for exploration, `nsamples=200` for reporting. The method flattens the 3D input to 2D for SHAP compatibility and re-wraps it internally.

### `inference.py` — `FloodClassifier`

Classifies model predictions against return-period thresholds (Q2–Q100 CFS) loaded from `src/static/top_site_quantile_thesholds.json`.
