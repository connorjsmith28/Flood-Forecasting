# notebooks/demo/

Demonstration notebooks showcasing the flood forecasting pipeline end-to-end.

## Notebooks

| Notebook | Description |
|----------|-------------|
| `demo.ipynb` | End-to-end demo: Load a trained model, preprocess data for a specific site, and run inference to predict flood probabilities. |

## Artifacts

- `artifacts/flood-dataset-top30-v0/` — Sample W&B artifact containing preprocessed data for the top 30 flood-prone sites, used in the demo.

## Usage

The `demo.ipynb` notebook demonstrates how to use the trained flood forecasting models for inference on new or historical data. Follow these steps to run the demo:

1. **Set up the environment:**
   ```bash
   uv sync
   just setup  # Ensure data is extracted and transformed
   uv run wandb login  # Authenticate with Weights & Biases
   ```

2. **Open the notebook:**
   - Launch Jupyter or open in VS Code.
   - The notebook is pre-configured for site ID "06820500" (a gauge in the Missouri Basin).

3. **Run the cells step-by-step:**
   - **Cell 1:** Imports required modules from `src/` (preprocessing, model, utils, postprocessing).
   - **Cell 2:** Defines static and dynamic features, window size, and configuration (including site ID, date range for inference, and W&B artifact details).
   - **Cell 3:** Initializes the data processor and pulls the dataset from W&B.
   - **Cell 4:** Loads the pre-trained LSTM model (`lstm_model_2xloss.keras`) with custom loss function.
   - **Cell 5:** Prepares inference data by fetching and processing the specified date range.
   - **Cell 6:** Creates input sequences, filters for the target site, runs model predictions, unscales the outputs, classifies flood risk levels, and displays results in a Polars DataFrame.

4. **Interpret the results:**
   - The output DataFrame shows `forecast_at` (timestamp), `predicted_cfs` (predicted streamflow in cfs), and flood classification columns (e.g., flood risk levels based on thresholds).
   - This demonstrates real-time or historical flood prediction for the selected site.

5. **Customize for your use:**
   - Change `SITE_ID` to another gauge ID from the top 30 sites.
   - Modify the `start_date` and `end_date` in the config to analyze different time periods.
   - Swap the model file to test different architectures (e.g., `gru_model.keras`).
   - Adjust features or window size for experimentation.

## Prerequisites

- Wandb login: `uv run wandb login`
- Access to the flood-dataset-top30 artifact in W&B.
- Pre-trained model files in the `models/` directory (e.g., `lstm_model_2xloss.keras`).