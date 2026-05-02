# src/static/

This directory contains static data files used throughout the flood forecasting pipeline. These are precomputed or reference datasets that don't change frequently.

## Files

### flooding_events.csv
Historical flooding events data with classifications.
- **Columns**:
  - `site_id`: USGS gauge site identifier
  - `observed_date`: Date of the flooding event
  - `streamflow_cfs_max`: Maximum streamflow in cubic feet per second during the event
  - `classification`: Flood return period classification (e.g., Q2, Q10)
- **Source**: Derived from historical data analysis and Log-Pearson Type III flood frequency analysis.

### lp3_results.csv
Log-Pearson Type III flood frequency analysis results for each site.
- **Columns** include:
  - Site metadata: `site_id`, `state`, `n_years`, `latitude`, `longitude`, `elevation_m`
  - LP3 parameters: `mean_log`, `std_log`, `station_skew`, `regional_skew`, `weighted_skew`
  - Thresholds: `pilf_threshold_cfs`, quantile flows (Q2_cfs to Q100_cfs)
  - Flood counts: Number of floods exceeding each quantile threshold
  - `flood_severity_score`: Computed severity metric for the site
- **Purpose**: Used for flood classification and threshold determination in postprocessing.

### top_site_quantile_thresholds.json
Quantile-based flood thresholds for the top-priority sites.
- **Structure**: JSON object with site IDs as keys, each containing quantile thresholds in cfs (Q2, Q5, Q10, Q25, Q50, Q100).
- **Usage**: Defines flood risk levels for classification in the FloodClassifier.

### upstream_pair_dict.json
Mapping of downstream to upstream gauge site pairs.
- **Structure**: JSON object where keys are downstream site IDs, values are upstream site IDs (or null if no upstream pair).
- **Purpose**: Used in analysis of upstream/downstream relationships and for potential cascade flood modeling.

## Usage

These files are loaded by modules in `src/`:

- `flooding_events.csv` and `lp3_results.csv`: Used in data exploration and for validating flood classifications.
- `top_site_quantile_thresholds.json`: Loaded by `src/postprocessing/inference.py` for flood risk classification.
- `upstream_pair_dict.json`: Referenced in notebooks for upstream/downstream pair analysis.

## Generation

These files are typically generated from data processing pipelines:

- LP3 results from flood frequency analysis (see `notebooks/data_exploration/Log_Pearson3_Flooding.ipynb`)
- Quantile thresholds derived from LP3 results
- Upstream pairs from site selection and geographic analysis
- Flooding events from historical peak flow data

If you need to regenerate these files, run the relevant data exploration notebooks or pipeline steps.