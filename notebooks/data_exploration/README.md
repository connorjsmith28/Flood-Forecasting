# notebooks/data_exploration/

Exploratory analysis notebooks for understanding the Missouri Basin dataset.

## Notebooks

| Notebook | Description |
|----------|-------------|
| `data_exploration.ipynb` | General EDA: distributions, missing data, site counts |
| `variables_over_time.ipynb` | Temporal trends in streamflow and forcing variables |
| `usgs_site_coverage.ipynb` | Map of USGS gauge coverage and IV vs daily availability |
| `gauge_attributes.ipynb` | Watershed static attribute distributions (GAGES-II, HydroATLAS) |
| `peak_streamflow_info.ipynb` | Peak flow statistics and return period estimates |
| `Log_Pearson3_Flooding.ipynb` | Log-Pearson Type III flood frequency analysis per site |
| `flood_research.ipynb` | Background literature on flood definitions and thresholds |
| `big_correlation.ipynb` / `corelation_matrix.ipynb` | Feature correlation heatmaps |
| `site_filtering.ipynb` | Site selection logic (drainage area, record length) |
| `top_30_filtering.ipynb` | Filtering to the 30 most data-rich sites |
| `top_37_upstream_pair.ipynb` | Upstream/downstream gauge pair analysis |
| `live_site_data_viz.ipynb` | Real-time data visualization for individual sites |
| `info_summary.ipynb` | Summary statistics for the full dataset |
| `test_artifacts.ipynb` | Validation checks on W&B artifact contents |

## Support scripts

- `correlation.py` — Plotly correlation heatmap from a W&B artifact
- `wandb_artifact_stats.py` — Prints artifact metadata (rows, columns, size) without downloading
