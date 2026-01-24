# Extraction Data Sources

This folder contains scripts for extracting data from hydrological and meteorological APIs. Static catchment attributes (GAGES-II, HydroATLAS, NLDAS climate) are provided via dbt seeds rather than API extraction.

## Data Sources

### USGS Streamflow & Sites (`usgs.py`)
- **Source**: USGS National Water Information System (NWIS) https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/continuous
- **Data**: Site metadata and instantaneous streamflow measurements (15-minute intervals)
- **Key identifier**: USGS site ID (e.g., "01010000") or HUC code for site discovery
- **Functions**:
  - `get_sites_by_huc()` - Finds all USGS stream sites within a HUC region
  - `get_site_info()` - Retrieves site metadata (location, drainage area, HUC code)
  - `fetch_usgs_streamflow()` - Retrieves discharge data in cubic feet per second

### Weather Forcing (`weather.py`)
- **Source**: Open-Meteo Historical Weather API. https://open-meteo.com/en/docs/historical-weather-api
- **Data**: Hourly meteorological variables (precipitation, temperature, humidity, wind, radiation).
- **Key identifier**: Longitude/latitude coordinates
- **Functions**:
  - `fetch_weather_forcing()` - Retrieves historical weather data for given coordinates

> **Note**: Open-Meteo also provides a forecast API (`api.open-meteo.com/v1/forecast`) for predictive weather data. This is not currently implemented because we lack predictive data for the other sources (streamflow, basin characteristics), so forecast weather data would be orphaned.

## Static Attributes (via dbt seeds)

The following static catchment attributes are provided via CSV seeds in `elt/transformation/seeds/` rather than API extraction:

- **GAGES-II**: 439 attributes covering geology, soils, climate normals, land cover, topography
- **HydroATLAS**: 195+ catchment attributes from the global HydroATLAS dataset
- **NLDAS-2 Climate**: CAMELS-style climate indices (aridity, precipitation seasonality, snow fraction)

## How They Fit Together

```
Extraction Pipeline (Dagster):
usgs_site_metadata(huc="10") → usgs_streamflow_raw → weather_forcing_raw
         │                              │                    │
         └── site_metadata ─────────────┴── coordinates ─────┘

Transformation (dbt):
Seeds + raw tables → staging views → mart tables → flood_model
```

The USGS site ID is the primary key that links all data sources together:
1. **Streamflow** is the prediction target - what we're trying to forecast
2. **Weather** provides the meteorological forcing that drives streamflow response
3. **Static attributes** (from seeds) describe the physical characteristics of each basin, explaining why different basins respond differently to the same weather inputs
