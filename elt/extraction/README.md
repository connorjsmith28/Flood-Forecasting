# Extraction Data Sources

This folder contains scripts for extracting data from hydrological and meteorological APIs. Static catchment attributes (GAGES-II, HydroATLAS, NLDAS climate) are provided via dbt seeds rather than API extraction.

## Data Sources

### USGS Streamflow & Sites (`usgs.py`)
- **Source**: USGS National Water Information System (NWIS) https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/continuous
- **Data**: Site metadata and streamflow measurements
- **Key identifier**: USGS site ID (e.g., "01010000") or HUC code for site discovery
- **Temporal resolution**:
  - **15-minute (IV)**: Instantaneous values, highest resolution. ~25% of sites have IV data.
  - **Daily (DV)**: Daily mean values, broader coverage. ~75% of sites have daily data.
- **Functions**:
  - `get_site_metadata()` - Discovers sites in a HUC region and retrieves metadata (location, drainage area, HUC code). Returns `has_iv` and `has_daily` flags.
  - `fetch_usgs_streamflow()` - Retrieves 15-minute discharge data (cfs)
  - `fetch_usgs_daily()` - Retrieves daily mean discharge data (cfs)

### Weather Forcing (`weather.py`)
- **Source**: Open-Meteo Historical Weather API. https://open-meteo.com/en/docs/historical-weather-api
- **Data**: Meteorological variables (precipitation, temperature, humidity, wind, radiation, pressure, evapotranspiration)
- **Key identifier**: Longitude/latitude coordinates (matched to USGS site locations)
- **Temporal resolution**: **Hourly only**. The historical archive API does not support sub-hourly data. The forecast API supports 15-minute for North America (HRRR model), but historical is limited to hourly.
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

Transformation (dbt):
Seeds + raw tables → staging views → mart tables → flood_model
```

The USGS site ID is the primary key that links all data sources together:
1. **Streamflow** is the prediction target - what we're trying to forecast
2. **Weather** provides the meteorological forcing that drives streamflow response
3. **Static attributes** (from seeds) describe the physical characteristics of each basin, explaining why different basins respond differently to the same weather inputs

## Raw Data Summary

| Source | Table | Resolution | Coverage |
|--------|-------|------------|----------|
| USGS | `raw.site_metadata` | - | All sites with discharge data in HUC region |
| USGS | `raw.streamflow_15min` | 15-minute | ~25% of sites (those with `has_iv=true`) |
| USGS | `raw.streamflow_daily` | Daily | ~75% of sites (those with `has_daily=true`) |
| Open-Meteo | `raw.weather_forcing` | Hourly | All sites (by lat/lon lookup) |

> **Resolution mismatch**: Weather data is hourly while streamflow can be 15-minute or daily. For ML training, either aggregate 15-min streamflow to hourly, or interpolate weather to 15-min (less ideal since weather doesn't actually vary that fast in most cases).
