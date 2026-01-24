# Extraction Data Sources

This folder contains scripts for extracting data from various hydrological and meteorological APIs. Each data source serves a specific purpose in the flood forecasting pipeline.

## Data Sources

### USGS Streamflow (`usgs.py`)
- **Source**: USGS National Water Information System (NWIS)
- **Data**: Instantaneous streamflow measurements (15-minute intervals)
- **Key identifier**: USGS site ID (e.g., "01010000")
- **Functions**:
  - `fetch_usgs_streamflow()` - Retrieves discharge data in cubic feet per second
  - `get_site_info()` - Retrieves site metadata (location, drainage area, HUC code)

### Weather Forcing (`weather.py`)
- **Source**: Open-Meteo Historical Weather API
- **Data**: Hourly meteorological variables (precipitation, temperature, humidity, wind, radiation)
- **Key identifier**: Longitude/latitude coordinates
- **Functions**:
  - `fetch_weather_forcing()` - Retrieves historical weather data for given coordinates

> **Note**: Open-Meteo also provides a forecast API (`api.open-meteo.com/v1/forecast`) for predictive weather data. This is not currently implemented because we lack predictive data for the other sources (streamflow, basin characteristics), so forecast weather data would be orphaned. 

### GAGES-II / CAMELS Attributes (`gages.py`)
- **Source**: USGS GAGES-II dataset via pygeohydro
- **Data**: Static catchment attributes (geology, soils, climate normals, land cover)
- **Key identifier**: USGS site ID
- **Functions**:
  - `get_sites_in_huc()` - Finds all USGS sites within a HUC region
  - `fetch_gages_attributes()` - Retrieves CAMELS catchment attributes

### NLDI Basin Characteristics (`nldi_attributes.py`)
- **Source**: Network-Linked Data Index (NLDI) via PyNHD
- **Data**: Basin characteristics from NHDPlus (elevation, slope, land cover percentages)
- **Key identifier**: NHDPlus ComID (resolved from USGS site ID)
- **Functions**:
  - `fetch_nldi_characteristics_batch()` - Retrieves basin characteristics for multiple sites

## How They Fit Together

```
USGS Sites (site_id)
       │
       ├──► usgs.py ──► Streamflow time series (target variable)
       │
       ├──► weather.py ──► Weather forcing time series (input features)
       │    (uses site coordinates)
       │
       ├──► gages.py ──► Static catchment attributes (input features)
       │
       └──► nldi_attributes.py ──► Basin characteristics (input features)
            (resolves site_id → ComID)
```

The USGS site ID is the primary key that links all data sources together:
1. **Streamflow** is the prediction target - what we're trying to forecast
2. **Weather** provides the meteorological forcing that drives streamflow response
3. **GAGES-II/CAMELS** and **NLDI** attributes describe the physical characteristics of each basin, explaining why different basins respond differently to the same weather inputs
