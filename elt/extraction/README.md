# Extraction Data Sources

This folder contains scripts for extracting data from various hydrological and meteorological APIs. Each data source serves a specific purpose in the flood forecasting pipeline.

## Data Sources

### USGS Streamflow (`usgs.py`)
- **Source**: USGS National Water Information System (NWIS) https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/continuous
- **Data**: Instantaneous streamflow measurements (15-minute intervals)
- **Key identifier**: USGS site ID (e.g., "01010000")
- **Functions**:
  - `fetch_usgs_streamflow()` - Retrieves discharge data in cubic feet per second
  - `get_site_info()` - Retrieves site metadata (location, drainage area, HUC code)

### Weather Forcing (`weather.py`)
- **Source**: Open-Meteo Historical Weather API. https://open-meteo.com/en/docs/historical-weather-api
- **Data**: Hourly meteorological variables (precipitation, temperature, humidity, wind, radiation). 
- **Key identifier**: Longitude/latitude coordinates
- **Functions**:
  - `fetch_weather_forcing()` - Retrieves historical weather data for given coordinates

> **Note**: Open-Meteo also provides a forecast API (`api.open-meteo.com/v1/forecast`) for predictive weather data. This is not currently implemented because we lack predictive data for the other sources (streamflow, basin characteristics), so forecast weather data would be orphaned.

- **What NLDAS-2 provides**
| Description | Name | Unit |
|-------------|------|------|
| Air temperature at 2 meters above the surface | Tair | °C |
| 2-meter above ground Specific humidity | Qair | kg/kg |
| Surface pressure | Psurf | Pa |
| 10-meter above ground Zonal wind speed | Wind_E | m/s |
| 10-meter above ground Meridional wind speed | Wind_N | m/s |
| Shortwave radiation flux downwards (surface) | SWdown | W/m² |
| Fraction of total precipitation that is convective | CRainf_frac | Fraction |
| Convective available potential energy | CAPE | J/kg |
| Potential evaporation | PotEvap | kg/m² |
| Hourly total precipitation | total_precipitation | kg/m² |
| Longwave radiation flux downwards (surface) | LWdown | W/m² |

### GAGES-II / CAMELS Attributes (`gages.py`)
- **Source**: USGS GAGES-II dataset via [pygeohydro](https://docs.hyriver.io/readme/pygeohydro.html)
- **Data**: Static catchment attributes (geology, soils, climate normals, land cover)
- **Key identifier**: USGS site ID
- **Functions**:
  - `get_sites_in_huc()` - Finds all USGS sites within a HUC region
  - `fetch_gages_attributes()` - Retrieves CAMELS catchment attributes
- **Paper description**
  `Basin selection. CAMELSH uses the basins from the Geospatial Attributes of Gages for Evaluating
Streamflow (GAGES-II) database 38 , which contains geospatial information for over 9,322 stream gages across
the USGS network, including CONUS, Alaska, and island territories. Our dataset prioritizes diversity in terms of
basin size, climate, topographical, geographical, geology, and human interventions that affect stream flow gen-
eration mechanisms, without imposing additional selection criteria. The only constraint is the geographical lim-
itation to CONUS (Contiguous United States) basins due to the spatial coverage of our meteorological forcing
data. This results in 9,008 basins in CAMELSH. All catchment shapefiles have been standardized to the WGS84
coordinate system.`

### NLDI Basin Characteristics (`nldi_attributes.py`)
- **Source**: Network-Linked Data Index (NLDI) via [PyNHD](https://docs.hyriver.io/readme/pynhd.html)
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
