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

### NLDAS Forcing (`nldas3.py`)
- **Source**: NLDAS-2 V2.0 (NLDAS_FORA0125_H) from NASA GES DISC
- **Data**: Hourly meteorological forcing per site
- **Temporal resolution**: **Hourly** (individual NetCDF files per timestep, ~5 MB each)
- **Spatial resolution**: 0.125 degree (~12 km), matching CAMELS-H methodology
- **Coverage**: 1979-present (we use 2001+), full CONUS grid
- **Auth**: Earthdata Login credentials in `~/.netrc` + GES DISC EULA
- **Variables**: 11 CAMELS-H forcing variables: Tair, Qair, PSurf, Wind_E, Wind_N, SWdown, LWdown, Rainf, CRainf_frac, CAPE, PotEvap
- **Functions**:
  - `open_nldas2_file()` - Opens a local NetCDF file and clips to Missouri Basin
  - `aggregate_to_watersheds()` - Computes weighted averages using grid weights
  - `fetch_nldas3_forcing()` - Searches, downloads, and processes files via earthaccess
  - `convert_units()` - Converts temperature K to C (other variables already in target units)

**Data source evolution:**
1. **NLDAS-2 GRIB** (GrADS/DODS) - Original CAMELS-H source, decommissioned
2. **Open-Meteo** (`weather.py`) - Point-based API, 10k/day rate limits
3. **NLDAS-3 S3** (beta) - 12 GB files, data only through Dec 2023, wrong path templates
4. **NLDAS-2 V2.0 NetCDF** (current) - Active, 1979-present, ~5 MB/file, earthaccess download

### Watershed Grid Mapping (`watershed_mapping.py`)
- **Source**: USGS NLDI basin delineation (via `pynhd`) + NLDAS 0.125 degree grid
- **Purpose**: Maps each site to NLDAS grid cells for forcing data extraction
- **Output**: `raw.nldas3_watershed_mapping` table with (site_id, grid_row, grid_col, area_weight)
- **Functions**:
  - `fetch_nldi_basins()` - Fetches watershed boundaries from USGS NLDI
  - `compute_watershed_grid_weights()` - Computes area-weighted grid cell fractions per watershed
  - `generate_point_weights()` - Maps site lat/lon to nearest grid cell (weight=1.0)

**Current coverage strategy:**
- **~45% of sites** (~1,560): NLDI returns watershed boundary polygons. These get proper area-weighted averages across all overlapping grid cells (multiple cells, fractional weights).
- **~55% of sites** (~1,870): NLDI has no boundary. These fall back to the single nearest NLDAS grid cell at the site's lat/lon (one cell, weight=1.0). This is a point-based approximation, not a true watershed average.

> **Future work — HydroBasins upstream tracing**: To get full watershed-averaged coverage for all sites, implement the CAMELS-H methodology: download HydroBasins Level 12 polygons, find the sub-basin containing each gauge, trace upstream via the `NEXT_DOWN` connectivity field, and union into a single watershed polygon per site. This approach covers any point on the global river network and would eliminate the point-based fallback. See `findwatershed_correct2.m` in the [CAMELS-H repo](https://github.com/vinhngoctran/CAMELSH/tree/main/functions) for reference.

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
| NLDAS-2 | `raw.nldas3_forcing` | Hourly | All sites (~45% watershed-avg, ~55% nearest cell) |
| NLDAS-2 | `raw.nldas3_watershed_mapping` | - | Grid weights per site (static, computed once) |

> **Resolution mismatch**: Weather data is hourly while streamflow can be 15-minute or daily. For ML training, either aggregate 15-min streamflow to hourly, or interpolate weather to 15-min (less ideal since weather doesn't actually vary that fast in most cases).
