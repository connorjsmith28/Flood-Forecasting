# NLDAS-3 Watershed-Averaged Meteorological Data Extractor

## Overview
Replace Open-Meteo point-based weather extraction with NLDAS-3 watershed-averaged forcing data, matching the CAMELS-H methodology for scientific rigor in flood forecasting.

## Key Changes
- **Data source**: Open-Meteo API → NLDAS-3 on AWS S3 (NetCDF files)
- **Aggregation**: Point-based (gage location) → Watershed-averaged (area-weighted)
- **Join key**: `(longitude, latitude, datetime)` → `(site_id, datetime)`

---

## Phase 1: Data Acquisition & Dependencies

### 1.1 Download GAGES-II Watershed Boundaries
- Source: https://doi.org/10.5066/P96CPHOT (USGS Science Data Catalog)
- Store in: `data/gagesii/boundaries/` (gitignored)
- Key field: `STAID` maps to USGS site_id

### 1.2 Add Python Dependencies
```
geopandas>=1.0.0      # Shapefile handling
xarray>=2024.0.0      # NetCDF reading
netcdf4>=1.6.0        # NetCDF backend
rioxarray>=0.15.0     # Raster/xarray integration
s3fs>=2024.0.0        # S3 filesystem access
```

---

## Phase 2: Grid-to-Watershed Mapping

### 2.1 New file: `elt/extraction/watershed_mapping.py`

```python
def load_gagesii_boundaries(shapefile_path, site_ids=None) -> gpd.GeoDataFrame
def get_nldas3_grid() -> tuple[lat, lon, transform]
def compute_watershed_grid_mapping(watersheds, transform, shape) -> dict[site_id, list[(row, col, weight)]]
def save_watershed_mapping(mapping, output_path)
def load_watershed_mapping(mapping_path) -> dict
```

### 2.2 New asset: `orchestration/assets/watershed_mapping.py`

```python
@asset(group_name="extraction")
def nldas3_watershed_mapping(context, config, duckdb) -> MaterializeResult:
    """Pre-compute grid-to-watershed mapping (one-time, recompute when sites change)"""
```

### 2.3 New table: `raw.nldas3_watershed_mapping`
```sql
(site_id VARCHAR, grid_row INT, grid_col INT, area_weight DOUBLE)
```

---

## Phase 3: NLDAS-3 Extraction

### 3.1 New file: `elt/extraction/nldas3.py`

```python
def download_nldas3_hourly(date, variables, cache_dir) -> xr.Dataset
def aggregate_to_watershed(ds, watershed_mapping, variables) -> pd.DataFrame
def fetch_nldas3_forcing_parallel(site_ids, start_date, end_date, mapping, ...) -> pl.DataFrame
```

**NLDAS-3 access:**
- Bucket: `s3://nasa-waterinsight/NLDAS3/forcing/hourly/[YYYYMM]/`
- Format: NetCDF, `--no-sign-request` (public)
- Resolution: 1km spatial, hourly temporal

### 3.2 New config: `orchestration/configs/extraction.py`

```python
class NLDAS3Config(ExtractionConfig):
    days_back: int = 7300
    min_date: str = "2007-10-01"
    s3_bucket: str = "nasa-waterinsight"
    cache_dir: str = ".cache/nldas3"
    variables: list[str] = ["Tair", "Qair", "Psurf", "Wind_E", "Wind_N", "SWdown", "LWdown", "Rainf"]
    gagesii_shapefile: str = "data/gagesii/boundaries/bas_nonref_COMIDs.shp"
```

### 3.3 New asset: `orchestration/assets/nldas3_forcing.py`

```python
@asset(group_name="extraction", deps=["usgs_streamflow_15min", "nldas3_watershed_mapping"])
def nldas3_forcing_raw(context, config, duckdb) -> MaterializeResult:
    """Extract NLDAS-3 forcing data aggregated to watershed level (incremental)"""
```

### 3.4 New table: `raw.nldas3_forcing`
```sql
(site_id VARCHAR, datetime TIMESTAMP, air_temp_c DOUBLE, precipitation_mm DOUBLE,
 specific_humidity DOUBLE, surface_pressure_pa DOUBLE, wind_u_ms DOUBLE, wind_v_ms DOUBLE,
 shortwave_radiation_wm2 DOUBLE, longwave_radiation_wm2 DOUBLE, extracted_at TIMESTAMP)
```

---

## Phase 4: dbt Model Updates

### 4.1 New staging: `elt/transformation/models/staging/stg_nldas3_weather.sql`
```sql
select site_id, datetime as observed_at, air_temp_c as temperature_c, precipitation_mm, ...
from {{ source('raw', 'nldas3_forcing') }}
```

### 4.2 Modify: `elt/transformation/models/marts/fct_streamflow_hourly.sql`
```sql
-- Change from:
left join weather as w on sf.longitude = w.longitude and sf.latitude = w.latitude and ...

-- To:
left join {{ ref('stg_nldas3_weather') }} as w on sf.site_id = w.site_id and sf.observation_hour = w.observed_at
```

### 4.3 Modify: `elt/transformation/models/final/flood_model.sql`
- Update join to use `site_id` instead of coordinates

---

## Files to Create
| File | Purpose |
|------|---------|
| `elt/extraction/nldas3.py` | Core extraction logic |
| `elt/extraction/watershed_mapping.py` | Grid-to-watershed utilities |
| `orchestration/assets/nldas3_forcing.py` | Dagster asset |
| `orchestration/assets/watershed_mapping.py` | Mapping asset |
| `elt/transformation/models/staging/stg_nldas3_weather.sql` | dbt staging |

## Files to Modify
| File | Change |
|------|--------|
| `orchestration/configs/extraction.py` | Add `NLDAS3Config` |
| `orchestration/assets/__init__.py` | Register new assets |
| `orchestration/definitions.py` | Add assets to definitions |
| `elt/transformation/models/marts/fct_streamflow_hourly.sql` | Change join to site_id |
| `elt/transformation/models/final/flood_model.sql` | Change join to site_id |
| `pyproject.toml` | Add geopandas, xarray, etc. |

---

## Verification

1. **Test watershed mapping**:
   ```bash
   uv run python -c "from elt.extraction.watershed_mapping import compute_watershed_grid_mapping; ..."
   ```

2. **Test NLDAS-3 download**:
   ```bash
   aws s3 ls s3://nasa-waterinsight/NLDAS3/forcing/hourly/202401/ --no-sign-request
   ```

3. **Run mapping asset**:
   ```bash
   uv run dagster asset materialize -m orchestration.definitions --select nldas3_watershed_mapping
   ```

4. **Run extraction asset (sample)**:
   ```bash
   uv run dagster asset materialize -m orchestration.definitions --select nldas3_forcing_raw -c run_config_sample.yaml
   ```

5. **Run dbt**:
   ```bash
   just transform
   ```

6. **Validate joins**:
   ```sql
   SELECT COUNT(*) FROM marts.fct_streamflow_hourly WHERE precipitation_mm IS NOT NULL;
   ```

---

## Background Context

### Why NLDAS-3?
- CAMELS-H uses NLDAS-2 for watershed-averaged forcing data
- NLDAS-2 is decommissioned; NLDAS-3 is the replacement
- Open-Meteo has 10k/day API limits; NLDAS-3 has no rate limits (file download)
- Watershed averaging is more scientifically rigorous than point-based extraction

### NLDAS-3 Details
- 1km resolution (finer than NLDAS-2's ~12km)
- Hourly temporal resolution
- 8 forcing variables: temperature, humidity, pressure, wind (U/V), shortwave/longwave radiation, precipitation
- Public access via AWS S3: `s3://nasa-waterinsight/NLDAS3/`

### Current Architecture
- Weather extraction: `elt/extraction/weather.py` (Open-Meteo, point-based)
- Weather asset: `orchestration/assets/weather_forcing.py`
- Join key: `(longitude, latitude, datetime)`
- ~1,037 sites in Missouri River Basin (HUC 10)
