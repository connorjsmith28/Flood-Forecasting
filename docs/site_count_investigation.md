# Site Count Investigation: Why 12,920 Sites But Only 1,430 Have Streamflow

**Date:** 2026-01-28

## Summary

The USGS site discovery for HUC 10 (Missouri River Basin) returns 12,920 stream sites, but only 1,430 of these sites actually have discharge (streamflow) measurements. The weather extraction was fetching data for all 12,920 sites unnecessarily.

As of 2023, https://www.usgs.gov/mission-areas/water-resources/science/usgs-streamgages-numbers shows 
"8,635 monitor streamflow and water level year-round
3,250 only record water level or operate less than year-round"

We take a subsample of the above, so it is probably a mix of an error in the extractor coupled with a number of sites not actually measuring the target water variable.

## Findings

### Site Counts

| Metric | Count |
|--------|-------|
| Sites in `raw.site_metadata` | 12,920 |
| Sites with discharge data in `raw.streamflow_raw` | 1,430 |
| Sites WITHOUT discharge data | 11,490 |

### Root Cause

The USGS `what_sites(huc="10", siteType="ST")` API returns ALL stream sites in the HUC region, but not all stream sites measure discharge (parameter code 00060). Many sites measure other parameters like:
- Water quality
- Water temperature
- Groundwater levels
- Sediment

### Verification

Tested sites WITHOUT streamflow data in our DB against USGS API:
```
06015480 (Grasshopper Creek tributary near Dillon, MT): NO discharge data available
06026000 (Birch Creek near Glen MT): NO discharge data available
06028500 (Little Pipestone Cr nr Whitehall MT): NO discharge data available
06034300 (South Boulder River near Cardwell MT): NO discharge data available
06042920 (Gallatin River near WY-MT stateline, YNP): NO discharge data available
```

Tested sites WITH streamflow data in our DB:
```
06214500: Has 2669 records
06191500: Has 2669 records
06192500: Has 2569 records
06185500: Has 2669 records
06186500: Has 2667 records
```

## Impact on Weather Extraction

### Before Fix
- Fetching weather for 12,920 locations
- ~259 API batches (at 50 coords/batch)
- Frequent rate limiting from Open-Meteo

### After Fix
- Fetching weather for 1,428 unique coordinates (from 1,430 sites)
- ~29 API batches
- **89% reduction in API calls**

## Fix Applied

Modified `orchestration/assets/weather_forcing.py` to only fetch weather for sites that have streamflow data:

```sql
-- Before: fetched ALL sites
SELECT site_id, longitude, latitude
FROM raw.site_metadata
WHERE longitude IS NOT NULL AND latitude IS NOT NULL

-- After: only sites with streamflow
SELECT DISTINCT m.site_id, m.longitude, m.latitude
FROM raw.site_metadata m
INNER JOIN raw.streamflow_raw s ON m.site_id = s.site_id
WHERE m.longitude IS NOT NULL AND m.latitude IS NOT NULL
```

## Future Considerations

1. **Site metadata table**: Could add a flag `has_discharge` to indicate which sites have discharge data
2. **Alternative approach**: Filter sites during metadata extraction by checking if they have parameter code 00060
3. **Weather data deduplication**: Some sites share coordinates (1,430 sites → 1,428 unique coords). Could further optimize by deduplicating coordinates before fetching weather.
