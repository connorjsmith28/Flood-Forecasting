"""NLDI basin characteristics extraction.

Uses PyNHD to fetch basin characteristics from the NLDI service.
"""

import pandas as pd
from pynhd import NLDI

# Default characteristics to fetch - commonly used for hydrology modeling
# CAT_ = local catchment, TOT_ = total upstream watershed
DEFAULT_CHARACTERISTICS = [
    # Hydrologic
    "CAT_BFI",  # Base Flow Index
    "CAT_PET",  # Potential Evapotranspiration
    "CAT_RECHG",  # Groundwater Recharge
    "CAT_TWI",  # Topographic Wetness Index
    # Climate
    "CAT_PPT7100_ANN",  # Mean Annual Precipitation
    "CAT_TAV7100_ANN",  # Mean Annual Temperature
    "CAT_TMAX7100",  # Mean Max Temperature
    "CAT_TMIN7100",  # Mean Min Temperature
    "CAT_RH",  # Relative Humidity
    "CAT_ET",  # Actual Evapotranspiration
    # Land Cover (NLCD 2019)
    "CAT_NLCD19_11",  # Open Water
    "CAT_NLCD19_21",  # Developed, Open Space
    "CAT_NLCD19_22",  # Developed, Low Intensity
    "CAT_NLCD19_41",  # Deciduous Forest
    "CAT_NLCD19_42",  # Evergreen Forest
    "CAT_NLCD19_43",  # Mixed Forest
    "CAT_NLCD19_52",  # Shrub/Scrub
    "CAT_NLCD19_71",  # Grassland/Herbaceous
    "CAT_NLCD19_81",  # Pasture/Hay
    "CAT_NLCD19_82",  # Cultivated Crops
    "CAT_NLCD19_90",  # Woody Wetlands
    "CAT_NLCD19_95",  # Emergent Herbaceous Wetlands
    # Impervious
    "CAT_IMPV11",  # Impervious 2011
    # Topography
    "CAT_ELEV_MEAN",  # Mean Elevation
    "CAT_ELEV_MAX",  # Max Elevation
    "CAT_ELEV_MIN",  # Min Elevation
    "CAT_SLOPE_PCT",  # Mean Slope
    # Basin geometry
    "CAT_BASIN_AREA",  # Basin Area
]


def fetch_nldi_characteristics_batch(site_ids, char_ids=None):
    """Fetch basin characteristics for USGS sites via NLDI."""
    if not site_ids:
        return pd.DataFrame()

    if not char_ids:
        char_ids = DEFAULT_CHARACTERISTICS

    site_ids = list(site_ids)
    nldi = NLDI()

    # Resolve USGS site IDs to NHDPlus ComIDs
    # Site ID = USGS NWIS gage station number (e.g., "01010000")
    # ComID = NHDPlus stream reach identifier that NLDI uses for basin characteristics
    site_to_comid = {}
    for site_id in site_ids:
        try:
            feature = nldi.getfeature_byid("nwissite", f"USGS-{site_id}")
            if feature is not None and not feature.empty:
                site_to_comid[site_id] = int(feature["comid"].iloc[0])
        except Exception:
            pass

    if not site_to_comid:
        return pd.DataFrame()

    # Get unique ComIDs (multiple gages may be on the same stream reach)
    comids = list(set(site_to_comid.values()))

    # Fetch characteristics
    all_chars = {}

    for char_id in char_ids:
        try:
            result = nldi.get_characteristics([char_id], comids=comids)
            if result is not None and not result.empty:
                all_chars[char_id] = result[char_id].to_dict()
        except Exception:
            pass

    if not all_chars:
        return pd.DataFrame()

    # Build records
    records = []
    for site_id, comid in site_to_comid.items():
        record = {"site_id": site_id, "comid": comid}
        for char_id, char_data in all_chars.items():
            if comid in char_data:
                record[char_id] = char_data[comid]
        records.append(record)

    return pd.DataFrame(records)
