"""Site-to-NLDAS-grid mapping.

Maps USGS site coordinates to the nearest NLDAS 0.125-degree grid cell.
Used by the NLDAS forcing extraction to look up gridded weather data per site.

Future work will add proper watershed boundary delineation (HydroBasins
upstream tracing) for area-weighted grid averaging. See README for details.
"""

import logging
from collections.abc import Callable

import pandas as pd

logger = logging.getLogger(__name__)

# Missouri Basin bounding box (for clipping NLDAS data)
MISSOURI_BASIN_BBOX = {
    "min_lon": -117.0,
    "max_lon": -89.0,
    "min_lat": 36.0,
    "max_lat": 49.5,
}

# NLDAS grid parameters (0.125 degree resolution, same for NLDAS-2 and NLDAS-3)
NLDAS3_RESOLUTION = 0.125
NLDAS3_ORIGIN_LAT = 25.0625  # Center of southern-most cell
NLDAS3_ORIGIN_LON = -124.9375  # Center of western-most cell
NLDAS3_NLAT = 224  # Number of latitude cells
NLDAS3_NLON = 464  # Number of longitude cells


def generate_point_weights(
    site_coords: list[tuple[str, float, float]],
    log: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Map each site to its nearest NLDAS grid cell.

    Args:
        site_coords: List of (site_id, longitude, latitude) tuples.
        log: Optional logging function.

    Returns:
        DataFrame with columns: site_id, grid_row, grid_col, area_weight
    """

    def _log(msg: str) -> None:
        if log is not None:
            log(msg)
        else:
            logger.info("%s", msg)

    rows = []
    for site_id, lon, lat in site_coords:
        grid_col = round((lon - NLDAS3_ORIGIN_LON) / NLDAS3_RESOLUTION)
        grid_row = round((lat - NLDAS3_ORIGIN_LAT) / NLDAS3_RESOLUTION)

        # Clamp to valid grid bounds
        grid_col = max(0, min(NLDAS3_NLON - 1, int(grid_col)))
        grid_row = max(0, min(NLDAS3_NLAT - 1, int(grid_row)))

        rows.append(
            {
                "site_id": site_id,
                "grid_row": grid_row,
                "grid_col": grid_col,
                "area_weight": 1.0,  # nearest-cell only; true area-weighting requires watershed boundary delineation
            }
        )

    _log(f"Generated grid cell mapping for {len(rows)} sites")
    return pd.DataFrame(rows)
