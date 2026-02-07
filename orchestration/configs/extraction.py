"""Configuration classes for data extraction assets."""

from dagster import Config


class ExtractionConfig(Config):
    """Base configuration for extraction assets."""

    sample_mode: bool = False  # If True, only load a limited sample for USGS data
    max_sites: int = 100  # Max sites to load in sample mode (controls USGS data volume)
    parallel_fetches: int = 1  # Max concurrent API requests (keep low to avoid rate limits)
    time_window_days: int = 365  # Batch fetches by time window (default: yearly)


class StreamflowConfig(ExtractionConfig):
    """Configuration for streamflow extraction."""

    days_back: int = 7  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    site_ids: list[str] | None = None
    min_date: str = "2007-10-01"  # USGS IV data availability starts here


class WeatherConfig(ExtractionConfig):
    """Configuration for weather forcing data extraction (Open-Meteo)."""

    days_back: int = 7
    incremental_days: int = 2
    min_date: str = "2007-10-01"  # Align with USGS IV data availability
    variables: list[str] = [
        "prcp",
        "temp",
        "humidity",
        "wind_speed",
        "wind_direction",
        "rsds",  # Shortwave radiation
        "rlds",  # Longwave radiation
        "psurf",  # Surface pressure
        "pet",  # Evapotranspiration
    ]


class SiteConfig(ExtractionConfig):
    """Configuration for site metadata extraction."""

    huc_code: str = "10"  # HUC region to extract (default: Missouri River Basin)


class NLDAS3Config(ExtractionConfig):
    """Configuration for NLDAS-2 V2.0 watershed-averaged forcing data extraction."""

    days_back: int = 7  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    min_date: str = "2001-01-01"  # NLDAS-2 data availability (1979-present, but 2001+ for our use)
    lag_days: int = 4  # NLDAS-2 data latency (files available ~4 days behind)
    cache_dir: str = ".cache/nldas"  # Local cache for downloaded NetCDF files
    # CAMELS-H forcing variables from NLDAS-2
    variables: list[str] = [
        "Tair",  # Air temperature at 2m (K)
        "Qair",  # Specific humidity at 2m (kg/kg)
        "PSurf",  # Surface pressure (Pa)
        "Wind_E",  # Eastward wind at 10m (m/s)
        "Wind_N",  # Northward wind at 10m (m/s)
        "SWdown",  # Surface downward shortwave radiation (W/m²)
        "LWdown",  # Surface downward longwave radiation (W/m²)
        "Rainf",  # Total rainfall (kg/m²)
        "CRainf_frac",  # Convective rainfall fraction (-)
        "CAPE",  # Convective available potential energy (J/kg)
        "PotEvap",  # Potential evaporation (kg/m²)
    ]
