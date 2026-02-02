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
