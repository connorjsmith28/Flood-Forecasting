"""Configuration classes for data extraction assets."""

from dagster import Config


class ExtractionConfig(Config):
    """Base configuration for extraction assets."""

    sample_mode: bool = True  # If True, only load a limited sample for USGS data
    max_sites: int = 100  # Max sites to load in sample mode (controls USGS data volume)


class StreamflowConfig(ExtractionConfig):
    """Configuration for streamflow extraction."""

    days_back: int = 30  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    site_ids: list[str] | None = None


class WeatherConfig(ExtractionConfig):
    """Configuration for weather forcing data extraction (Open-Meteo)."""

    days_back: int = 7
    incremental_days: int = 2
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
