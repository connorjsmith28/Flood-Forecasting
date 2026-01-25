"""Configuration classes for extraction assets."""

from dagster import Config


class ExtractionConfig(Config):
    """Base configuration for extraction assets."""

    sample_mode: bool = True  # If True, only load a limited sample for USGS data


class StreamflowConfig(ExtractionConfig):
    """Configuration for streamflow extraction."""

    days_back: int = 30  # Days of history for initial load
    incremental_days: int = 2  # Days to look back for incremental (overlap for safety)
    site_ids: list[str] | None = None


class WeatherConfig(ExtractionConfig):
    """Configuration for weather forcing data extraction (Open-Meteo).

    Rate limiting defaults are set for the free tier:
    - 600 calls/minute (we use 500 for safety margin)
    - 5,000 calls/hour
    - 10,000 calls/day
    """

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
    # Rate limiting (free tier defaults)
    calls_per_minute: int = 500  # Free tier limit is 600, use 500 for safety
    retry_attempts: int = 3  # Number of retries on failure
    retry_backoff: int = 60  # Base backoff in seconds for rate limit errors


class SiteConfig(ExtractionConfig):
    """Configuration for site metadata extraction."""

    huc_code: str = "10"  # HUC region to extract (default: Missouri River Basin)
