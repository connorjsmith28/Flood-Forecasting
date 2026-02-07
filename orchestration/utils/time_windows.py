"""Time window utilities for batched data extraction."""

from datetime import datetime, timedelta


def _to_naive(dt: datetime) -> datetime:
    """Convert to naive datetime for comparison (strip timezone if present)."""
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def generate_time_windows(
    start_date: datetime,
    end_date: datetime,
    window_days: int = 365,
) -> list[tuple[datetime, datetime]]:
    """Generate non-overlapping time windows for batched extraction.

    Args:
        start_date: Start of the overall date range
        end_date: End of the overall date range
        window_days: Size of each window in days (default: 365 for yearly batches)

    Returns:
        List of (window_start, window_end) tuples covering the full range
    """
    start_date = _to_naive(start_date)
    end_date = _to_naive(end_date)

    windows = []
    current = start_date

    while current < end_date:
        window_end = min(current + timedelta(days=window_days - 1), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)

    return windows
