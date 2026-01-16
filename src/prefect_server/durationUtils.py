"""Utilities for parsing duration strings."""

import re
from datetime import timedelta


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse a duration string into a timedelta.

    Supports formats like:
    - "30d" or "30D" for 30 days
    - "12h" or "12H" for 12 hours
    - "45m" or "45M" for 45 minutes
    - "60s" or "60S" for 60 seconds
    - "1d12h" for 1 day and 12 hours (combinations)

    Args:
        duration_str: Duration string to parse

    Returns:
        timedelta representing the duration

    Raises:
        ValueError: If the duration string is invalid
    """
    if not duration_str or not isinstance(duration_str, str):
        raise ValueError(f"Invalid duration string: {duration_str}")

    duration_str = duration_str.strip().lower()

    # Pattern to match number followed by unit
    pattern = r"(\d+)([dhms])"
    matches = re.findall(pattern, duration_str)

    if not matches:
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            "Expected format like '30d', '12h', '45m', '60s', or combinations like '1d12h'"
        )

    total_seconds = 0
    for value, unit in matches:
        value = int(value)
        if unit == "d":
            total_seconds += value * 86400  # 24 * 60 * 60
        elif unit == "h":
            total_seconds += value * 3600  # 60 * 60
        elif unit == "m":
            total_seconds += value * 60
        elif unit == "s":
            total_seconds += value

    return timedelta(seconds=total_seconds)


def format_duration(td: timedelta) -> str:
    """
    Format a timedelta into a human-readable duration string.

    Args:
        td: timedelta to format

    Returns:
        String like "3d", "12h", "45m", etc.
    """
    total_seconds = int(td.total_seconds())

    if total_seconds == 0:
        return "0s"

    parts = []
    days = total_seconds // 86400
    if days > 0:
        parts.append(f"{days}d")
        total_seconds %= 86400

    hours = total_seconds // 3600
    if hours > 0:
        parts.append(f"{hours}h")
        total_seconds %= 3600

    minutes = total_seconds // 60
    if minutes > 0:
        parts.append(f"{minutes}m")
        total_seconds %= 60

    if total_seconds > 0:
        parts.append(f"{total_seconds}s")

    return "".join(parts)
