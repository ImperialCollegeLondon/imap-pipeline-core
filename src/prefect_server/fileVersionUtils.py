"""Utilities for extracting version and date information from file paths."""

import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from imap_db.model import File


def extract_version_and_date(file_path: Path) -> tuple[datetime | None, int]:
    """
    Extract date and version number from a file path.

    Looks for patterns like:
    - YYYY-MM-DD or YYYYMMDD for dates
    - v###, version###, _###_ for version numbers

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (date, version) where date is a datetime object or None,
        and version is an integer (0 if not found)
    """
    path_str = str(file_path)

    # Extract date - try multiple patterns
    date = None
    # Pattern 1: YYYY-MM-DD
    date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", path_str)
    if date_match:
        try:
            date = datetime(
                int(date_match.group(1)),
                int(date_match.group(2)),
                int(date_match.group(3)),
                tzinfo=UTC,
            )
        except ValueError:
            pass

    # Pattern 2: YYYYMMDD
    if date is None:
        date_match = re.search(r"(\d{4})(\d{2})(\d{2})", path_str)
        if date_match:
            try:
                date = datetime(
                    int(date_match.group(1)),
                    int(date_match.group(2)),
                    int(date_match.group(3)),
                    tzinfo=UTC,
                )
            except ValueError:
                pass

    # Extract version number - try multiple patterns
    version = 0
    # Pattern 1: v### or version###
    version_match = re.search(r"v(?:ersion)?[\s_-]?(\d+)", path_str, re.IGNORECASE)
    if version_match:
        version = int(version_match.group(1))
    else:
        # Pattern 2: _###_ or -###- (version between separators)
        version_match = re.search(r"[_-](\d{3,})[_-]", path_str)
        if version_match:
            version = int(version_match.group(1))

    return date, version


def select_latest_version_per_day(files: list[File]) -> list[File]:
    """
    Select only the latest version of files per day.

    Groups files by date and selects the file with the highest version number
    for each date. Files without dates are kept separate and the latest version
    among them is selected.

    Args:
        files: List of File objects from database

    Returns:
        List of File objects containing only the latest version per day
    """
    # Group files by date
    files_by_date: dict[tuple[str, datetime | None], list[tuple[File, int]]] = (
        defaultdict(list)
    )

    for file in files:
        date, version = extract_version_and_date(Path(file.path))
        type_date_key = (file.get_file_type_string(), date.date() if date else None)
        files_by_date[type_date_key].append((file, version))

    # Select latest version per date
    latest_files = []
    for _, file_list in files_by_date.items():
        # Sort by version (descending) and take the first one
        file_list.sort(key=lambda x: x[1], reverse=True)
        latest_files.append(file_list[0][0])  # Append the file object

    return latest_files


def get_file_type_date_key(file: File) -> tuple[str, datetime | None]:
    """
    Get the file type and date key for grouping files.

    Args:
        file: File object from database

    Returns:
        Tuple of (file_type_string, date) for grouping
    """
    date, _ = extract_version_and_date(Path(file.path))
    return (file.get_file_type_string(), date.date() if date else None)


def get_file_version(file: File) -> int:
    """
    Get the version number from a file.

    Args:
        file: File object from database

    Returns:
        Version number (0 if not found)
    """
    _, version = extract_version_and_date(Path(file.path))
    return version
