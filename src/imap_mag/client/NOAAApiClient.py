"""Interct with the other L1 API."""

import logging
from datetime import UTC, datetime
from time import time
from typing import Any, Literal

import requests

logger = logging.getLogger(__name__)


def _download_json_file(base_url: str, file_name: str) -> list[Any]:
    """Download a JSON file return its content as a list of objects.

    Args:
        base_url: The base URL of the API.
        file_name: The name of the JSON file to download.

    Returns:
        A list of dictionaries containing the JSON data.
    """

    url = f"{base_url.rstrip('/')}/{file_name.lstrip('/')}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()  # Assuming the response is a JSON array


class NOAARTSWApiClient:
    """Interacts with SOLAR-1 and ACE APIs provided by NOAA.

    Which specific JSON file to use depends on the spacecraft as well as the
    time range to retrieve, using the file that covers as much as possible
    within the requested time range.
    """

    def __init__(self, solar1_ace_url: str):
        self._url = solar1_ace_url

        if not self._url:
            raise ValueError("SOLAR-1 and ACE URL cannot be empty.")

    def get_data(
        self, spacecraft: Literal["SOLAR1", "ACE"], content: Literal["mag", "plasma"]
    ) -> list[dict[str, Any]]:
        """Download SOLAR-1 and ACE data from real-time space weather API.

        As there is one file for magnetic field and another for plasma, both containing
        the last 24h, we can just download the selected file and return the data.
        No date range needed.

        Only the data associated to the selected spacecraft is returned.

        Args:
            spacecraft: The spacecraft to retrieve the data for. Must be "SOLAR1" or
                "ACE"
            content: The content to retrieve. Must be `mag` or `plasma`.

        Returns:
            A list of dictionaries containing the combined data from both files.
        """
        if spacecraft not in ("SOLAR1", "ACE"):
            raise ValueError(
                "Invalid spacecraft type requested. "
                f"It must be 'SOLAR1' or 'ACE', but {spacecraft} found"
            )

        if content not in ("mag", "plasma"):
            raise ValueError(
                f"Invalid content type requested for {spacecraft}. "
                f"It must be 'mag' or 'plasma', but {content} found"
            )

        _data: list[dict[str, Any]] = _download_json_file(
            self._url, f"{content}_1m.json"
        )

        # We return only data relevant for the selected spacecraft
        return [record for record in _data if record["source"] == spacecraft]


class DSCOVRApiClient:
    """Interacts with the DSCOVR APIs provided by NOAA."""

    def __init__(self, dscovr_url: str):
        self._url = dscovr_url

        if not self._url:
            raise ValueError("DSCOVR URL cannot be empty.")

    def get_data(
        self, content: Literal["mag", "plasma"], start_date: datetime
    ) -> list[dict[str, Any]]:
        """Download DSCOVR data from solar wind API.

        The DSCOVR API privides several files, each containing data for a specific time
        range for the last 2h, 6h, 1 day, 3 day and 7 day. We will download the file
        that covers as much as possible of the requested time range.

        Args:
            content: The content to retrieve. Must be `mag` or `plasma`.
            start_date: The start date of the requested time range.

        Returns:
            A list of dictionaries containing the DSCOVR data.
        """
        if content not in ("mag", "plasma"):
            raise ValueError(
                "Invalid content type requested for DSCOVR. "
                f"It must be 'mag' or 'plasma', but {content} found"
            )

        start_time = time()
        logger.info(
            f"Downloading {content} data for DSCOVR from {start_date} "
            + f"until {datetime.now(tz=UTC)}."
        )

        # Determine which file to download based on the time range
        time_ranges = {
            "2-hour": 2,
            "6-hour": 6,
            "1-day": 24,
            "3-day": 72,
            "7-day": 168,
        }
        dt = datetime.now(tz=UTC) - start_date
        for file_name, hours in time_ranges.items():
            if dt.total_seconds() <= hours * 3600:
                _file = f"{content}-{file_name}.json"
                break
        else:
            _file = f"{content}-7-day.json"

        # Download the data from the DSCOVR API
        _data: list[list[Any]] = _download_json_file(self._url, _file)

        # Convert the list of lists to a list of dictionaries using the first row as
        # keys
        _cols = _data[0]
        _data_dicts = [dict(zip(_cols, row)) for row in _data[1:]]

        logger.debug(
            f"Downloaded {len(_data_dicts)} {content} records for DSCOVR in "
            f"{time() - start_time:.2f} seconds."
        )

        return _data_dicts
