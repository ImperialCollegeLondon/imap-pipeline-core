"""Interct with the other L1 API."""

import logging
from datetime import UTC, datetime
from time import time
from typing import Any, Literal

import requests

logger = logging.getLogger(__name__)


def _download_json_file(base_url: str, file_name: str) -> list[Any]:
    """Download a JSON file return its instrument as a list of objects.

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
    """Interacts with NOAA's RTSW API, for SOLAR-1 and ACE spacecrafts data.

    Which specific JSON file to use depends on the specific instrument to fetch, mag or
    wind data.
    """

    def __init__(self, url: str):
        self._url = url

        if not self._url:
            raise ValueError("SOLAR-1 and ACE URL cannot be empty.")

    def get_data(
        self, spacecraft: Literal["SOLAR1", "ACE"], instrument: Literal["mag", "wind"]
    ) -> list[dict[str, Any]]:
        """Download SOLAR-1 and ACE data from real-time space weather API.

        As there is one file for magnetic field and another for wind, both containing
        the last 24h, we can just download the selected file and return the data.
        No date range needed.

        Only the data associated to the selected spacecraft is returned.

        Args:
            spacecraft: The spacecraft to retrieve the data for. Must be "SOLAR1" or
                "ACE"
            instrument: The instrument to retrieve. Must be `mag` or `wind`.

        Returns:
            A list of dictionaries containing the combined data from both files.
        """
        if spacecraft not in ("SOLAR1", "ACE"):
            raise ValueError(
                "Invalid spacecraft requested. "
                f"It must be 'SOLAR1' or 'ACE', but '{spacecraft}' found"
            )

        if instrument not in ("mag", "wind"):
            raise ValueError(
                f"Invalid instrument type requested for {spacecraft}. "
                f"It must be 'mag' or 'wind', but '{instrument}' found"
            )

        start_time = time()
        logger.info(f"Downloading {instrument} data for {spacecraft}...")

        _data: list[dict[str, Any]] = _download_json_file(
            self._url, f"rtsw_{instrument}_1m.json"
        )
        # We return only data relevant for the selected spacecraft
        _data = [record for record in _data if record["source"] == spacecraft]

        logger.debug(
            f"Downloaded {len(_data)} {instrument} records for {spacecraft} in "
            f"{time() - start_time:.2f} seconds."
        )

        return _data


class DSCOVRApiClient:
    """Interacts with the DSCOVR APIs provided by NOAA."""

    def __init__(self, url: str):
        self._url = url

        if not self._url:
            raise ValueError("DSCOVR URL cannot be empty.")

    def get_data(
        self, instrument: Literal["mag", "wind"], start_date: datetime
    ) -> list[dict[str, Any]]:
        """Download DSCOVR data from solar wind API.

        The DSCOVR API privides several files, each containing data for a specific time
        range for the last 2h, 6h, 1 day, 3 day and 7 day. We will download the file
        that covers as much as possible of the requested time range.

        Args:
            instrument: The instrument to retrieve. Must be `mag` or `wind`.
            start_date: The start date of the requested time range.

        Returns:
            A list of dictionaries containing the DSCOVR data.
        """
        if instrument not in ("mag", "wind"):
            raise ValueError(
                "Invalid instrument type requested for DSCOVR. "
                f"It must be 'mag' or 'wind', but '{instrument}' found"
            )

        start_time = time()
        logger.info(
            f"Downloading {instrument} data for DSCOVR from {start_date} "
            + f"until {datetime.now(tz=UTC)}..."
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
                _file = f"{instrument}-{file_name}.json"
                break
        else:
            _file = f"{instrument}-7-day.json"

        # Download the data from the DSCOVR API
        _data: list[list[Any]] = _download_json_file(self._url, _file)

        # Convert the list of lists to a list of dictionaries using the first row as
        # keys
        _cols = _data[0]
        _data_dicts = [dict(zip(_cols, row)) for row in _data[1:]]

        logger.debug(
            f"Downloaded {len(_data_dicts)} {instrument} records for DSCOVR in "
            f"{time() - start_time:.2f} seconds."
        )

        return _data_dicts
