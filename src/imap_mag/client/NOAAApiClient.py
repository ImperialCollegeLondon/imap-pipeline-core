"""Interct with the other L1 API."""

import logging
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
