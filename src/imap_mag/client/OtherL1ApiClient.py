"""Interct with the other L1 API."""

import logging
from datetime import datetime
from time import time
from turtle import st
from typing import Any

import requests

logger = logging.getLogger(__name__)


class OtherL1ApiClient:
    """Interacts with SOLAR-1, ACE and DSCOVR APIs.

    Which specific JSON file to use depends on the spacecraft as well as the
    time range to retrieve, using the file that covers as much as possible
    within the requested time range.
    """

    def __init__(self, solar1_ace_url: str, dscovr_url: str):
        self._solar1_ace_url: str = solar1_ace_url
        self._dscovr_url: str = dscovr_url

    @staticmethod
    def _download_json_file(base_url: str, file_name: str) -> list[dict[str, Any]]:
        """Download a JSON file return its content as a list of dictionaries.

        Args:
            base_url: The base URL of the API.
            file_name: The name of the JSON file to download.

        Returns:
            A list of dictionaries containing the JSON data.
        """

        url = f"{base_url}/{file_name}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            return response.json()  # Assuming the response is a JSON array
        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    def _get_solar1_ace_data(self) -> list[dict[str, Any]]:
        """Download SOLAR-1 and ACE data.

        As there is one file for magnetic field and another for plasma, both containing
        the last 24h, we can just download both files and return the data. No date range
        needed.

        Note that the files might contain data from other spacecrafts as well as
        variables that are not needed, but we will filter them later in the pipeline.

        Returns:
            A list of dictionaries containing the combined data from both files.
        """

        mag_file = "rtsw_mag_1m.json"
        plasma_file = "rtsw_plasma_1m.json"

        try:
            mag_data: list[dict[str, Any]] = self._download_json_file(
                self._solar1_ace_url, mag_file
            )
            plasma_data: list[dict[str, Any]] = self._download_json_file(
                self._solar1_ace_url, plasma_file
            )
        except Exception as e:
            logger.error(f"Error downloading SOLAR-1 and ACE data: {e}")
            return []

        return mag_data + plasma_data

    def _get_dscovr_data(
        self, start_date: datetime, end_date: datetime
    ) -> list[dict[str, Any]]:
        """Download DSCOVR data.

        The DSCOVR API privides several files, each containing data for a specific time
        range for 2h, 6h, 1 day, 3 day and 7 day. We will download the file that covers
        as much as possible of the requested time range.

        Args:
            start_date: The start date of the requested time range.
            end_date: The end date of the requested time range.

        Returns:
            A list of dictionaries containing the DSCOVR data.
        """
        # Determine which file to download based on the time range
        time_ranges = {
            "2-hour": 2,
            "6-hour": 6,
            "1-day": 24,
            "3-day": 72,
            "7-day": 168,
        }
        dt = end_date - start_date
        for file_name, hours in time_ranges.items():
            if dt.total_seconds() <= hours * 3600:
                _mag_file = f"mag-{file_name}.json"
                _plasma_file = f"plasma-{file_name}.json"
                break
        else:
            _mag_file = "mag-7-day.json"
            _plasma_file = "plasma-7-day.json"

        # Download the data from the DSCOVR API
        try:
            mag_data: list[dict[str, Any]] = self._download_json_file(
                self._dscovr_url, _mag_file
            )
            plasma_data: list[dict[str, Any]] = self._download_json_file(
                self._dscovr_url, _plasma_file
            )
        except Exception as e:
            logger.error(f"Error downloading DSCOVR data: {e}")
            return []

        # Convert the list of lists to a list of dictionaries using the first row as
        # keys
        mag_cols = mag_data[0]
        plasma_cols = plasma_data[0]
        mag_data_dicts = [dict(zip(mag_cols, row)) for row in mag_data[1:]]
        plasma_data_dicts = [dict(zip(plasma_cols, row)) for row in plasma_data[1:]]
        return mag_data_dicts + plasma_data_dicts

    def get_all_data(
        self,
        *,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, list[dict[str, Any]]]:
        """Download data from all other L1 spacecrafts.

        Args:
            start_date: The start date of the requested time range.
            end_date: The end date of the requested time range.

        Returns:
            A list of dictionaries containing the DSCOVR data.
        """
        start_time = time()
        logger.info(
            f"Downloading data from other L1 spacecrafts between {start_date} "
            + f"and {end_date}."
        )

        solar1_ace_data = self._get_solar1_ace_data()
        dscovr_data = self._get_dscovr_data(start_date, end_date)

        logger.debug(
            f"Downloaded {len(solar1_ace_data) + len(dscovr_data)} records from other "
            + f"L1 spacecrafts in {time() - start_time:.2f} seconds."
        )
        return {
            "solar1_ace": solar1_ace_data,
            "dscovr": dscovr_data,
        }
