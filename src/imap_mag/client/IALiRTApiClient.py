"""Interact with SDC APIs to get MAG data via ialirt-data-access."""

import json
import logging
from datetime import datetime, timedelta

import ialirt_data_access
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class IALiRTApiClient:
    """
    Download all data from I-ALiRT API between dates.
    Will paginate results over API as needed to get all data.
    Returns when no more data available or end_date is reached.
    Uses ialirt-data-access to issue HTTP requests.
    """

    __DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    __DATE_INDEX = "time_utc"

    def __init__(self, auth_code: SecretStr | None, sdc_url: str | None = None) -> None:
        """Initialize SDC API client."""

        if auth_code:
            ialirt_data_access.config["API_KEY"] = auth_code.get_secret_value()
        if sdc_url:
            ialirt_data_access.config["DATA_ACCESS_URL"] = sdc_url

    def get_all_by_dates(
        self,
        *,
        instrument: str,
        start_date: datetime,
        end_date: datetime,
        max_hours_per_chunk: int | None = None,
    ) -> list[dict]:
        """Download data from I-ALiRT via ialirt-data-access for a specific instrument."""

        if start_date.tzinfo is not None:
            start_date = start_date.replace(tzinfo=None)
        if end_date.tzinfo is not None:
            end_date = end_date.replace(tzinfo=None)

        whole_data: list[dict] = []
        window_start: datetime = start_date

        while (end_date - window_start) > timedelta(seconds=4):
            window_end = (
                min(end_date, window_start + timedelta(hours=max_hours_per_chunk))
                if max_hours_per_chunk is not None
                else end_date
            )
            data_chunk: list[dict] = self.__do_download(
                instrument, window_start, window_end
            )

            if data_chunk is None:
                logger.warning(
                    f"API returned None for {instrument}. Treating as empty."
                )
                data_chunk = []

            whole_data.extend(data_chunk)

            window_start = window_end

        return whole_data

    def __do_download(
        self, instrument: str, start_date: datetime, end_date: datetime
    ) -> list[dict]:

        result = ialirt_data_access.data_product_query(
            instrument=instrument,
            time_utc_start=start_date.strftime(self.__DATE_FORMAT),
            time_utc_end=end_date.strftime(self.__DATE_FORMAT),
        )

        if isinstance(result, (str, bytes)):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from API response: {result}")

        if isinstance(result, dict):
            return result.get("data", [])

        return result
