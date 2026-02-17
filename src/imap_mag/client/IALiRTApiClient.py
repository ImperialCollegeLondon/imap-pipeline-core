"""Interact with SDC APIs to get MAG data via ialirt-data-access."""

import logging
from datetime import UTC, datetime, timedelta

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
    ) -> list[dict]:
        """Download data from I-ALiRT via ialirt-data-access for a specific instrument."""

        whole_data: list[dict] = []
        latest_date: datetime = start_date

        while (end_date - latest_date) > timedelta(seconds=4):
            data_chunk: list[dict] = self.__do_download(
                instrument, latest_date, end_date
            )
            whole_data.extend(data_chunk)

            if data_chunk:
                max_chunk_date = max(
                    datetime.strptime(d[self.__DATE_INDEX], self.__DATE_FORMAT)
                    for d in data_chunk
                )

                logger.debug(
                    f"Downloaded {len(data_chunk)} records from I-ALiRT between {latest_date} and {max_chunk_date}."
                )
                latest_date = max_chunk_date + timedelta(seconds=1)
            else:
                logger.debug(
                    f"No more data to download between {latest_date} and {end_date}."
                )
                break

        return whole_data

    def __do_download(
        self, instrument: str, start_date: datetime, end_date: datetime
    ) -> list[dict]:
        result = ialirt_data_access.data_product_query(
            instrument=instrument,
            time_utc_start=start_date.astimezone(UTC).strftime(self.__DATE_FORMAT),
            time_utc_end=end_date.astimezone(UTC).strftime(self.__DATE_FORMAT),
        )

        if isinstance(result, dict):
            return result.get("data", [])

        return result
