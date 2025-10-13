"""Interact with SDC APIs to get MAG data via ialirt-data-access."""

import logging
from datetime import datetime, timedelta

import ialirt_data_access
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class IALiRTDataAccess:
    """Class for downloading MAG data via ialirt-data-access."""

    def __init__(self, auth_code: SecretStr | None, sdc_url: str | None = None) -> None:
        """Initialize SDC API client."""

        if auth_code:
            ialirt_data_access.config["API_KEY"] = auth_code.get_secret_value()
        if sdc_url:
            ialirt_data_access.config["DATA_ACCESS_URL"] = sdc_url

    def download(
        self,
        *,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Download MAG data from I-ALiRT via ialirt-data-access."""

        whole_data: list[dict] = []
        latest_date: datetime = start_date

        while (end_date - latest_date) > timedelta(seconds=4):
            data_chunk: list[dict] = self.__do_download(latest_date, end_date)
            whole_data.extend(data_chunk)

            if data_chunk:
                max_chunk_date = max(
                    datetime.strptime(d["met_in_utc"], "%Y-%m-%dT%H:%M:%S")
                    for d in data_chunk
                )

                logger.info(
                    f"Downloaded {len(data_chunk)} records from I-ALiRT between {latest_date} and {max_chunk_date}."
                )
                latest_date = max_chunk_date + timedelta(seconds=1)
            else:
                logger.info(
                    f"No more data to download between {latest_date} and {end_date}."
                )
                break

        return whole_data

    def __do_download(self, start_date: datetime, end_date: datetime) -> list[dict]:
        return ialirt_data_access.data_product_query(
            met_in_utc_start=start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            met_in_utc_end=end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        )
