"""Interact with SDC APIs to get MAG data via ialirt-data-access."""

import json
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
        max_hours_per_chunk: int | None = None,
    ) -> list[dict]:
        """Download data from I-ALiRT via ialirt-data-access for a specific instrument."""

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

            if data_chunk:
                newest_data_timestamp = max(
                    datetime.strptime(d[self.__DATE_INDEX], self.__DATE_FORMAT)
                    for d in data_chunk
                )

                logger.debug(
                    f"Downloaded {len(data_chunk)} records from I-ALiRT between {window_start} and {newest_data_timestamp}."
                )

                next_date = newest_data_timestamp + timedelta(seconds=1)

                if next_date <= window_start:
                    logger.warning(
                        f"Data timestamps did not advance past {window_start}. Forcing window forward to {window_end}"
                    )
                    window_start = window_end
                else:
                    window_start = next_date

            elif window_end < end_date:
                logger.debug(
                    f"No data downloaded between {window_start} and {window_end}, but end date not reached. Advancing window_start to {window_end} to continue downloading."
                )
                window_start = window_end
            else:
                logger.debug(
                    f"No more data to download between {window_start} and {end_date}."
                )
                break

        return whole_data

    def _ensure_utc_string(self, dt: datetime) -> str:
        """Datetime to a UTC string."""
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=UTC)
        else:
            dt_utc = dt.astimezone(UTC)

        return dt_utc.strftime(self.__DATE_FORMAT)

    def __do_download(
        self, instrument: str, start_date: datetime, end_date: datetime
    ) -> list[dict]:
        result = ialirt_data_access.data_product_query(
            instrument=instrument,
            time_utc_start=self._ensure_utc_string(start_date),
            time_utc_end=self._ensure_utc_string(end_date),
        )

        if isinstance(result, (str, bytes)):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from API response: {result}")

        if isinstance(result, dict):
            return result.get("data", [])

        return result
