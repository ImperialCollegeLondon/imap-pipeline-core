"""Download telemetry data from WebTCAD LaTiS API."""

import logging
from datetime import datetime

import requests
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class WebTCADLaTiS:
    """Client for downloading telemetry data from the WebTCAD LaTiS API."""

    __auth_code: SecretStr
    __base_url: str

    def __init__(self, auth_code: SecretStr | None, base_url: str) -> None:
        self.__auth_code = auth_code or SecretStr("")
        self.__base_url = base_url.rstrip("/")

    def download_csv(
        self,
        *,
        tmid: int,
        start_date: datetime,
        end_date: datetime,
    ) -> str:
        """Download CSV data from the WebTCAD LaTiS API for a given TMID and date range.

        Args:
            tmid: The telemetry item ID to query.
            start_date: Start of the time range (inclusive).
            end_date: End of the time range (exclusive).

        Returns:
            The CSV content as a string.
        """

        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        url = (
            f"{self.__base_url}/AnalogTelemetryItem_SID1.csv"
            f"?TMID={tmid}"
            f"&time,value"
            f"&time%3E={start_str}"
            f"&time%3C={end_str}"
            f"&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
        )

        headers = {
            "Authorization": f"Basic {self.__auth_code.get_secret_value()}",
        }

        logger.info(f"Downloading TMID {tmid} from {start_date} to {end_date}.")
        logger.debug(f"Downloading from: {url}")

        try:
            response: requests.Response = requests.get(
                url,
                headers=headers,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download from WebTCAD LaTiS: {e}")
            raise

        return response.text
