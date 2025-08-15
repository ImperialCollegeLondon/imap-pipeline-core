"""Download raw packets from WebPODA."""

import logging
import os
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class WebPODA:
    """Class for downloading raw packets from WebPODA."""

    __auth_code: SecretStr
    __output_dir: Path
    __webpoda_url: str

    def __init__(
        self, auth_code: SecretStr | None, output_dir: Path, webpoda_url: str
    ) -> None:
        """Initialize WebPODA interface."""

        self.__auth_code = auth_code or SecretStr("")
        self.__output_dir = output_dir
        self.__webpoda_url = webpoda_url

    def download(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> Path:
        """Download packet data from WebPODA."""

        file_path: Path = (
            self.__output_dir
            / f"{packet}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.bin"
        )

        logger.info(
            f"Downloading {packet} from {start_date} to {end_date} into {file_path}."
        )

        if not self.__output_dir.exists():
            os.makedirs(self.__output_dir)

        packet_response: requests.Response = self.__download_from_webpoda(
            packet,
            "bin",
            start_date,
            end_date,
            ert,
            "project(packet)",
        )

        with open(file_path, "wb") as f:
            f.write(packet_response.content)

        return file_path

    def get_max_ert(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> datetime | None:
        logger.info(
            f"Downloading ERT information for {packet} from {start_date} to {end_date}."
        )

        ert_response: requests.Response = self.__download_from_webpoda(
            packet,
            "csv",
            start_date,
            end_date,
            ert,
            "project(ert)&formatTime(\"yyyy-MM-dd'T'HH:mm:ss\")",
        )

        ert_info = ert_response.content.decode("utf-8")
        lines = ert_info.strip().splitlines()[1:]

        if not lines:
            logger.debug("No ERT data found.")
            max_ert = None
        else:
            datetimes = [datetime.fromisoformat(line) for line in lines]
            max_ert = max(datetimes)

            logger.debug(f"Max ERT: {max_ert}")

        return max_ert

    def __download_from_webpoda(
        self,
        packet: str,
        extension: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool,
        data: str,
    ) -> requests.Response:
        """Download any data from WebPODA."""

        headers = {
            "Authorization": f"Basic {self.__auth_code.get_secret_value()}",
        }

        start_value: str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_value: str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
        time_var = "ert" if ert else "time"

        # default to the pre-launch system ID if one is not passed in the URL
        url_base: str = (
            f"{urllib.parse.urljoin(self.__webpoda_url, 'packets/SID2/')}"
            if "packets/SID" not in self.__webpoda_url
            else self.__webpoda_url
        )
        url_base = url_base.rstrip("/")
        url = f"{url_base}/{packet}.{extension}?{time_var}%3E={start_value}&{time_var}%3C{end_value}&{data}"
        logger.debug(f"Downloading from: {url}")

        try:
            response: requests.Response = requests.get(
                url,
                headers=headers,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download from {url}: {e}")
            raise

        return response
