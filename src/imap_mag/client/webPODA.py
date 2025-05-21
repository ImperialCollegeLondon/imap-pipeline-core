"""Download raw packets from WebPODA."""

import abc
import logging
import os
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class IWebPODA(abc.ABC):
    """Interface for downloading raw packets from WebPODA."""

    @abc.abstractmethod
    def download(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> Path:
        """Download packet data from WebPODA."""
        pass

    @abc.abstractmethod
    def get_max_ert(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> datetime | None:
        """Get max ERT from WebPODA."""
        pass

    @abc.abstractmethod
    def get_min_sctime(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> datetime | None:
        """Get min S/C time from WebPODA."""
        pass


class WebPODA(IWebPODA):
    """Class for downloading raw packets from WebPODA."""

    __webpoda_url: str
    __auth_code: str
    __output_dir: Path

    def __init__(
        self, auth_code: str, output_dir: Path, webpoda_url: str | None = None
    ) -> None:
        """Initialize WebPODA interface."""

        self.__auth_code = auth_code
        self.__output_dir = output_dir
        self.__webpoda_url = (
            webpoda_url or "https://lasp.colorado.edu/ops/imap/poda/dap2/"
        )

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

    def get_min_sctime(
        self,
        *,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        ert: bool = False,
    ) -> datetime | None:
        logger.info(
            f"Downloading time information for {packet} from {start_date} to {end_date}."
        )

        time_response: requests.Response = self.__download_from_webpoda(
            packet,
            "csv",
            start_date,
            end_date,
            ert,
            "project(time)&formatTime(\"yyyy-MM-dd'T'HH:mm:ss\")",
        )

        time_info = time_response.content.decode("utf-8")
        lines = time_info.strip().splitlines()[1:]

        if not lines:
            logger.debug("No time data found.")
            min_time = None
        else:
            datetimes = [datetime.fromisoformat(line) for line in lines]
            min_time = min(datetimes)

            logger.debug(f"Min time: {min_time}")

        return min_time

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
            "Authorization": f"Basic {self.__auth_code}",
        }

        start_value: str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_value: str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
        time_var = "ert" if ert else "time"

        url = (
            f"{urllib.parse.urljoin(self.__webpoda_url, 'packets/SID2/')}"
            f"{packet}.{extension}?"
            f"{time_var}%3E={start_value}&"
            f"{time_var}%3C{end_value}&"
            f"{data}"
        )
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
