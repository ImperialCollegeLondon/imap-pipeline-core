"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd

from imap_mag.client.sdcDataAccess import ISDCDataAccess
from imap_mag.outputManager import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


class MAGMode(str, Enum):
    Normal = "norm"
    Burst = "burst"


class MAGSensor(str, Enum):
    IBS = "magi"
    OBS = "mago"


class FetchScience:
    """Manage SOC data."""

    __data_access: ISDCDataAccess

    __modes: list[MAGMode]
    __sensor: list[MAGSensor]

    def __init__(
        self,
        data_access: ISDCDataAccess,
        modes: list[MAGMode] = [MAGMode.Normal, MAGMode.Burst],
        sensors: list[MAGSensor] = [MAGSensor.IBS, MAGSensor.OBS],
    ) -> None:
        """Initialize SDC interface."""

        self.__data_access = data_access
        self.__modes = modes
        self.__sensor = sensors

    def download_latest_science(
        self, level: str, start_date: datetime, end_date: datetime
    ) -> dict[Path, StandardSPDFMetadataProvider]:
        """Retrieve SDC data."""

        downloaded: dict[Path, StandardSPDFMetadataProvider] = dict()

        for mode in self.__modes:
            date_range: pd.DatetimeIndex = pd.date_range(
                start=start_date,
                end=end_date,
                freq="D",
                normalize=True,
            )

            for date in date_range.to_pydatetime():
                for sensor in self.__sensor:
                    file_details = self.__data_access.get_filename(
                        level=level,
                        descriptor=mode.value + "-" + sensor.value,
                        start_date=date,
                        end_date=date,
                        version="latest",
                        extension="cdf",
                    )

                    if file_details is not None:
                        for file in file_details:
                            downloaded_file = self.__data_access.download(
                                file["file_path"]
                            )

                            if downloaded_file.stat().st_size > 0:
                                logger.info(
                                    f"Downloaded file from SDC Data Access: {downloaded_file}"
                                )

                                downloaded[downloaded_file] = (
                                    StandardSPDFMetadataProvider(
                                        level=level,
                                        descriptor=file["descriptor"],
                                        date=date,
                                        extension="cdf",
                                    )
                                )
                            else:
                                logger.debug(
                                    f"Downloaded file {downloaded_file} is empty and will not be used."
                                )

        return downloaded
