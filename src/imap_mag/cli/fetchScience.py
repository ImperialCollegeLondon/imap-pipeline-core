"""Program to retrieve and process MAG CDF files."""

import logging
import typing
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import typing_extensions

from imap_mag.client.sdcDataAccess import ISDCDataAccess
from imap_mag.outputManager import StandardSPDFMetadataProvider


class MAGMode(str, Enum):
    Normal = "norm"
    Burst = "burst"


class MAGSensor(str, Enum):
    IBS = "magi"
    OBS = "mago"


class FetchScienceOptions(typing.TypedDict):
    """Options for SOC interactions."""

    level: str
    start_date: datetime
    end_date: datetime


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
        self, **options: typing_extensions.Unpack[FetchScienceOptions]
    ) -> dict[Path, StandardSPDFMetadataProvider]:
        """Retrieve SDC data."""

        downloaded: dict[Path, StandardSPDFMetadataProvider] = dict()

        for mode in self.__modes:
            date_range: pd.DatetimeIndex = pd.date_range(
                start=options["start_date"],
                end=options["end_date"],
                freq="D",
                normalize=True,
            )

            for date in date_range.to_pydatetime():
                for sensor in self.__sensor:
                    file_details = self.__data_access.get_filename(
                        level=options["level"],
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
                                logging.info(
                                    f"Downloaded file from SDC Data Access: {downloaded_file}"
                                )

                                downloaded[downloaded_file] = (
                                    StandardSPDFMetadataProvider(
                                        level=options["level"],
                                        descriptor=file["descriptor"],
                                        date=date,
                                        extension="cdf",
                                    )
                                )
                            else:
                                logging.debug(
                                    f"Downloaded file {downloaded_file} is empty and will not be used."
                                )

        return downloaded
