"""Program to retrieve and process MAG CDF files."""

import typing
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import typing_extensions

from ..client.sdcDataAccess import ISDCDataAccess
from ..outputManager import IOutputManager


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
    output_dir: str


class FetchScience:
    """Manage SOC data."""

    __data_access: ISDCDataAccess
    __output_manager: IOutputManager | None

    __modes: list[MAGMode]
    __sensor: list[MAGSensor]

    def __init__(
        self,
        data_access: ISDCDataAccess,
        output_manager: IOutputManager | None = None,
        modes: list[MAGMode] = ["norm", "burst"],
        sensors: list[MAGSensor] = ["magi", "mago"],
    ) -> None:
        """Initialize SDC interface."""

        self.__data_access = data_access
        self.__output_manager = output_manager
        self.__modes = modes
        self.__sensor = sensors

    def download_latest_science(
        self, **options: typing_extensions.Unpack[FetchScienceOptions]
    ) -> list[Path]:
        """Retrieve SDC data."""

        downloaded = []

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
                        descriptor=str(mode) + "-" + str(sensor),
                        start_date=date,
                        end_date=date,
                        version="latest",
                        extension="cdf",
                    )

                    if file_details is not None:
                        for file in file_details:
                            downloaded += [
                                self.__data_access.download(file["file_path"])
                            ]

                            if self.__output_manager is not None:
                                self.__output_manager.add_default_file(
                                    downloaded[-1],
                                    level=options["level"],
                                    descriptor=file["descriptor"],
                                    date=date,
                                    extension="cdf",
                                )

        return downloaded
