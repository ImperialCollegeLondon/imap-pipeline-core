"""Program to retrieve and process MAG CDF files."""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from imap_mag.client.sdcDataAccess import ISDCDataAccess
from imap_mag.outputManager import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


# TODO: Move these Enums to their own files or to a constants file
class MAGMode(str, Enum):
    Normal = "norm"
    Burst = "burst"


class MAGSensor(str, Enum):
    IBS = "magi"
    OBS = "mago"


@dataclass
class SDCMetadataProvider(StandardSPDFMetadataProvider):
    """
    Metadata for SDC files.
    """

    ingestion_date: datetime | None = None  # date data was ingested by SDC


# TODO: why is this class in a folder named "cli" when it is not a command line app?
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
        self,
        level: str,
        start_date: datetime,
        end_date: datetime,
        use_ingestion_date: bool = False,
    ) -> dict[Path, SDCMetadataProvider]:
        """Retrieve SDC data."""

        downloaded: dict[Path, SDCMetadataProvider] = dict()

        dates: dict[str, datetime] = {
            "ingestion_start_date" if use_ingestion_date else "start_date": start_date,
            "ingestion_end_date" if use_ingestion_date else "end_date": end_date,
        }

        for mode in self.__modes:
            for sensor in self.__sensor:
                file_details = self.__data_access.get_filename(
                    level=level,
                    descriptor=mode.value + "-" + sensor.value,
                    extension="cdf",
                    **dates,
                )

                if file_details is not None:
                    for file in file_details:
                        downloaded_file = self.__data_access.download(file["file_path"])

                        if downloaded_file.stat().st_size > 0:
                            logger.info(
                                f"Downloaded file from SDC Data Access: {downloaded_file}"
                            )

                            downloaded[downloaded_file] = SDCMetadataProvider(
                                level=level,
                                descriptor=file["descriptor"],
                                content_date=datetime.strptime(
                                    file["start_date"], "%Y%m%d"
                                ),
                                ingestion_date=datetime.strptime(
                                    file["ingestion_date"], "%Y%m%d %H:%M:%S"
                                ),
                                version=int(file["version"].lstrip("v")),
                                extension="cdf",
                            )
                        else:
                            logger.debug(
                                f"Downloaded file {downloaded_file} is empty and will not be used."
                            )

        return downloaded
