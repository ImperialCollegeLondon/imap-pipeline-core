"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from pathlib import Path

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.io.file import SciencePathHandler
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceLevel, ScienceMode

logger = logging.getLogger(__name__)


class FetchScience:
    """Manage SOC data."""

    __data_access: SDCDataAccess

    __modes: list[ScienceMode] | None
    __sensor: list[MAGSensor] | None

    def __init__(
        self,
        data_access: SDCDataAccess,
        modes: list[ScienceMode] | None,
        sensors: list[MAGSensor] | None,
    ) -> None:
        """Initialize SDC interface."""

        self.__data_access = data_access
        self.__modes = modes
        self.__sensor = sensors

    def download_science(
        self,
        level: ScienceLevel,
        start_date: datetime,
        end_date: datetime,
        reference_frames: list[ReferenceFrame] | None = None,
        use_ingestion_date: bool = False,
    ) -> dict[Path, SciencePathHandler]:
        """Retrieve SDC data."""

        downloaded: dict[Path, SciencePathHandler] = dict()

        dates: dict[str, datetime] = {
            "ingestion_start_date" if use_ingestion_date else "start_date": start_date,
            "ingestion_end_date" if use_ingestion_date else "end_date": end_date,
        }

        for descriptor in self.get_descriptors(
            reference_frames=reference_frames, level=level
        ):
            file_details = self.__data_access.query_sdc_files(
                level=level.value,
                descriptor=descriptor,
                extension="cdf",
                **dates,  # type: ignore
            )

            for file in file_details if file_details else []:
                downloaded_file = self.__data_access.download(file["file_path"])

                if downloaded_file.stat().st_size > 0:
                    logger.info(
                        f"Downloaded file from SDC Data Access: {downloaded_file}"
                    )

                    downloaded[downloaded_file] = SciencePathHandler(
                        level=level.value,
                        descriptor=file["descriptor"],
                        content_date=datetime.strptime(file["start_date"], "%Y%m%d"),
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

    def get_descriptors(
        self,
        reference_frames: list[ReferenceFrame] | None = None,
        level: ScienceLevel | None = None,
    ) -> list[str] | list[None]:
        """Get list of descriptors based on modes and reference frames."""
        descriptors: list[str] = []

        for mode in self.__modes if self.__modes else []:
            descriptors.append(mode.short_name)

        if self.__sensor:
            if descriptors:
                descriptors = [
                    descriptor + "-" + sensor.value
                    for descriptor in descriptors
                    for sensor in self.__sensor
                ]
            else:
                descriptors.extend([sensor.value for sensor in self.__sensor])

        if reference_frames:
            if descriptors:
                descriptors = [
                    descriptor + "-" + reference_frame.value
                    for descriptor in descriptors
                    for reference_frame in reference_frames
                ]
            else:
                descriptors.extend(
                    [reference_frame.value for reference_frame in reference_frames]
                )

        return descriptors if descriptors else [None]
