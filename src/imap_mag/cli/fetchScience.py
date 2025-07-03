"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from pathlib import Path

from imap_mag.client.sdcDataAccess import ISDCDataAccess
from imap_mag.io import ScienceMetadataProvider
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceLevel, ScienceMode

logger = logging.getLogger(__name__)


# TODO: why is this class in a folder named "cli" when it is not a command line app?
class FetchScience:
    """Manage SOC data."""

    __data_access: ISDCDataAccess

    __modes: list[ScienceMode]
    __sensor: list[MAGSensor]

    def __init__(
        self,
        data_access: ISDCDataAccess,
        modes: list[ScienceMode] = [ScienceMode.Normal, ScienceMode.Burst],
        sensors: list[MAGSensor] = [MAGSensor.IBS, MAGSensor.OBS],
    ) -> None:
        """Initialize SDC interface."""

        self.__data_access = data_access
        self.__modes = modes
        self.__sensor = sensors

    def download_latest_science(
        self,
        level: ScienceLevel,
        start_date: datetime,
        end_date: datetime,
        reference_frame: ReferenceFrame | None = None,
        use_ingestion_date: bool = False,
    ) -> dict[Path, ScienceMetadataProvider]:
        """Retrieve SDC data."""

        downloaded: dict[Path, ScienceMetadataProvider] = dict()

        dates: dict[str, datetime] = {
            "ingestion_start_date" if use_ingestion_date else "start_date": start_date,
            "ingestion_end_date" if use_ingestion_date else "end_date": end_date,
        }
        frame_suffix = ("-" + reference_frame.value) if reference_frame else ""

        if (level == ScienceLevel.l2) and (self.__sensor != [MAGSensor.OBS]):
            logger.debug("Forcing download of only OBS (mago) sensor for L2 data.")
            sensors: list[MAGSensor] = [MAGSensor.OBS]
        else:
            sensors = self.__sensor

        for mode in self.__modes:
            for sensor in sensors:
                sensor_suffix = "-" + sensor.value

                file_details = self.__data_access.get_filename(
                    level=level.value,
                    descriptor=mode.short_name
                    + (frame_suffix if (level == ScienceLevel.l2) else sensor_suffix),
                    extension="cdf",
                    **dates,  # type: ignore
                )

                if file_details is not None:
                    for file in file_details:
                        downloaded_file = self.__data_access.download(file["file_path"])

                        if downloaded_file.stat().st_size > 0:
                            logger.info(
                                f"Downloaded file from SDC Data Access: {downloaded_file}"
                            )

                            downloaded[downloaded_file] = ScienceMetadataProvider(
                                level=level.value,
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
