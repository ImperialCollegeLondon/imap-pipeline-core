"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from pathlib import Path

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.io.file import SciencePathHandler
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceLevel, ScienceMode

logger = logging.getLogger(__name__)


class FetchScience:
    """Download MAG science data from the SDC."""

    __data_access: SDCDataAccess

    def __init__(
        self,
        data_access: SDCDataAccess,
    ) -> None:
        self.__data_access = data_access

    def download_science(
        self,
        level: ScienceLevel,
        start_date: datetime,
        end_date: datetime,
        reference_frames: list[ReferenceFrame] | None = None,
        modes: list[ScienceMode] | None = None,
        sensors: list[MAGSensor] | None = None,
        use_ingestion_date: bool = False,
        max_downloads: int | None = None,
        skip_items_count: int = 0,
    ) -> dict[Path, SciencePathHandler]:
        """Retrieve SDC data."""

        downloaded: dict[Path, SciencePathHandler] = dict()
        max_downloads_reached = False

        if max_downloads is not None and max_downloads <= 0:
            raise ValueError("max_downloads must be greater than zero or None")

        if skip_items_count < 0:
            raise ValueError("skip_items_count must be zero or greater")

        dates: dict[str, datetime] = {
            "ingestion_start_date" if use_ingestion_date else "start_date": start_date,
            "ingestion_end_date" if use_ingestion_date else "end_date": end_date,
        }

        for descriptor in self.get_descriptors(
            level=level, modes=modes, sensors=sensors, reference_frames=reference_frames
        ):
            if max_downloads_reached:
                break

            file_details = self.__data_access.query_sdc_files(
                level=level.value,
                descriptor=descriptor,
                extension="cdf",
                **dates,  # type: ignore
            )

            # sort by ingestion date to ensure we process in cronological order
            file_details = sorted(
                file_details,
                key=lambda x: datetime.strptime(x["ingestion_date"], "%Y%m%d %H:%M:%S"),
            )

            for file in file_details if file_details else []:
                if skip_items_count > 0:
                    skip_items_count -= 1
                    logger.debug(
                        f"Skipping file {file['file_path']} as part of skip_items_count."
                    )
                    continue

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
                    max_downloads_reached = (
                        max_downloads is not None and len(downloaded) >= max_downloads
                    )
                    if max_downloads_reached:
                        logger.info(
                            f"Reached current batch limit of downloads ({max_downloads})"
                        )
                        break
                else:
                    logger.debug(
                        f"Downloaded file {downloaded_file} is empty and will not be used."
                    )

        return downloaded

    def get_descriptors(
        self,
        level: ScienceLevel | None,
        modes: list[ScienceMode] | None,
        sensors: list[MAGSensor] | None,
        reference_frames: list[ReferenceFrame] | None,
    ) -> list[str] | list[None]:
        """Get list of descriptors based on modes and reference frames."""
        descriptors: list[str] = []

        if level in [ScienceLevel.l1d, ScienceLevel.l2]:
            if (modes is not None and reference_frames is None) or (
                modes is None and reference_frames is not None
            ):
                raise ValueError(
                    "Both modes and reference_frames must be provided together."
                )

            if modes is not None and reference_frames is not None:
                descriptors = [
                    f"{m.short_name}-{rf.value}"
                    for m in modes
                    for rf in reference_frames
                ]

        elif level in [ScienceLevel.l1a, ScienceLevel.l1b, ScienceLevel.l1c]:
            if (modes is not None and sensors is None) or (
                modes is None and sensors is not None
            ):
                raise ValueError("Both modes and sensors must be provided together.")

            if modes is not None and sensors is not None:
                descriptors = [
                    f"{m.short_name}-{s.value}" for m in modes for s in sensors
                ]

        return descriptors if descriptors else [None]
