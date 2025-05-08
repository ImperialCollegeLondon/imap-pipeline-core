import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig
from imap_mag.config.FetchMode import FetchMode
from imap_mag.db import Database
from imap_mag.io import (
    DatabaseFileOutputManager,
    IFileMetadataProvider,
    IOutputManager,
    OutputManager,
)
from imap_mag.util import CONSTANTS, DatetimeProvider

logger = logging.getLogger(__name__)


def convertMETToJ2000ns(
    met: np.typing.ArrayLike,
    reference_epoch: np.datetime64 = CONSTANTS.IMAP_EPOCH,
) -> np.typing.ArrayLike:
    """Convert mission elapsed time (MET) to nanoseconds from J2000."""
    time_array = (np.asarray(met, dtype=float) * 1e9).astype(np.int64)
    j2000_offset = (
        (reference_epoch - CONSTANTS.J2000_EPOCH)
        .astype("timedelta64[ns]")
        .astype(np.int64)
    )
    return j2000_offset + time_array


def forceUTCTimeZone(date: datetime) -> datetime:
    """Convert given datetime objects to UTC timezone and remove timezone."""
    return date.astimezone(timezone.utc).replace(tzinfo=None)


# TODO: Replace all uses of this with getOutputManagerByMode version
def getOutputManager(destination: appConfig.Destination) -> IOutputManager:
    """Retrieve output manager based on destination."""

    output_manager: IOutputManager = OutputManager(destination.folder)

    if destination.export_to_database:
        output_manager = DatabaseFileOutputManager(output_manager)

    return output_manager


def getOutputManagerByMode(destination_folder: Path, mode: FetchMode) -> IOutputManager:
    """Retrieve output manager based on destination and mode."""

    if mode == FetchMode.DownloadOnly:
        return OutputManager(destination_folder)
    elif mode == FetchMode.DownloadAndUpdateProgress:
        return DatabaseFileOutputManager(OutputManager(destination_folder))
    else:
        raise ValueError(f"Unsupported mode: {mode}")


def copyFileToDestination(
    file_path: Path,
    destination: appConfig.Destination,
    output_manager: Optional[OutputManager] = None,
) -> tuple[Path, IFileMetadataProvider]:
    """Copy file to destination folder."""

    class SimpleMetadataProvider(IFileMetadataProvider):
        """Simple metadata provider for compatibility."""

        def __init__(self, filename: str) -> None:
            self.filename = filename

        def supports_versioning(self) -> bool:
            return False

        def get_folder_structure(self) -> str:
            return ""

        def get_filename(self) -> str:
            return self.filename

    destination_folder = Path(destination.folder)

    if output_manager is None:
        output_manager = OutputManager(destination_folder)

    return output_manager.add_file(
        file_path, SimpleMetadataProvider(destination.filename)
    )


class DownloadDateManager:
    def __init__(
        self,
        packet_name: str,
        last_updated_date: datetime,
        logger: logging.Logger | logging.LoggerAdapter,
    ):
        self.__packet_name = packet_name
        self.__last_updated_date = last_updated_date
        self.__logger = logger

    def get_start_date(self, original_start_date: datetime | None) -> datetime | None:
        if original_start_date is None and self.__last_updated_date is None:
            self.__logger.info(
                f"Start date not provided. Using yesterday as default download date for {self.__packet_name}."
            )
            return DatetimeProvider.yesterday()
        elif original_start_date is None:
            self.__logger.info(
                f"Start date not provided. Using last updated date {self.__last_updated_date} for {self.__packet_name} from database."
            )
            return self.__last_updated_date
        else:
            self.__logger.info(
                f"Using provided start date {original_start_date} for {self.__packet_name}."
            )
            return forceUTCTimeZone(original_start_date)

    def get_end_date(self, original_end_date: datetime | None) -> datetime:
        if original_end_date is None:
            self.__logger.info(
                f"End date not provided. Using end of today as default download date for {self.__packet_name}."
            )
            return DatetimeProvider.end_of_today()
        else:
            self.__logger.info(
                f"Using provided end date {original_end_date} for {self.__packet_name}."
            )
            return forceUTCTimeZone(original_end_date)

    def validate_download_dates(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[datetime, datetime] | None:
        if self.__last_updated_date is None or self.__last_updated_date <= start_date:
            self.__logger.info(
                f"Packet {self.__packet_name} is not up to date. Downloading from {start_date}."
            )
        elif self.__last_updated_date >= end_date:
            self.__logger.info(
                f"Packet {self.__packet_name} is already up to date. Not downloading."
            )
            return None
        else:
            self.__logger.info(
                f"Packet {self.__packet_name} is partially up to date. Downloading from {self.__last_updated_date}."
            )
            start_date = self.__last_updated_date

        return start_date, end_date


def get_dates_for_download(
    *,
    packet_name: str,
    database: Database,
    original_start_date: datetime | None,
    original_end_date: datetime | None,
    check_and_update_database: bool,
    logger: logging.Logger | logging.LoggerAdapter,
) -> tuple[datetime, datetime] | None:
    download_progress = database.get_download_progress(packet_name)
    last_updated_date = download_progress.get_progress_timestamp()

    if check_and_update_database:
        download_progress.record_checked_download(DatetimeProvider.now())
        database.save(download_progress)

    manager = DownloadDateManager(packet_name, last_updated_date, logger)

    start_date = manager.get_start_date(original_start_date)
    end_date = manager.get_end_date(original_end_date)

    if start_date is None:
        return None

    if check_and_update_database:
        return manager.validate_download_dates(start_date, end_date)
    else:
        logger.info(
            f"Not checking database and forcing download from {start_date} to {end_date}."
        )
        return start_date, end_date
