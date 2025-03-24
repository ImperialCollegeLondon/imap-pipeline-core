import logging
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig
from imap_mag.DB import Database, DatabaseFileOutputManager
from imap_mag.outputManager import IFileMetadataProvider, IOutputManager, OutputManager

logger = logging.getLogger(__name__)

IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")

APID_TO_PACKET: dict[int, str] = {
    1028: "MAG_HSK_SID1",
    1055: "MAG_HSK_SID2",
    1063: "MAG_HSK_PW",
    1064: "MAG_HSK_STATUS",
    1082: "MAG_HSK_SCI",
    1051: "MAG_HSK_PROCSTAT",
    1060: "MAG_HSK_SID12",
    1053: "MAG_HSK_SID15",
    1054: "MAG_HSK_SID16",
    1045: "MAG_HSK_SID20",
}

HKPacket = Enum("HKPacket", [(value, value) for value in APID_TO_PACKET.values()])  # type: ignore
HK_PACKETS: list[str] = [e.value for e in HKPacket]  # type: ignore


def convertMETToJ2000ns(
    met: np.typing.ArrayLike,
    reference_epoch: np.datetime64 = IMAP_EPOCH,
) -> np.typing.ArrayLike:
    """Convert mission elapsed time (MET) to nanoseconds from J2000."""
    time_array = (np.asarray(met, dtype=float) * 1e9).astype(np.int64)
    j2000_offset = (
        (reference_epoch - J2000_EPOCH).astype("timedelta64[ns]").astype(np.int64)
    )
    return j2000_offset + time_array


def getPacketFromApID(apid: int) -> str:
    """Get packet name from ApID."""
    if apid not in APID_TO_PACKET:
        logger.critical(f"ApID {apid} does not match any known packet.")
        raise ValueError(f"ApID {apid} does not match any known packet.")
    return APID_TO_PACKET[apid]


def forceUTCTimeZone(date: datetime) -> datetime:
    """Convert given datetime objects to UTC timezone and remove timezone."""
    return date.astimezone(timezone.utc).replace(tzinfo=None)


def getOutputManager(destination: appConfig.Destination) -> IOutputManager:
    """Retrieve output manager based on destination."""

    output_manager: IOutputManager = OutputManager(destination.folder)

    if destination.export_to_database:
        output_manager = DatabaseFileOutputManager(output_manager)

    return output_manager


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


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library."""

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    @staticmethod
    def today(type=datetime) -> date:
        return type.today()

    @staticmethod
    def tomorrow(type=datetime) -> date:
        return type.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)

    @staticmethod
    def yesterday(type=datetime) -> date:
        return type.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    @staticmethod
    def end_of_today() -> datetime:
        return datetime.today().replace(
            hour=23, minute=59, second=59, microsecond=999999
        )


class DownloadDateManager:
    def __init__(
        self,
        packet_name: str,
        database: Database,
        logger: logging.Logger | logging.LoggerAdapter,
        check_and_update_database: bool,
    ):
        self.__packet_name = packet_name
        self.__database = database
        self.__logger = logger
        self.__check_and_update_database = check_and_update_database

        self.__download_progress = self.__database.get_download_progress(packet_name)
        self.__last_updated_date = self.__download_progress.get_progress_timestamp()

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

    def update_database_progress(self):
        if self.__check_and_update_database:
            self.__download_progress.record_checked_download(DatetimeProvider.now())
            self.__database.save(self.__download_progress)

    def validate_download_dates(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[datetime, datetime] | None:
        if not self.__check_and_update_database:
            self.__logger.info(
                f"Not checking database and forcing download from {start_date} to {end_date}."
            )
            return start_date, end_date

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


def get_start_and_end_dates_for_download(
    *,
    packet_name: str,
    database: Database,
    original_start_date: datetime | None,
    original_end_date: datetime | None,
    check_and_update_database: bool,
    logger: logging.Logger | logging.LoggerAdapter,
) -> tuple[datetime, datetime] | None:
    manager = DownloadDateManager(
        packet_name, database, logger, check_and_update_database
    )
    manager.update_database_progress()

    end_date = manager.get_end_date(original_end_date)
    start_date = manager.get_start_date(original_start_date)

    if start_date is None:
        return None

    return manager.validate_download_dates(start_date, end_date)


def update_database_with_progress(
    packet_name: str,
    database: Database,
    latest_timestamp: datetime,
    check_and_update_database: bool,
    logger: logging.Logger | logging.LoggerAdapter,
) -> None:
    download_progress = database.get_download_progress(packet_name)

    logger.debug(
        f"Latest downloaded timestamp for packet {packet_name} is {latest_timestamp}."
    )

    if check_and_update_database and (
        (download_progress.progress_timestamp is None)
        or (latest_timestamp > download_progress.progress_timestamp)
    ):
        download_progress.record_successful_download(latest_timestamp)
        database.save(download_progress)
    else:
        logger.info(f"Database not updated for {packet_name}.")
