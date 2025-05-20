import logging
from datetime import datetime, timedelta

from imap_mag.db import Database
from imap_mag.util.DatetimeProvider import DatetimeProvider

logger = logging.getLogger(__name__)


def force_utc_timezone(date: datetime) -> datetime:
    """No matter the timezone of the input date, it will be treated as UTC."""
    return date.replace(tzinfo=None)


class DownloadDateManager:
    def __init__(
        self,
        packet_name: str,
        last_checked_date: datetime | None,
        progress_timestamp: datetime | None,
        logger: logging.Logger | logging.LoggerAdapter,
    ):
        self.__packet_name = packet_name
        self.__last_checked_date = last_checked_date
        self.__progress_timestamp = progress_timestamp
        self.__logger = logger

    def get_start_date(self, original_start_date: datetime | None) -> datetime:
        if original_start_date is not None:
            self.__logger.info(
                f"Using provided start date {original_start_date} for {self.__packet_name}."
            )
            return force_utc_timezone(original_start_date)

        elif self.__progress_timestamp is not None:
            self.__logger.info(
                f"Start date not provided. Using last updated date {self.__progress_timestamp} for {self.__packet_name} from database."
            )
            return self.__progress_timestamp

        elif self.__last_checked_date is not None:
            # If the packet has been checked at least once, even though no data was downloaded last time, use yesterday or the last checked date,
            # whichever comes first, as the start date.
            inferred = min(
                DatetimeProvider.yesterday(),
                self.__last_checked_date - timedelta(hours=1),
            )
            self.__logger.info(
                f"Start date not provided. Using {inferred} as default download date for {self.__packet_name}, as this packet has been checked at least once."
            )
            return inferred

        else:
            # If this is the first time the packet is downloaded, use the beginning of IMAP as the start date.
            self.__logger.info(
                f"Start date not provided. Using {DatetimeProvider.beginning_of_imap()} as default download date for {self.__packet_name}, as this is the first time it is downloaded."
            )
            return DatetimeProvider.beginning_of_imap()

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
            return force_utc_timezone(original_end_date)

    def validate_download_dates(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[datetime, datetime] | None:
        if self.__progress_timestamp is None or self.__progress_timestamp <= start_date:
            self.__logger.info(
                f"Packet {self.__packet_name} is not up to date. Downloading from {start_date}."
            )
        elif self.__progress_timestamp >= end_date:
            self.__logger.info(
                f"Packet {self.__packet_name} is already up to date. Not downloading."
            )
            return None
        else:
            self.__logger.info(
                f"Packet {self.__packet_name} is partially up to date. Downloading from {self.__progress_timestamp}."
            )
            start_date = self.__progress_timestamp

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

    last_checked_date = download_progress.get_last_checked_date()
    progress_timestamp = download_progress.get_progress_timestamp()

    if check_and_update_database:
        download_progress.record_checked_download(DatetimeProvider.now())
        database.save(download_progress)

    manager = DownloadDateManager(
        packet_name, last_checked_date, progress_timestamp, logger
    )

    start_date = manager.get_start_date(original_start_date)
    end_date = manager.get_end_date(original_end_date)

    if check_and_update_database:
        return manager.validate_download_dates(start_date, end_date)
    else:
        logger.info(
            f"Not checking database and forcing download from {start_date} to {end_date}."
        )
        return start_date, end_date
