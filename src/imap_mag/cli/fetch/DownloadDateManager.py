import logging
from datetime import datetime, timedelta

from imap_mag.db.Database import Database
from imap_mag.util.DatetimeProvider import DatetimeProvider

logger = logging.getLogger(__name__)


class DownloadDateManager:
    def __init__(
        self,
        packet_name: str,
        database: Database,
        *,
        earliest_date: datetime | None = None,
    ):
        self.__packet_name = packet_name
        self.__database = database
        self.__earliest_date = earliest_date

        self.__last_checked_date: datetime | None = None
        self.__progress_timestamp: datetime | None = None

    def _get_start_date(self, original_start_date: datetime | None) -> datetime:
        if original_start_date is not None:
            logger.info(
                f"Using provided start date {force_utc_timezone(original_start_date)} for {self.__packet_name}."
            )
            return force_utc_timezone(original_start_date)

        elif self.__progress_timestamp is not None:
            logger.info(
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
            logger.info(
                f"Start date not provided. Using {inferred} as default download date for {self.__packet_name}, as this packet has been checked at least once."
            )
            return inferred

        else:
            # If this is the first time the packet is downloaded, use the beginning of IMAP as the start date.
            earliest_date = self.__earliest_date or DatetimeProvider.beginning_of_imap()

            logger.info(
                f"Start date not provided. Using {earliest_date} as default download date for {self.__packet_name}, as this is the first time it is downloaded."
            )
            return earliest_date

    def _get_end_date(self, original_end_date: datetime | None) -> datetime:
        if original_end_date is None:
            logger.info(
                f"End date not provided. Using end of today as default download date for {self.__packet_name}."
            )
            return DatetimeProvider.end_of_today()
        else:
            logger.info(
                f"Using provided end date {force_utc_timezone(original_end_date)} for {self.__packet_name}."
            )
            return force_utc_timezone(original_end_date)

    def _validate_download_dates(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[datetime, datetime] | None:
        if self.__progress_timestamp is None or self.__progress_timestamp <= start_date:
            logger.info(
                f"Packet {self.__packet_name} is not up to date. Downloading from {start_date}."
            )
        elif self.__progress_timestamp >= end_date:
            logger.info(
                f"Packet {self.__packet_name} is already up to date. Not downloading."
            )
            return None
        else:
            logger.info(
                f"Packet {self.__packet_name} is partially up to date. Downloading from {self.__progress_timestamp}."
            )
            start_date = self.__progress_timestamp

        return start_date, end_date

    def get_dates_for_download(
        self,
        *,
        original_start_date: datetime | None,
        original_end_date: datetime | None,
        validate_with_database: bool,
    ) -> tuple[datetime, datetime] | None:
        workflow_progress = self.__database.get_workflow_progress(self.__packet_name)
        self.__last_checked_date = workflow_progress.get_last_checked_date()
        self.__progress_timestamp = workflow_progress.get_progress_timestamp()

        start_date = self._get_start_date(original_start_date)
        end_date = self._get_end_date(original_end_date)

        if validate_with_database:
            return self._validate_download_dates(start_date, end_date)
        else:
            logger.info(
                f"Not checking database and forcing download from {start_date} to {end_date}."
            )
            return start_date, end_date


def force_utc_timezone(date: datetime) -> datetime:
    """No matter the timezone of the input date, it will be treated as UTC."""
    return date.replace(tzinfo=None)
