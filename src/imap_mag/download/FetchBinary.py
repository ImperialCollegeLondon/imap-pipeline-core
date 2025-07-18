"""Program to retrieve and process MAG binary files."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from imap_mag.client.WebPODA import WebPODA
from imap_mag.io import HKPathHandler
from imap_mag.util import HKLevel

logger = logging.getLogger(__name__)


class FetchBinary:
    """Manage WebPODA data."""

    __web_poda: WebPODA

    def __init__(
        self,
        web_poda: WebPODA,
    ) -> None:
        """Initialize WebPODA interface."""

        self.__web_poda = web_poda

    def download_binaries(
        self,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        use_ert: bool = False,
    ) -> dict[Path, HKPathHandler]:
        """Retrieve WebPODA data."""

        downloaded: dict[Path, HKPathHandler] = dict()

        # If the start and end dates are the same, download all the data for that day.
        if start_date == end_date:
            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = start_date + timedelta(days=1)

        # If the end date is midnight, include the whole day.
        elif end_date.time() == datetime.min.time():
            end_date = end_date + timedelta(days=1)

        # Download data as a whole.
        file = self.__web_poda.download(
            packet=packet,
            start_date=start_date,
            end_date=end_date,
            ert=use_ert,
        )

        if file.stat().st_size > 0:
            logger.info(f"Downloaded file from WebPODA: {file}")

            max_ert: datetime | None = self.__web_poda.get_max_ert(
                packet=packet,
                start_date=start_date,
                end_date=end_date,
                ert=use_ert,
            )
            min_time: datetime | None = self.__web_poda.get_min_sctime(
                packet=packet,
                start_date=start_date,
                end_date=end_date,
                ert=use_ert,
            )

            downloaded[file] = HKPathHandler(
                level=HKLevel.l0.value,
                descriptor=HKPathHandler.convert_packet_to_descriptor(packet),
                content_date=(
                    min_time.replace(hour=0, minute=0, second=0) if min_time else None
                ),
                ert=max_ert,
                extension="pkts",
            )
        else:
            logger.debug(f"Downloaded file {file} is empty and will not be used.")

        return downloaded
