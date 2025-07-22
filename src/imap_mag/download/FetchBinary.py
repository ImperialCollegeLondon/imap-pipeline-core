"""Program to retrieve and process MAG binary files."""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from imap_mag.client.WebPODA import WebPODA
from imap_mag.io import HKPathHandler
from imap_mag.util import CCSDSBinaryPacketFile, HKLevel

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

        if file.stat().st_size == 0:
            logger.debug(f"Downloaded file {file} is empty and will not be used.")
            return downloaded

        logger.info(f"Downloaded file from WebPODA: {file}")

        max_ert: datetime | None = self.__web_poda.get_max_ert(
            packet=packet,
            start_date=start_date,
            end_date=end_date,
            ert=use_ert,
        )

        # Split the binary file by S/C day.
        packets_by_day: dict[date, bytearray] = CCSDSBinaryPacketFile(
            file
        ).split_packets_by_day()

        for day, packet_bytes in packets_by_day.items():
            logger.debug(
                f"Processing {len(packet_bytes)} bytes for {day.strftime('%Y-%m-%d')}."
            )

            day_file = file.parent / f"{packet}_{day.strftime('%Y%m%d')}_sclk.bin"
            with open(day_file, "wb") as f:
                f.write(packet_bytes)

            downloaded[day_file] = HKPathHandler(
                level=HKLevel.l0.value,
                descriptor=HKPathHandler.convert_packet_to_descriptor(packet),
                content_date=datetime.combine(day, datetime.min.time()),
                ert=max_ert,
                extension="pkts",
            )

        return downloaded
