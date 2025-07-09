"""Program to retrieve and process MAG binary files."""

import io
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import ccsdspy
from ccsdspy.utils import iter_packet_bytes
from rich.progress import Progress

from imap_mag.client.WebPODA import WebPODA
from imap_mag.io import HKPathHandler
from imap_mag.util import HKLevel, TimeConversion

logger = logging.getLogger(__name__)


def split_packets_by_day(binary_file: Path) -> dict[date, bytearray]:
    packets_by_day: dict[date, bytearray] = dict()

    packet_definition = ccsdspy.FixedLength(
        [
            ccsdspy.PacketField(name="SHCOARSE", data_type="uint", bit_length=32),
        ]
    )

    size = os.path.getsize(binary_file)

    with Progress(refresh_per_second=1) as progress:
        task = progress.add_task(f"Splitting {binary_file} by S/C day...", total=size)

        for packet_bytes in iter_packet_bytes(binary_file, include_primary_header=True):
            progress.update(task, advance=len(packet_bytes))

            packet = packet_definition.load(
                io.BytesIO(packet_bytes), include_primary_header=True
            )

            day: list[date] = TimeConversion.convert_met_to_date(packet["SHCOARSE"])
            packets_by_day.setdefault(day[0], bytearray()).extend(packet_bytes)

    return packets_by_day


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
        packets_by_day: dict[date, bytearray] = split_packets_by_day(file)

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
