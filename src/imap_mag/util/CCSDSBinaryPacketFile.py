import io
import logging
import os
from datetime import date
from pathlib import Path

import ccsdspy
from ccsdspy.utils import iter_packet_bytes
from rich.progress import Progress

from imap_mag.util.TimeConversion import TimeConversion

logger = logging.getLogger(__name__)


class CCSDSBinaryPacketFile:
    """
    Class to handle CCSDS binary data processing.
    """

    file: Path
    packet_definition: ccsdspy.FixedLength

    def __init__(self, file: Path):
        self.file = file
        self.packet_definition = ccsdspy.FixedLength(
            [
                ccsdspy.PacketField(name="SHCOARSE", data_type="uint", bit_length=32),
            ]
        )

    def get_apids(self) -> set[int]:
        """Retrieve all ApIDs."""

        apids: set[int] = set()

        for packet_bytes in iter_packet_bytes(self.file, include_primary_header=True):
            packet: dict | None = self.__load_bytes_with_definition(packet_bytes)

            if packet is None:
                continue

            apids.update(packet["CCSDS_APID"])

        return apids

    def get_days_by_apid(self) -> dict[int, set[date]]:
        """Retrieve SCLK days for each ApID."""

        days_by_apid: dict[int, set[date]] = dict()

        size = os.path.getsize(self.file)

        with Progress(refresh_per_second=1) as progress:
            task = progress.add_task(
                f"Retrieving ApIDs and days in {self.file}...", total=size
            )

            for packet_bytes in iter_packet_bytes(
                self.file, include_primary_header=True
            ):
                progress.update(task, advance=len(packet_bytes))
                packet: dict | None = self.__load_bytes_with_definition(packet_bytes)

                if packet is None:
                    continue

                for apid in packet["CCSDS_APID"]:
                    days_by_apid.setdefault(int(apid), set()).update(
                        TimeConversion.convert_met_to_date(packet["SHCOARSE"])
                    )

        return days_by_apid

    def split_packets_by_day(self) -> dict[date, bytearray]:
        """Splits packets in a binary file by SCLK day."""

        packets_by_day: dict[date, bytearray] = dict()

        size = os.path.getsize(self.file)

        with Progress(refresh_per_second=1) as progress:
            task = progress.add_task(f"Splitting {self.file} by S/C day...", total=size)

            for packet_bytes in iter_packet_bytes(
                self.file, include_primary_header=True
            ):
                progress.update(task, advance=len(packet_bytes))
                packet: dict | None = self.__load_bytes_with_definition(packet_bytes)

                if packet is None:
                    continue

                day: list[date] = TimeConversion.convert_met_to_date(packet["SHCOARSE"])
                packets_by_day.setdefault(day[0], bytearray()).extend(packet_bytes)

        return packets_by_day

    def __load_bytes_with_definition(self, packet_bytes: bytes) -> dict | None:
        """Load bytes with the packet definition, handling potential errors."""

        try:
            packet = self.packet_definition.load(
                io.BytesIO(packet_bytes), include_primary_header=True
            )
        except (IndexError, RuntimeError) as e:
            logger.error(
                f"Error decoding {len(packet_bytes)} bytes in {self.file}", exc_info=e
            )
            packet = None

        return packet

    @staticmethod
    def combine_days_by_apid(
        binary_files: list[Path],
    ) -> dict[int, set[date]]:
        """Retrieve SCLK days and ApIDs included in one or more binary files."""

        days_by_apid: dict[int, set[date]] = dict()

        for file in binary_files:
            file_apids = CCSDSBinaryPacketFile(file).get_days_by_apid()

            for apid, days in file_apids.items():
                days_by_apid.setdefault(apid, set()).update(days)

        return days_by_apid
