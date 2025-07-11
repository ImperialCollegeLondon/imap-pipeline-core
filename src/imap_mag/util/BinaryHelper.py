import io
import os
from datetime import date
from pathlib import Path

import ccsdspy
from ccsdspy.utils import iter_packet_bytes
from rich.progress import Progress

from imap_mag.util.TimeConversion import TimeConversion


class BinaryHelper:
    """
    Class to handle binary data processing.
    """

    @staticmethod
    def get_apids_and_days(binary_file: Path) -> dict[int, set[date]]:
        """Retrieve SCLK days and ApIDs included in a binary file."""

        apids_by_day: dict[int, set[date]] = dict()

        packet_definition = ccsdspy.FixedLength(
            [
                ccsdspy.PacketField(name="SHCOARSE", data_type="uint", bit_length=32),
            ]
        )

        size = os.path.getsize(binary_file)

        with Progress(refresh_per_second=1) as progress:
            task = progress.add_task(
                f"Retrieving ApIDs and days in {binary_file}...", total=size
            )

            for packet_bytes in iter_packet_bytes(
                binary_file, include_primary_header=True
            ):
                progress.update(task, advance=len(packet_bytes))
                packet = packet_definition.load(
                    io.BytesIO(packet_bytes), include_primary_header=True
                )

                for apid in packet["CCSDS_APID"]:
                    apids_by_day.setdefault(int(apid), set()).update(
                        TimeConversion.convert_met_to_date(packet["SHCOARSE"])
                    )

        return apids_by_day

    @staticmethod
    def split_packets_by_day(binary_file: Path) -> dict[date, bytearray]:
        """Splits packets in a binary file by SCLK day."""

        packets_by_day: dict[date, bytearray] = dict()

        packet_definition = ccsdspy.FixedLength(
            [
                ccsdspy.PacketField(name="SHCOARSE", data_type="uint", bit_length=32),
            ]
        )

        size = os.path.getsize(binary_file)

        with Progress(refresh_per_second=1) as progress:
            task = progress.add_task(
                f"Splitting {binary_file} by S/C day...", total=size
            )

            for packet_bytes in iter_packet_bytes(
                binary_file, include_primary_header=True
            ):
                progress.update(task, advance=len(packet_bytes))

                packet = packet_definition.load(
                    io.BytesIO(packet_bytes), include_primary_header=True
                )

                day: list[date] = TimeConversion.convert_met_to_date(packet["SHCOARSE"])
                packets_by_day.setdefault(day[0], bytearray()).extend(packet_bytes)

        return packets_by_day
