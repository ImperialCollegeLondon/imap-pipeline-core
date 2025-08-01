from datetime import datetime

import pytest

from imap_mag.io.file import (
    HKBinaryPathHandler,
    HKDecodedPathHandler,
)
from imap_mag.util import HKLevel, HKPacket
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


def test_hk_binary_path_handler_is_l0_only():
    handler = HKBinaryPathHandler()
    assert handler.level == HKLevel.l0.value


def test_hk_decoded_path_handler_is_l1_only():
    handler = HKDecodedPathHandler()
    assert handler.level == HKLevel.l1.value


@pytest.mark.parametrize("packet", [p for p in HKPacket])
def test_hk_binary_path_handler_supports_all_hk_packets(packet: HKPacket):
    # Set up.
    filename = f"imap_mag_l0_{HKBinaryPathHandler.convert_packet_to_descriptor(packet.packet)}_20241210_003.pkts"
    expected_handler = HKBinaryPathHandler(
        descriptor=HKBinaryPathHandler.convert_packet_to_descriptor(packet.packet),
        content_date=datetime(2024, 12, 10),
        part=3,
        extension="pkts",
    )

    # Exercise.
    actual_handler = HKBinaryPathHandler.from_filename(filename)

    # Verify.
    assert actual_handler == expected_handler


@pytest.mark.parametrize("packet", [p for p in HKPacket])
def test_hk_decoded_path_handler_supports_all_hk_packets(packet: HKPacket):
    # Set up.
    filename = f"imap_mag_l1_{HKDecodedPathHandler.convert_packet_to_descriptor(packet.packet)}_20241210_v003.pkts"
    expected_handler = HKDecodedPathHandler(
        descriptor=HKDecodedPathHandler.convert_packet_to_descriptor(packet.packet),
        content_date=datetime(2024, 12, 10),
        version=3,
        extension="pkts",
    )

    # Exercise.
    actual_handler = HKDecodedPathHandler.from_filename(filename)

    # Verify.
    assert actual_handler == expected_handler


def test_hk_binary_path_handler_does_not_support_version_numbers():
    # Set up.
    filename = "imap_mag_l0_hsk-pw_20241210_v003.pkts"

    # Exercise.
    handler = HKBinaryPathHandler.from_filename(filename)

    # Verify.
    assert handler is None


def test_hk_decoded_path_handler_does_not_support_part_numbers():
    # Set up.
    filename = "imap_mag_l1_hsk-pw_20241210_003.pkts"

    # Exercise.
    handler = HKDecodedPathHandler.from_filename(filename)

    # Verify.
    assert handler is None
