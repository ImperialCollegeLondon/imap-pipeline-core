"""Tests for app utilities."""

from datetime import datetime, timedelta, timezone

import pytest

from imap_mag.util import (
    HKPacket,
    force_utc_timezone,
)
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


def test_name_all_packets() -> None:
    """Test listing all HK packets."""
    packets_list = HKPacket.names()

    assert len(packets_list) == 43


def test_get_packet_from_apid_success() -> None:
    packet = HKPacket.from_apid(1053)
    assert packet == HKPacket.SID15


def test_get_packet_from_apid_errors_on_invalid_apid() -> None:
    with pytest.raises(ValueError):
        HKPacket.from_apid(12345)


@pytest.mark.parametrize(
    "date",
    [
        datetime(2025, 3, 20, 10, 0, 0, tzinfo=timezone(timedelta(hours=-1))),
        datetime(2025, 3, 20, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2025, 3, 20, 10, 0, 0, tzinfo=None),
    ],
)
def test_force_remove_timezone(date) -> None:
    assert force_utc_timezone(date) == datetime(2025, 3, 20, 10, 0, 0)
