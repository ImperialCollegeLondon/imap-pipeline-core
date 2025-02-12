"""Tests for app utilities."""

import pytest

from imap_mag.appUtils import convertToDatetime, getPacketFromApID

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def test_get_packet_from_apid_errors_on_invalid_apid() -> None:
    with pytest.raises(ValueError):
        getPacketFromApID(12345)


def test_convert_to_datetime_on_invalid_datetime() -> None:
    with pytest.raises(ValueError):
        convertToDatetime("ABCDEF")
