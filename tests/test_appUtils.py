"""Tests for app utilities."""

import pytest

from imap_mag.appUtils import getPacketFromApID

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def test_get_packet_from_apid_errors_on_invalid_apid() -> None:
    with pytest.raises(ValueError):
        getPacketFromApID(12345)
