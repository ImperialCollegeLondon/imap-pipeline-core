"""Tests for app utilities."""

import logging

import pytest

from imap_mag.util import HKPacket

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401

LOGGER = logging.getLogger(__name__)


def test_get_packet_from_apid_errors_on_invalid_apid() -> None:
    with pytest.raises(ValueError):
        HKPacket.from_apid(12345)
