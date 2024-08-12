"""Tests for app utilities."""

import pytest
import typer
from imap_mag.appUtils import getPacketFromApID

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def test_get_packet_from_apid_errors_on_invalid_apid() -> None:
    with pytest.raises(typer.Abort):
        getPacketFromApID(12345)


def test_convert_to_datetime_on_invalid_datetime() -> None:
    with pytest.raises(typer.Abort):
        getPacketFromApID("ABCDEF")
