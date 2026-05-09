"""Tests for fetch binary CLI command."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.fetch.binary import fetch_binary
from imap_mag.util import HKPacket


class TestFetchBinary:
    def test_raises_when_neither_apid_nor_packet_provided(self, dynamic_work_folder):
        with pytest.raises(ValueError, match="Must provide either"):
            fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=None,
                packet=None,
            )

    def test_raises_when_both_apid_and_packet_provided(self, dynamic_work_folder):
        with pytest.raises(ValueError, match="Must provide either"):
            fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=1063,
                packet=HKPacket.SID1,
            )

    def test_downloads_using_apid(self, dynamic_work_folder, clean_datastore):
        mock_poda = MagicMock()
        mock_fetch_binary = MagicMock()
        mock_fetch_binary.download_binaries.return_value = {}

        with (
            patch("imap_mag.cli.fetch.binary.WebPODA", return_value=mock_poda),
            patch(
                "imap_mag.cli.fetch.binary.FetchBinary", return_value=mock_fetch_binary
            ),
            patch("imap_mag.cli.fetch.binary.initialiseLoggingForCommand"),
        ):
            result = fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=1063,
            )

        assert isinstance(result, dict)

    def test_downloads_using_packet_name(self, dynamic_work_folder, clean_datastore):
        mock_poda = MagicMock()
        mock_fetch_binary = MagicMock()
        mock_fetch_binary.download_binaries.return_value = {}

        with (
            patch("imap_mag.cli.fetch.binary.WebPODA", return_value=mock_poda),
            patch(
                "imap_mag.cli.fetch.binary.FetchBinary", return_value=mock_fetch_binary
            ),
            patch("imap_mag.cli.fetch.binary.initialiseLoggingForCommand"),
        ):
            result = fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                packet=HKPacket.SID1,
            )

        assert isinstance(result, dict)
