"""Tests for fetch IALiRT CLI command."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from imap_mag.cli.fetch.ialirt import fetch_ialirt, fetch_ialirt_hk


class TestFetchIalirt:
    def test_fetch_ialirt_returns_empty_when_no_data(
        self, dynamic_work_folder, clean_datastore
    ):
        mock_ialirt_client = MagicMock()
        mock_fetch = MagicMock()
        mock_fetch.download_mag_to_csv.return_value = {}

        with (
            patch(
                "imap_mag.cli.fetch.ialirt.IALiRTApiClient",
                return_value=mock_ialirt_client,
            ),
            patch("imap_mag.cli.fetch.ialirt.FetchIALiRT", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.ialirt.initialiseLoggingForCommand"),
        ):
            result = fetch_ialirt(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}

    def test_fetch_ialirt_hk_returns_empty_when_no_data(
        self, dynamic_work_folder, clean_datastore
    ):
        mock_ialirt_client = MagicMock()
        mock_fetch = MagicMock()
        mock_fetch.download_mag_hk_to_csv.return_value = {}

        with (
            patch(
                "imap_mag.cli.fetch.ialirt.IALiRTApiClient",
                return_value=mock_ialirt_client,
            ),
            patch("imap_mag.cli.fetch.ialirt.FetchIALiRT", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.ialirt.initialiseLoggingForCommand"),
        ):
            result = fetch_ialirt_hk(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}
