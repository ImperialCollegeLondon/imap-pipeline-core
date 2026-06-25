"""Tests for fetch science CLI command."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from imap_mag.cli.fetch.science import fetch_science


class TestFetchScience:
    def test_fetch_science_returns_empty_when_no_data(
        self, dynamic_work_folder, clean_datastore
    ):
        mock_sdc = MagicMock()
        mock_fetch_science = MagicMock()
        mock_fetch_science.download_science.return_value = {}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess", return_value=mock_sdc),
            patch(
                "imap_mag.cli.fetch.science.FetchScience",
                return_value=mock_fetch_science,
            ),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
        ):
            result = fetch_science(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}
