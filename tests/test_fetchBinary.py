"""Tests for `FetchBinary` class."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from imap_mag.cli.fetchBinary import FetchBinary
from imap_mag.client.webPODA import IWebPODA
from imap_mag.outputManager import StandardSPDFMetadataProvider

from .testUtils import create_test_file, enableLogging, tidyDataFolders  # noqa: F401


@pytest.fixture
def mock_poda() -> mock.Mock:
    """Fixture for a mock IWebPODA instance."""
    return mock.create_autospec(IWebPODA, spec_set=True)


def test_fetch_binary_empty_download_not_added_to_output(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, None)

    # Exercise.
    actual_downloaded: dict[Path, StandardSPDFMetadataProvider] = (
        fetchBinary.download_binaries(
            packet="MAG_HSK_PW",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 2),
        )
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert actual_downloaded == dict()


def test_fetch_binary_with_same_start_end_date(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, "content")

    # Exercise.
    actual_downloaded: dict[Path, StandardSPDFMetadataProvider] = (
        fetchBinary.download_binaries(
            packet="MAG_HSK_PW",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 2),
        )
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    assert test_file in actual_downloaded.keys()
    assert (
        StandardSPDFMetadataProvider(
            descriptor="hsk-pw",
            date=datetime(2025, 5, 2),
            extension="pkts",
        )
        in actual_downloaded.values()
    )
