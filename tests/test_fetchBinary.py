"""Tests for `FetchBinary` class."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from imap_mag.client.WebPODA import WebPODA
from imap_mag.download.FetchBinary import FetchBinary
from imap_mag.io.file import HKBinaryPathHandler
from tests.util.miscellaneous import (
    TEST_DATA,
    create_test_file,
)


@pytest.fixture
def mock_poda() -> mock.Mock:
    """Fixture for a mock WebPODA instance."""
    return mock.create_autospec(WebPODA, spec_set=True)


def test_fetch_binary_empty_download_not_added_to_output(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, None)

    # Exercise.
    actual_downloaded: dict[Path, HKBinaryPathHandler] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
        ert=False,
    )

    assert actual_downloaded == dict()


def test_fetch_binary_hk_added_to_output(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = TEST_DATA / "MAG_HSK_PW.pkts"
    expected_file = TEST_DATA / "MAG_HSK_PW_20250502_sclk.bin"

    mock_poda.download.side_effect = lambda **_: test_file
    mock_poda.get_max_ert.side_effect = lambda **_: datetime(2025, 6, 3, 8, 58, 39)

    # Exercise.
    actual_downloaded: dict[Path, HKBinaryPathHandler] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
    )

    # Verify.
    try:
        mock_poda.download.assert_called_once_with(
            packet="MAG_HSK_PW",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
            ert=False,
        )

        assert len(actual_downloaded) == 1

        assert expected_file in actual_downloaded.keys()
        assert (
            HKBinaryPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2025, 5, 2),
                extension="pkts",
                ert=datetime(2025, 6, 3, 8, 58, 39),
            )
            in actual_downloaded.values()
        )
    finally:
        expected_file.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "start_date, end_date, expected_start_date, expected_end_date",
    [
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 2),
            datetime(2025, 5, 2),
            datetime(2025, 5, 3),
        ),
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 3),
            datetime(2025, 5, 2),
            datetime(2025, 5, 4),
        ),
        (
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 3),
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 4),
        ),
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 3, 12, 45, 29),
            datetime(2025, 5, 2),
            datetime(2025, 5, 3, 12, 45, 29),
        ),
        (
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 3, 12, 45, 29),
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 3, 12, 45, 29),
        ),
        (
            datetime(2025, 6, 2, 5, 30, 0),
            datetime(2025, 6, 2, 23, 59, 59, 999999),
            datetime(2025, 6, 2, 5, 30, 0),
            datetime(2025, 6, 2, 23, 59, 59, 999999),
        ),
    ],
)
def test_fetch_binary_different_start_end_dates(
    mock_poda: mock.Mock, start_date, end_date, expected_start_date, expected_end_date
) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, None)

    # Exercise.
    actual_downloaded: dict[Path, HKBinaryPathHandler] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=start_date,
        end_date=end_date,
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=expected_start_date,
        end_date=expected_end_date,
        ert=False,
    )

    assert actual_downloaded == dict()


def test_fetch_binary_with_ert_start_end_date(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = TEST_DATA / "MAG_HSK_PW.pkts"
    expected_file = TEST_DATA / "MAG_HSK_PW_20250502_sclk.bin"

    mock_poda.download.side_effect = lambda **_: test_file
    mock_poda.get_max_ert.side_effect = lambda **_: datetime(2025, 6, 2, 12, 45, 29)

    # Exercise.
    actual_downloaded: dict[Path, HKBinaryPathHandler] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
        use_ert=True,
    )

    # Verify.
    try:
        mock_poda.download.assert_called_once_with(
            packet="MAG_HSK_PW",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
            ert=True,
        )

        assert len(actual_downloaded) == 1

        assert expected_file in actual_downloaded.keys()
        assert (
            HKBinaryPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2025, 5, 2),
                extension="pkts",
                ert=datetime(2025, 6, 2, 12, 45, 29),
            )
            in actual_downloaded.values()
        )
    finally:
        expected_file.unlink(missing_ok=True)
