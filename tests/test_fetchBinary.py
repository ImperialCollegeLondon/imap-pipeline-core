"""Tests for `FetchBinary` class."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from imap_mag.cli.fetchBinary import FetchBinary, WebPODAMetadataProvider
from imap_mag.client.webPODA import IWebPODA
from tests.util.miscellaneous import (  # noqa: F401
    create_test_file,
    enableLogging,
    tidyDataFolders,
)


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
    actual_downloaded: dict[Path, WebPODAMetadataProvider] = (
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
        ert=False,
    )

    assert actual_downloaded == dict()


def test_fetch_binary_hk_added_to_output(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, "content")
    mock_poda.get_max_ert.side_effect = lambda **_: datetime(2025, 6, 3, 8, 58, 39)
    mock_poda.get_min_sctime.side_effect = lambda **_: datetime(2025, 5, 2, 12, 45, 29)

    # Exercise.
    actual_downloaded: dict[Path, WebPODAMetadataProvider] = (
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
        ert=False,
    )

    assert len(actual_downloaded) == 1

    assert test_file in actual_downloaded.keys()
    assert (
        WebPODAMetadataProvider(
            descriptor="hsk-pw",
            content_date=datetime(2025, 5, 2),
            extension="pkts",
            ert=datetime(2025, 6, 3, 8, 58, 39),
        )
        in actual_downloaded.values()
    )


@pytest.mark.parametrize(
    "start_date, end_date, expected_start_dates, expected_end_dates",
    [
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 2),
            [datetime(2025, 5, 2)],
            [datetime(2025, 5, 3)],
        ),
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 3),
            [datetime(2025, 5, 2), datetime(2025, 5, 3)],
            [datetime(2025, 5, 3), datetime(2025, 5, 4)],
        ),
        (
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 3),
            [datetime(2025, 5, 2, 12, 45, 29), datetime(2025, 5, 3)],
            [datetime(2025, 5, 3), datetime(2025, 5, 4)],
        ),
        (
            datetime(2025, 5, 2),
            datetime(2025, 5, 3, 12, 45, 29),
            [datetime(2025, 5, 2), datetime(2025, 5, 3)],
            [datetime(2025, 5, 3), datetime(2025, 5, 3, 12, 45, 29)],
        ),
        (
            datetime(2025, 5, 2, 12, 45, 29),
            datetime(2025, 5, 3, 12, 45, 29),
            [datetime(2025, 5, 2, 12, 45, 29), datetime(2025, 5, 3)],
            [datetime(2025, 5, 3), datetime(2025, 5, 3, 12, 45, 29)],
        ),
        (
            datetime(2025, 6, 2, 5, 30, 0),
            datetime(2025, 6, 2, 23, 59, 59, 999999),
            [datetime(2025, 6, 2, 5, 30, 0)],
            [datetime(2025, 6, 2, 23, 59, 59, 999999)],
        ),
    ],
)
def test_fetch_binary_different_start_end_dates(
    mock_poda: mock.Mock, start_date, end_date, expected_start_dates, expected_end_dates
) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, None)

    # Exercise.
    actual_downloaded: dict[Path, WebPODAMetadataProvider] = (
        fetchBinary.download_binaries(
            packet="MAG_HSK_PW",
            start_date=start_date,
            end_date=end_date,
        )
    )

    # Verify.
    assert mock_poda.download.call_count == len(expected_start_dates)

    calls = []

    for i in range(len(expected_start_dates)):
        calls.append(
            mock.call(
                packet="MAG_HSK_PW",
                start_date=expected_start_dates[i],
                end_date=expected_end_dates[i],
                ert=False,
            )
        )

    mock_poda.download.assert_has_calls(calls)

    assert actual_downloaded == dict()


def test_fetch_binary_with_ert_start_end_date(mock_poda: mock.Mock) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_test_file(test_file, "content")
    mock_poda.get_max_ert.side_effect = lambda **_: datetime(2025, 5, 2, 12, 45, 29)
    mock_poda.get_min_sctime.side_effect = lambda **_: datetime(2025, 4, 3, 8, 58, 39)

    # Exercise.
    actual_downloaded: dict[Path, WebPODAMetadataProvider] = (
        fetchBinary.download_binaries(
            packet="MAG_HSK_PW",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 2),
            use_ert=True,
        )
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
        ert=True,
    )

    assert len(actual_downloaded) == 1

    assert test_file in actual_downloaded.keys()
    assert (
        WebPODAMetadataProvider(
            descriptor="hsk-pw",
            content_date=datetime(2025, 4, 3),
            extension="pkts",
            ert=datetime(2025, 5, 2, 12, 45, 29),
        )
        in actual_downloaded.values()
    )
