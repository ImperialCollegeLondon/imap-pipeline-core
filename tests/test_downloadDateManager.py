import logging
from datetime import datetime, timedelta
from unittest import mock

import pytest

from imap_db.model import DownloadProgress
from imap_mag.db import Database
from imap_mag.util import get_dates_for_download
from tests.util.miscellaneous import (  # noqa: F401  # noqa: F401
    BEGINNING_OF_IMAP,
    END_OF_TODAY,
    NOW,
    YESTERDAY,
    mock_datetime_provider,
    tidyDataFolders,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock Database instance."""
    return mock.create_autospec(Database, spec_set=True)


@pytest.mark.parametrize("validate_with_database", [True, False])
def test_get_start_end_dates_no_dates_defined_empty_database(
    capture_cli_logs,
    mock_database,
    validate_with_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        validate_with_database=validate_with_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == BEGINNING_OF_IMAP
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in capture_cli_logs.text
    )
    assert (
        f"Start date not provided. Using {BEGINNING_OF_IMAP} as default download date for MAG_SCI_NORM, as this is the first time it is downloaded"
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


@pytest.mark.parametrize("validate_with_database", [True, False])
def test_get_start_end_dates_end_date_defined_empty_database(
    capture_cli_logs,
    mock_database,
    validate_with_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=original_end_date,
        validate_with_database=validate_with_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == BEGINNING_OF_IMAP
    assert end_date == original_end_date

    assert "Using provided end date" in capture_cli_logs.text
    assert (
        f"Start date not provided. Using {BEGINNING_OF_IMAP} as default download date for MAG_SCI_NORM, as this is the first time it is downloaded"
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


def test_get_start_end_dates_both_dates_defined_empty_database(
    capture_cli_logs,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = datetime(2025, 2, 12, 0, 0, 0)
    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        validate_with_database=False,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == original_start_date
    assert end_date == original_end_date

    assert "Using provided end date" in capture_cli_logs.text
    assert "Using provided start date" in capture_cli_logs.text
    assert (
        f"Not checking database and forcing download from {original_start_date} to {original_end_date}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


@pytest.mark.parametrize("validate_with_database", [True, False])
def test_get_start_end_dates_no_dates_defined_with_progress_timestamp(
    capture_cli_logs,
    mock_database,
    validate_with_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.progress_timestamp = datetime(2025, 3, 21, 12, 45, 7)

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        validate_with_database=validate_with_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == download_progress.progress_timestamp
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in capture_cli_logs.text
    )
    assert (
        f"Start date not provided. Using last updated date {download_progress.progress_timestamp} for MAG_SCI_NORM from database."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


@pytest.mark.parametrize("validate_with_database", [True, False])
def test_get_start_end_dates_no_dates_defined_with_last_checked_date(
    capture_cli_logs,
    mock_database,
    validate_with_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    original_last_checked_date = YESTERDAY + timedelta(hours=1)

    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.last_checked_date = original_last_checked_date

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        validate_with_database=validate_with_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == YESTERDAY
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in capture_cli_logs.text
    )
    assert (
        f"Start date not provided. Using {YESTERDAY} as default download date for MAG_SCI_NORM, as this packet has been checked at least once."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is original_last_checked_date
    assert not mock_database.save.called


@pytest.mark.parametrize("validate_with_database", [True, False])
def test_get_start_end_dates_no_dates_defined_with_last_checked_date_older_than_yesterday(
    capture_cli_logs,
    mock_database,
    validate_with_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    older_than_yesterday = datetime(2025, 3, 21, 12, 45, 7)
    expected_start_date = datetime(2025, 3, 21, 12, 45, 7) - timedelta(hours=1)

    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.last_checked_date = older_than_yesterday

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        validate_with_database=validate_with_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == expected_start_date
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in capture_cli_logs.text
    )
    assert (
        f"Start date not provided. Using {expected_start_date} as default download date for MAG_SCI_NORM, as this packet has been checked at least once."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is older_than_yesterday
    assert not mock_database.save.called


def test_get_start_end_dates_not_up_to_date(
    capture_cli_logs,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = datetime(2025, 3, 21, 12, 45, 7)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=None,
        validate_with_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == original_start_date
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in capture_cli_logs.text
    )
    assert "Using provided start date" in capture_cli_logs.text
    assert (
        f"Packet MAG_SCI_NORM is not up to date. Downloading from {original_start_date}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


def test_get_start_end_dates_fully_up_to_date(
    capture_cli_logs,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.progress_timestamp = datetime(2025, 3, 21, 12, 45, 7)

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = download_progress.progress_timestamp - timedelta(days=2)
    original_end_date = download_progress.progress_timestamp - timedelta(days=1)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        validate_with_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is None

    assert "Using provided end date" in capture_cli_logs.text
    assert "Using provided start date" in capture_cli_logs.text
    assert (
        "Packet MAG_SCI_NORM is already up to date. Not downloading."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


def test_get_start_end_dates_partially_up_to_date(
    capture_cli_logs,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.progress_timestamp = datetime(2025, 3, 21, 12, 45, 7)

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = download_progress.progress_timestamp - timedelta(days=1)
    original_end_date = download_progress.progress_timestamp + timedelta(days=1)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        validate_with_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == download_progress.progress_timestamp
    assert end_date == original_end_date

    assert "Using provided end date" in capture_cli_logs.text
    assert "Using provided start date" in capture_cli_logs.text
    assert (
        f"Packet MAG_SCI_NORM is partially up to date. Downloading from {download_progress.progress_timestamp}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called
