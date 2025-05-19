import logging
from datetime import datetime, timedelta
from unittest import mock

import pytest

from imap_db.model import DownloadProgress
from imap_mag.db import IDatabase
from imap_mag.util import get_dates_for_download
from tests.util.miscellaneous import (  # noqa: F401  # noqa: F401
    BEGINNING_OF_IMAP,
    END_OF_TODAY,
    NOW,
    YESTERDAY,
    enableLogging,
    mock_datetime_provider,
    tidyDataFolders,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock IDatabase instance."""
    return mock.create_autospec(IDatabase, spec_set=True)


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_end_dates_no_dates_defined_empty_database(
    caplog,
    mock_database,
    check_and_update_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        check_and_update_database=check_and_update_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == BEGINNING_OF_IMAP
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in caplog.text
    )
    assert (
        f"Start date not provided. Using {BEGINNING_OF_IMAP} as default download date for MAG_SCI_NORM, as this is the first time it is downloaded"
        in caplog.text
    )

    if check_and_update_database:
        assert download_progress.last_checked_date == NOW
        assert mock_database.save.called
    else:
        assert download_progress.last_checked_date is None
        assert not mock_database.save.called


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_end_dates_end_date_defined_empty_database(
    caplog,
    mock_database,
    check_and_update_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=original_end_date,
        check_and_update_database=check_and_update_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == BEGINNING_OF_IMAP
    assert end_date == original_end_date

    assert "Using provided end date" in caplog.text
    assert (
        f"Start date not provided. Using {BEGINNING_OF_IMAP} as default download date for MAG_SCI_NORM, as this is the first time it is downloaded"
        in caplog.text
    )

    if check_and_update_database:
        assert download_progress.last_checked_date == NOW
        assert mock_database.save.called
    else:
        assert download_progress.last_checked_date is None
        assert not mock_database.save.called


def test_get_start_end_dates_both_dates_defined_empty_database(
    caplog,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = datetime(2025, 2, 12, 0, 0, 0)
    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        check_and_update_database=False,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == original_start_date
    assert end_date == original_end_date

    assert "Using provided end date" in caplog.text
    assert "Using provided start date" in caplog.text
    assert (
        f"Not checking database and forcing download from {original_start_date} to {original_end_date}."
        in caplog.text
    )

    assert download_progress.last_checked_date is None
    assert not mock_database.save.called


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_end_dates_no_dates_defined_with_progress_timestamp(
    caplog,
    mock_database,
    check_and_update_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.progress_timestamp = datetime(2025, 3, 21, 12, 45, 7)

    mock_database.get_download_progress.return_value = download_progress

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        check_and_update_database=check_and_update_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == download_progress.progress_timestamp
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in caplog.text
    )
    assert (
        f"Start date not provided. Using last updated date {download_progress.progress_timestamp} for MAG_SCI_NORM from database."
        in caplog.text
    )

    if check_and_update_database:
        assert download_progress.last_checked_date == NOW
        assert mock_database.save.called
    else:
        assert download_progress.last_checked_date is None
        assert not mock_database.save.called


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_end_dates_no_dates_defined_with_last_checked_date(
    caplog,
    mock_database,
    check_and_update_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"
    download_progress.last_checked_date = datetime(2025, 3, 21, 12, 45, 7)

    mock_database.get_download_progress.return_value = download_progress

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=None,
        original_end_date=None,
        check_and_update_database=check_and_update_database,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == YESTERDAY
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in caplog.text
    )
    assert (
        f"Start date not provided. Using {YESTERDAY} as default download date for MAG_SCI_NORM, as this packet has been checked at least once."
        in caplog.text
    )

    if check_and_update_database:
        assert download_progress.last_checked_date == NOW
        assert mock_database.save.called
    else:
        assert (
            download_progress.last_checked_date is download_progress.last_checked_date
        )
        assert not mock_database.save.called


def test_get_start_end_dates_not_up_to_date(
    caplog,
    mock_database,
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = datetime(2025, 3, 21, 12, 45, 7)

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=None,
        check_and_update_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == original_start_date
    assert end_date == END_OF_TODAY

    assert (
        "End date not provided. Using end of today as default download date for MAG_SCI_NORM."
        in caplog.text
    )
    assert "Using provided start date" in caplog.text
    assert (
        f"Packet MAG_SCI_NORM is not up to date. Downloading from {original_start_date}."
        in caplog.text
    )

    assert download_progress.last_checked_date == NOW
    assert mock_database.save.called


def test_get_start_end_dates_fully_up_to_date(
    caplog,
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

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        check_and_update_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is None

    assert "Using provided end date" in caplog.text
    assert "Using provided start date" in caplog.text
    assert "Packet MAG_SCI_NORM is already up to date. Not downloading." in caplog.text

    assert download_progress.last_checked_date == NOW
    assert mock_database.save.called


def test_get_start_end_dates_partially_up_to_date(
    caplog,
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

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_dates_for_download(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        original_start_date=original_start_date,
        original_end_date=original_end_date,
        check_and_update_database=True,
        logger=LOGGER,
    )

    # Verify
    assert result is not None

    start_date, end_date = result

    assert start_date == download_progress.progress_timestamp
    assert end_date == original_end_date

    assert "Using provided end date" in caplog.text
    assert "Using provided start date" in caplog.text
    assert (
        f"Packet MAG_SCI_NORM is partially up to date. Downloading from {download_progress.progress_timestamp}."
        in caplog.text
    )

    assert download_progress.last_checked_date == NOW
    assert mock_database.save.called
