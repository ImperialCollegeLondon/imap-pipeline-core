"""Tests for app utilities."""

import datetime as dt
import logging
from datetime import datetime, timedelta
from unittest import mock

import pytest

from imap_db.model import DownloadProgress
from imap_mag.DB import IDatabase
from prefect_server.prefectUtils import get_start_and_end_dates_for_download

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401

LOGGER = logging.getLogger(__name__)
YESTERDAY = datetime.today().replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=1)
END_OF_TODAY = datetime.today().replace(
    hour=23, minute=59, second=59, microsecond=999999
)
NOW = datetime.now()


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock IDatabase instance."""
    return mock.create_autospec(IDatabase, spec_set=True)


@pytest.fixture(autouse=True)
def mock_datetime_now(monkeypatch):
    """Mock datetime to specific time."""

    datetime_mock = mock.MagicMock(wrap=datetime)
    datetime_mock.now.return_value = NOW

    monkeypatch.setattr(dt, "datetime", datetime_mock)


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_and_end_dates_no_dates_defined(
    caplog,
    mock_database,
    check_and_update_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_start_and_end_dates_for_download(
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
        "Start date not provided. Using yesterday as default download date for MAG_SCI_NORM."
        in caplog.text
    )


@pytest.mark.parametrize("check_and_update_database", [True, False])
def test_get_start_and_end_dates_end_date_defined(
    caplog,
    mock_database,
    check_and_update_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_start_and_end_dates_for_download(
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

    assert start_date == YESTERDAY
    assert end_date == original_end_date

    assert "Using provided end date" in caplog.text
    assert (
        "Start date not provided. Using yesterday as default download date for MAG_SCI_NORM."
        in caplog.text
    )


def test_get_start_and_end_dates_start_and_end_date_defined_no_database_update(
    caplog,
    mock_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    mock_database.get_download_progress.return_value = download_progress

    original_start_date = datetime(2025, 2, 12, 0, 0, 0)
    original_end_date = datetime(2025, 2, 13, 12, 34, 0)

    caplog.set_level(logging.INFO)

    # Exercise
    result = get_start_and_end_dates_for_download(
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
