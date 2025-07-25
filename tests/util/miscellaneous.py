import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from imap_mag.appLogging import AppLogging
from imap_mag.util import DatetimeProvider

NOW = datetime(2025, 6, 2, 12, 37, 9)
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
TOMORROW = TODAY + timedelta(days=1)
YESTERDAY = TODAY - timedelta(days=1)
BEGINNING_OF_IMAP = YESTERDAY - timedelta(days=1)
END_OF_TODAY = TODAY.replace(hour=23, minute=59, second=59, microsecond=999999)


DATASTORE = Path("tests/datastore")


@pytest.fixture(autouse=False)
def enableLogging():
    AppLogging.set_up_logging(
        console_log_output="stdout",
        console_log_level="debug",
        console_log_color=True,
        logfile_file="debug",
        logfile_log_level="debug",
        logfile_log_color=False,
        log_line_template="%(color_on)s[%(asctime)s] [%(levelname)-8s] %(message)s%(color_off)s",
        console_log_line_template="%(color_on)s%(message)s%(color_off)s",
    )
    yield
    AppLogging.reset_setup_flag()  # Reset logging setup after test


@pytest.fixture(autouse=True)
def tidyDataFolders():
    os.system("rm -rf .work")
    os.system("rm -rf output/*")
    yield


@pytest.fixture(autouse=False)
def mock_datetime_provider(monkeypatch):
    """Mock DatetimeProvider to specific time."""

    monkeypatch.setattr(DatetimeProvider, "now", lambda: NOW)
    monkeypatch.setattr(DatetimeProvider, "today", lambda: TODAY)
    monkeypatch.setattr(DatetimeProvider, "tomorrow", lambda: TOMORROW)
    monkeypatch.setattr(DatetimeProvider, "yesterday", lambda: YESTERDAY)
    monkeypatch.setattr(DatetimeProvider, "end_of_today", lambda: END_OF_TODAY)
    monkeypatch.setattr(
        DatetimeProvider, "beginning_of_imap", lambda: BEGINNING_OF_IMAP
    )


def create_test_file(file_path: Path, content: str | None = None) -> Path:
    """Create a file with the given content."""

    file_path.unlink(missing_ok=True)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    file_path.touch()

    if content is not None:
        file_path.write_text(content)

    return file_path
