import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from imap_mag import appLogging
from imap_mag.util import DatetimeProvider

NOW = datetime.now()
TODAY = datetime.today()
TOMORROW = datetime.today() + timedelta(days=1)
YESTERDAY = datetime.today().replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=1)
BEGINNING_OF_IMAP = datetime(2025, 1, 1, 0, 0, 0)
END_OF_TODAY = datetime.today().replace(
    hour=23, minute=59, second=59, microsecond=999999
)


@pytest.fixture(autouse=True)
def enableLogging():
    appLogging.set_up_logging(
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
