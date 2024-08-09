"""Tests for `OutputManager` class."""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
from imap_mag.cli.fetchBinary import FetchBinary
from imap_mag.client.webPODA import IWebPODA
from imap_mag.outputManager import IOutputManager

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def create_file(file_path: Path, content: str | None) -> Path:
    """Create a file with the given content."""

    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        os.remove(file_path)

    with open(file_path, "w") as file:
        if content is not None:
            file.write(content)

    return file_path


@pytest.fixture
def mock_poda() -> mock.Mock:
    """Fixture for a mock IWebPODA instance."""
    return mock.create_autospec(IWebPODA, spec_set=True)


@pytest.fixture
def mock_output_manager() -> mock.Mock:
    """Fixture for a mock IOutputManager instance."""
    return mock.create_autospec(IOutputManager, spec_set=True)


def test_fetch_binary_empty_download_not_added_to_output(
    mock_poda: mock.Mock, mock_output_manager: mock.Mock
) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda, mock_output_manager)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_file(test_file, None)

    # Exercise.
    actual_downloaded: list[Path] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert not mock_output_manager.add_default_file.called
    assert actual_downloaded == []


def test_fetch_binary_with_same_start_end_date(
    mock_poda: mock.Mock, mock_output_manager: mock.Mock
) -> None:
    # Set up.
    fetchBinary = FetchBinary(mock_poda, mock_output_manager)

    test_file = Path(tempfile.gettempdir()) / "test_file"
    mock_poda.download.side_effect = lambda **_: create_file(test_file, "content")

    # Exercise.
    actual_downloaded: list[Path] = fetchBinary.download_binaries(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
    )

    # Verify.
    mock_poda.download.assert_called_once_with(
        packet="MAG_HSK_PW",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    mock_output_manager.add_default_file.assert_called_once_with(
        test_file,
        descriptor="hsk-pw",
        date=datetime(2025, 5, 2),
        extension="pkts",
    )

    assert actual_downloaded == [test_file]
