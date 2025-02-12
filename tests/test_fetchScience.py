"""Tests for `FetchScience` class."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from imap_mag.cli.fetchScience import FetchScience, MAGMode, MAGSensor
from imap_mag.client.sdcDataAccess import ISDCDataAccess
from imap_mag.outputManager import StandardSPDFMetadataProvider

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


@pytest.fixture
def mock_soc() -> mock.Mock:
    """Fixture for a mock ISDCDataAccess instance."""
    return mock.create_autospec(ISDCDataAccess, spec_set=True)


def test_fetch_science_no_matching_files(mock_soc: mock.Mock) -> None:
    # Set up.
    fetchScience = FetchScience(
        mock_soc, modes=[MAGMode.Normal], sensors=[MAGSensor.OBS]
    )

    mock_soc.get_filename.side_effect = lambda **_: {}  # return empty dictionary

    # Exercise.
    actual_downloaded: dict[Path, StandardSPDFMetadataProvider] = (
        fetchScience.download_latest_science(
            level="l1b",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 2),
        )
    )

    # Verify.
    mock_soc.get_filename.assert_called_once_with(
        level="l1b",
        descriptor="norm-mago",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
        version="latest",
        extension="cdf",
    )

    mock_soc.download.assert_not_called()

    assert actual_downloaded == dict()


def test_fetch_science_with_same_start_end_date(mock_soc: mock.Mock) -> None:
    # Set up.
    fetchScience = FetchScience(
        mock_soc, modes=[MAGMode.Normal], sensors=[MAGSensor.OBS]
    )

    test_file = Path(tempfile.gettempdir()) / "test_file"

    mock_soc.get_filename.side_effect = lambda **_: [
        {
            "file_path": test_file.absolute(),
            "descriptor": "norm-mago",
        }
    ]
    mock_soc.download.side_effect = lambda file_path: file_path

    # Exercise.
    actual_downloaded: dict[Path, StandardSPDFMetadataProvider] = (
        fetchScience.download_latest_science(
            level="l1b",
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 2),
        )
    )

    # Verify.
    mock_soc.get_filename.assert_called_once_with(
        level="l1b",
        descriptor="norm-mago",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 2),
        version="latest",
        extension="cdf",
    )
    mock_soc.download.assert_called_once_with(
        test_file.absolute(),
    )

    assert len(actual_downloaded) == 1

    assert test_file in actual_downloaded.keys()
    assert (
        StandardSPDFMetadataProvider(
            level="l1b",
            descriptor="norm-mago",
            date=datetime(2025, 5, 2),
            extension="cdf",
        )
        in actual_downloaded.values()
    )
