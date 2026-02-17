"""Tests for `FetchIALiRT` class."""

import math
import re
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.download.FetchIALiRT import (
    FetchIALiRT,
    process_ialirt_hk_data,
    process_ialirt_mag_data,
)
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler
from imap_mag.util.constants import CONSTANTS
from tests.util.miscellaneous import temp_datastore  # noqa: F401

IALIRT_PACKET_DEFINITION = (
    Path(__file__).parent.parent / "src" / "imap_mag" / "packet_def"
)


@pytest.fixture
def mock_ialirt_data_access() -> mock.Mock:
    """Fixture for a mock IALiRTDataAccess instance."""
    return mock.create_autospec(IALiRTApiClient, spec_set=True)


def test_fetch_ialirt_no_data(
    mock_ialirt_data_access: mock.Mock,
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(Path(tempfile.mkdtemp())),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = (
        lambda **_: []
    )  # return empty list

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert actual_downloaded == dict()
    assert "No mag data downloaded from I-ALiRT Data Access." in capture_cli_logs.text


def test_fetch_ialirt_single_day_no_existing_data(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"time_utc": "2025-05-02T01:00:00", "data": [4, 5, 6]},
        {"time_utc": "2025-05-02T02:00:00", "data": [7, 8, 9]},
    ]

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path.name == "imap_ialirt_20250502.csv"
    assert path_handler.content_date == datetime(2025, 5, 2, 2, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "time_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,1,2,3" in file_content
        assert "2025-05-02T01:00:00,4,5,6" in file_content
        assert "2025-05-02T02:00:00,7,8,9" in file_content

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert "Creating new file for 2025-05-02." in capture_cli_logs.text
    assert "I-ALiRT mag data written to " in capture_cli_logs.text


def test_fetch_ialirt_multiple_days_no_existing_data(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"time_utc": "2025-05-03T01:00:00", "data": [4, 5, 6]},
        {"time_utc": "2025-05-04T02:00:00", "data": [7, 8, 9]},
    ]

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 3

    for file_path, _ in actual_downloaded.items():
        assert file_path.exists()

        with open(file_path) as f:
            file_content = f.read()

            assert "time_utc,data_1,data_2,data_3" in file_content
            assert re.search(r"2025-05-0\dT0\d:00:00,\d,\d,\d", file_content)

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert (
        "Downloaded I-ALiRT mag data for 3 days: 2025-05-02, 2025-05-03, 2025-05-04"
        in capture_cli_logs.text
    )
    assert "Creating new file for 2025-05-02." in capture_cli_logs.text
    assert "Creating new file for 2025-05-03." in capture_cli_logs.text
    assert "Creating new file for 2025-05-04." in capture_cli_logs.text
    assert "I-ALiRT mag data written to " in capture_cli_logs.text


def test_fetch_ialirt_single_day_existing_older_data_in_datastore(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T02:00:00", "data": [1, 2, 3]},
        {"time_utc": "2025-05-02T03:00:00", "data": [4, 5, 6]},
        {"time_utc": "2025-05-02T04:00:00", "data": [7, 8, 9]},
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("time_utc,data_1,data_2,data_3\n")
        f.write("2025-05-02T00:00:00,10,11,12\n")
        f.write("2025-05-02T01:00:00,13,14,15\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path == datastore_file
    assert path_handler.content_date == datetime(2025, 5, 2, 4, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "time_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,10,11,12" in file_content
        assert "2025-05-02T01:00:00,13,14,15" in file_content
        assert "2025-05-02T02:00:00,1,2,3" in file_content
        assert "2025-05-02T03:00:00,4,5,6" in file_content
        assert "2025-05-02T04:00:00,7,8,9" in file_content

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT mag data appended to " in capture_cli_logs.text


def test_fetch_ialirt_single_day_existing_older_data_in_datastore_with_more_columns(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T02:00:00", "a": 1, "c": 2, "d": 3},
        {"time_utc": "2025-05-02T03:00:00", "a": 4, "c": 5, "d": 6},
        {"time_utc": "2025-05-02T04:00:00", "a": 7, "c": 8, "d": 9},
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("time_utc,a,b,c,d,e\n")
        f.write("2025-05-02T00:00:00,10,11,12,13,14\n")
        f.write("2025-05-02T01:00:00,15,16,17,18,19\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path == datastore_file
    assert path_handler.content_date == datetime(2025, 5, 2, 4, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "time_utc,a,b,c,d,e" in file_content
        assert "2025-05-02T00:00:00,10,11,12,13,14" in file_content
        assert "2025-05-02T01:00:00,15,16,17,18,19" in file_content
        assert "2025-05-02T02:00:00,1,,2,3," in file_content
        assert "2025-05-02T03:00:00,4,,5,6," in file_content
        assert "2025-05-02T04:00:00,7,,8,9," in file_content

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT mag data appended to " in capture_cli_logs.text


def test_fetch_ialirt_single_day_existing_older_data_in_datastore_with_fewer_columns(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T02:00:00", "a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
        {"time_utc": "2025-05-02T03:00:00", "a": 6, "b": 7, "c": 8, "d": 9, "e": 10},
        {
            "time_utc": "2025-05-02T04:00:00",
            "a": 11,
            "b": 12,
            "c": 13,
            "d": 14,
            "e": 15,
        },
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("time_utc,a,c,d\n")
        f.write("2025-05-02T00:00:00,16,17,18\n")
        f.write("2025-05-02T01:00:00,19,20,21\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path == datastore_file
    assert path_handler.content_date == datetime(2025, 5, 2, 4, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "time_utc,a,b,c,d,e" in file_content
        assert "2025-05-02T00:00:00,16,,17,18," in file_content
        assert "2025-05-02T01:00:00,19,,20,21," in file_content
        assert "2025-05-02T02:00:00,1,2.0,3,4,5.0" in file_content
        assert "2025-05-02T03:00:00,6,7.0,8,9,10.0" in file_content
        assert "2025-05-02T04:00:00,11,12.0,13,14,15.0" in file_content

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT mag data written to " in capture_cli_logs.text


def test_fetch_ialirt_single_day_existing_newer_data_in_datastore(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"time_utc": "2025-05-02T01:00:00", "data": [4, 5, 6]},
        {"time_utc": "2025-05-02T02:00:00", "data": [7, 8, 9]},
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("time_utc,data_1,data_2,data_3\n")
        f.write("2025-05-02T03:00:00,10,11,12\n")
        f.write("2025-05-02T04:00:00,13,14,15\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path == datastore_file
    assert path_handler.content_date == datetime(2025, 5, 2, 2, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "time_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,1,2,3" in file_content
        assert "2025-05-02T01:00:00,4,5,6" in file_content
        assert "2025-05-02T02:00:00,7,8,9" in file_content
        assert "2025-05-02T03:00:00,10,11,12" in file_content
        assert "2025-05-02T04:00:00,13,14,15" in file_content

    assert "Downloaded 3 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT mag data written to " in capture_cli_logs.text


def test_fetch_ialirt_duplicate_timestamps_different_instruments(
    mock_ialirt_data_access: mock.Mock,
    temp_datastore,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    fetch_ialirt = FetchIALiRT(
        mock_ialirt_data_access,
        Path(tempfile.mkdtemp()),
        DatastoreFileFinder(temp_datastore),
        IALIRT_PACKET_DEFINITION,
    )

    mock_ialirt_data_access.get_all_by_dates.side_effect = lambda **_: [
        {
            "time_utc": "2025-05-02T00:00:00",
            "instrument": "mag",
            "mag_data": [1, 2, 3],
        },
        {
            "time_utc": "2025-05-02T00:00:00",
            "instrument": "swe",
            "swe_data": [1.5, 2.5, 3.5],
        },
        {
            "time_utc": "2025-05-02T01:00:00",
            "instrument": "mag",
            "mag_data": [4, 5, 6],
        },
        {
            "time_utc": "2025-05-02T01:00:00",
            "instrument": "swe",
            "swe_data": [4.5, 5.5, 6.5],
        },
        {
            "time_utc": "2025-05-02T02:00:00",
            "instrument": "mag",
            "mag_data": [7, 8, 9],
        },
        {
            "time_utc": "2025-05-02T02:00:00",
            "instrument": "swe",
            "swe_data": [7.5, 8.5, 9.5],
        },
    ]

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = fetch_ialirt.download_mag_to_csv(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        instrument="mag",
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert path_handler.content_date == datetime(2025, 5, 2, 2, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert (
            "time_utc,instrument,mag_data_1,mag_data_2,mag_data_3,swe_data_1,swe_data_2,swe_data_3"
            in file_content
        )
        assert '2025-05-02T00:00:00,"mag,swe",1.0,2.0,3.0,1.5,2.5,3.5' in file_content
        assert '2025-05-02T01:00:00,"mag,swe",4.0,5.0,6.0,4.5,5.5,6.5' in file_content
        assert '2025-05-02T02:00:00,"mag,swe",7.0,8.0,9.0,7.5,8.5,9.5' in file_content

    assert "Downloaded 6 mag entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT mag data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert "Creating new file for 2025-05-02." in capture_cli_logs.text
    assert "I-ALiRT mag data written to " in capture_cli_logs.text


def test_split_gse_gsm_to_xyz_components() -> None:
    # Set up.
    raw_data = [
        {
            "time_utc": "2025-05-02T00:00:00",
            "data_gse": [1, 2, 3],
            "data_gsm": [4, 5, 6],
            "data_not_3elements_gse": 7,
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_mag_data(df)

    # Verify.
    assert "data_gse_x" in processed_df.columns
    assert "data_gse_y" in processed_df.columns
    assert "data_gse_z" in processed_df.columns
    assert "data_gsm_x" in processed_df.columns
    assert "data_gsm_y" in processed_df.columns
    assert "data_gsm_z" in processed_df.columns
    assert "data_not_3components_gse_x" not in processed_df.columns
    assert "data_not_3components_gse_y" not in processed_df.columns
    assert "data_not_3components_gse_z" not in processed_df.columns

    assert processed_df.at[0, "data_gse_x"] == 1
    assert processed_df.at[0, "data_gse_y"] == 2
    assert processed_df.at[0, "data_gse_z"] == 3
    assert processed_df.at[0, "data_gsm_x"] == 4
    assert processed_df.at[0, "data_gsm_y"] == 5
    assert processed_df.at[0, "data_gsm_z"] == 6
    assert processed_df.at[0, "data_not_3elements_gse"] == 7


def test_split_rtn_to_rtn_components() -> None:
    # Set up.
    raw_data = [
        {
            "time_utc": "2025-05-02T00:00:00",
            "data_rtn": [1, 2, 3],
            "data_not_3elements_rtn": 4,
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_mag_data(df)

    # Verify.
    assert "data_rtn_r" in processed_df.columns
    assert "data_rtn_t" in processed_df.columns
    assert "data_rtn_n" in processed_df.columns
    assert "data_not_3elements_rtn_r" not in processed_df.columns
    assert "data_not_3elements_rtn_t" not in processed_df.columns
    assert "data_not_3elements_rtn_n" not in processed_df.columns

    assert processed_df.at[0, "data_rtn_r"] == 1
    assert processed_df.at[0, "data_rtn_t"] == 2
    assert processed_df.at[0, "data_rtn_n"] == 3
    assert processed_df.at[0, "data_not_3elements_rtn"] == 4


def test_process_mag_hk() -> None:
    # Set up.
    raw_data = [
        {
            "time_utc": "2025-05-02T00:00:00",
            "mag_hk_icu_temp": 3000,
            "mag_hk_fib_temp": 3000,
            "mag_hk_fob_temp": 3000,
            "mag_hk_hk3v3": 3000,
            "mag_hk_hk3v3_current": 3000,
            "mag_hk_hkn8v5": 3000,
            "mag_hk_hkn8v5_current": 3000,
            "mag_hk_mode": 5,
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_hk_data(
        df, IALIRT_PACKET_DEFINITION / CONSTANTS.IALIRT_PACKET_DEFINITION_FILE
    )

    # Verify.
    assert math.isclose(processed_df.at[0, "mag_hk_icu_temp"], 97.5681, rel_tol=1e-5)  # type: ignore
    assert math.isclose(processed_df.at[0, "mag_hk_fib_temp"], 62.642015, rel_tol=1e-5)  # type: ignore
    assert math.isclose(processed_df.at[0, "mag_hk_fob_temp"], 57.237986, rel_tol=1e-5)  # type: ignore
    assert math.isclose(processed_df.at[0, "mag_hk_hk3v3"], 3.492084, rel_tol=1e-5)  # type: ignore
    assert math.isclose(
        processed_df.at[0, "mag_hk_hk3v3_current"],  # type: ignore
        225.28006,
        rel_tol=1e-5,
    )
    assert math.isclose(processed_df.at[0, "mag_hk_hkn8v5"], -7.773122, rel_tol=1e-5)  # type: ignore
    assert math.isclose(
        processed_df.at[0, "mag_hk_hkn8v5_current"],  # type: ignore
        345.0094,
        rel_tol=1e-5,
    )
    assert processed_df.at[0, "mag_hk_mode"] == "Normal"


def test_process_mag_hk_flattens_nested_status() -> None:
    # Set up - data shaped like the real API response.
    raw_data = [
        {
            "time_utc": "2025-05-02T00:00:00",
            "instrument": "mag_hk",
            "mag_hk_status": {
                "icu_temp": 3000,
                "fib_temp": 3000,
                "fob_temp": 3000,
                "hk3v3": 3000,
                "hk3v3_current": 3000,
                "hkn8v5": 3000,
                "hkn8v5_current": 3000,
                "mode": 5,
            },
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_hk_data(
        df, IALIRT_PACKET_DEFINITION / CONSTANTS.IALIRT_PACKET_DEFINITION_FILE
    )

    # Verify - nested dict should be flattened with mag_hk_ prefix.
    assert "mag_hk_status" not in processed_df.columns
    assert math.isclose(processed_df.at[0, "mag_hk_icu_temp"], 97.5681, rel_tol=1e-5)  # type: ignore
    assert math.isclose(processed_df.at[0, "mag_hk_fib_temp"], 62.642015, rel_tol=1e-5)  # type: ignore
    assert processed_df.at[0, "mag_hk_mode"] == "Normal"


def test_process_mag_data_with_vectors() -> None:
    # Set up.
    raw_data = [
        {
            "time_utc": "2025-05-02T00:00:00",
            "mag_B_GSM": [1, 2, 3],
            "mag_B_RTN": [4, 5, 6],
        },
        {
            "time_utc": "2025-05-02T02:00:00",
            "mag_B_GSM": [7, 8, 9],
            "mag_B_RTN": [10, 11, 12],
        },
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_mag_data(df)

    # Verify.
    assert processed_df.at[0, "mag_B_GSM_x"] == 1
    assert processed_df.at[0, "mag_B_GSM_y"] == 2
    assert processed_df.at[0, "mag_B_GSM_z"] == 3
    assert processed_df.at[0, "mag_B_RTN_r"] == 4
    assert processed_df.at[0, "mag_B_RTN_t"] == 5
    assert processed_df.at[0, "mag_B_RTN_n"] == 6

    assert processed_df.at[1, "mag_B_GSM_x"] == 7
    assert processed_df.at[1, "mag_B_GSM_y"] == 8
    assert processed_df.at[1, "mag_B_GSM_z"] == 9
    assert processed_df.at[1, "mag_B_RTN_r"] == 10
    assert processed_df.at[1, "mag_B_RTN_t"] == 11
    assert processed_df.at[1, "mag_B_RTN_n"] == 12
