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
from imap_mag.download.FetchIALiRT import FetchIALiRT, process_ialirt_data
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler
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
    actual_downloaded: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
        )
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert actual_downloaded == dict()
    assert "No data downloaded from I-ALiRT Data Access." in capture_cli_logs.text


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
        {"met_in_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"met_in_utc": "2025-05-02T01:00:00", "data": [4, 5, 6]},
        {"met_in_utc": "2025-05-02T02:00:00", "data": [7, 8, 9]},
    ]

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
        )
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
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

        assert "met_in_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,1,2,3" in file_content
        assert "2025-05-02T01:00:00,4,5,6" in file_content
        assert "2025-05-02T02:00:00,7,8,9" in file_content

    assert "Downloaded 3 entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert "Creating new file for 2025-05-02." in capture_cli_logs.text
    assert "I-ALiRT data written to " in capture_cli_logs.text


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
        {"met_in_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"met_in_utc": "2025-05-03T01:00:00", "data": [4, 5, 6]},
        {"met_in_utc": "2025-05-04T02:00:00", "data": [7, 8, 9]},
    ]

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
        )
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 3

    for file_path, _ in actual_downloaded.items():
        assert file_path.exists()

        with open(file_path) as f:
            file_content = f.read()

            assert "met_in_utc,data_1,data_2,data_3" in file_content
            assert re.search(r"2025-05-0\dT0\d:00:00,\d,\d,\d", file_content)

    assert "Downloaded 3 entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert (
        "Downloaded I-ALiRT data for 3 days: 2025-05-02, 2025-05-03, 2025-05-04"
        in capture_cli_logs.text
    )
    assert "Creating new file for 2025-05-02." in capture_cli_logs.text
    assert "Creating new file for 2025-05-03." in capture_cli_logs.text
    assert "Creating new file for 2025-05-04." in capture_cli_logs.text
    assert "I-ALiRT data written to " in capture_cli_logs.text


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
        {"met_in_utc": "2025-05-02T02:00:00", "data": [1, 2, 3]},
        {"met_in_utc": "2025-05-02T03:00:00", "data": [4, 5, 6]},
        {"met_in_utc": "2025-05-02T04:00:00", "data": [7, 8, 9]},
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("met_in_utc,data_1,data_2,data_3\n")
        f.write("2025-05-02T00:00:00,10,11,12\n")
        f.write("2025-05-02T01:00:00,13,14,15\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
        )
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
        start_date=datetime(2025, 5, 2),
        end_date=datetime(2025, 5, 3),
    )

    assert len(actual_downloaded) == 1

    ((file_path, path_handler),) = actual_downloaded.items()

    assert file_path.exists()
    assert file_path.name == "imap_ialirt_20250502.csv"
    assert path_handler.content_date == datetime(2025, 5, 2, 4, 0, 0)

    with open(file_path) as f:
        file_content = f.read()

        assert "met_in_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,10,11,12" in file_content
        assert "2025-05-02T01:00:00,13,14,15" in file_content
        assert "2025-05-02T02:00:00,1,2,3" in file_content
        assert "2025-05-02T03:00:00,4,5,6" in file_content
        assert "2025-05-02T04:00:00,7,8,9" in file_content

    assert "Downloaded 3 entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT data appended to " in capture_cli_logs.text


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
        {"met_in_utc": "2025-05-02T00:00:00", "data": [1, 2, 3]},
        {"met_in_utc": "2025-05-02T01:00:00", "data": [4, 5, 6]},
        {"met_in_utc": "2025-05-02T02:00:00", "data": [7, 8, 9]},
    ]

    datastore_file = (
        temp_datastore / "ialirt" / "2025" / "05" / "imap_ialirt_20250502.csv"
    )
    datastore_file.parent.mkdir(parents=True, exist_ok=True)

    with open(datastore_file, "w") as f:
        f.write("met_in_utc,data_1,data_2,data_3\n")
        f.write("2025-05-02T03:00:00,10,11,12\n")
        f.write("2025-05-02T04:00:00,13,14,15\n")

    # Exercise.
    actual_downloaded: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=datetime(2025, 5, 2),
            end_date=datetime(2025, 5, 3),
        )
    )

    # Verify.
    mock_ialirt_data_access.get_all_by_dates.assert_called_once_with(
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

        assert "met_in_utc,data_1,data_2,data_3" in file_content
        assert "2025-05-02T00:00:00,1,2,3" in file_content
        assert "2025-05-02T01:00:00,4,5,6" in file_content
        assert "2025-05-02T02:00:00,7,8,9" in file_content
        assert "2025-05-02T03:00:00,10,11,12" in file_content
        assert "2025-05-02T04:00:00,13,14,15" in file_content

    assert "Downloaded 3 entries from I-ALiRT Data Access." in capture_cli_logs.text
    assert "Downloaded I-ALiRT data for 1 days: 2025-05-02" in capture_cli_logs.text
    assert (
        f"File for 2025-05-02 already exists: {datastore_file.as_posix()}. Appending new data."
        in capture_cli_logs.text
    )
    assert "I-ALiRT data written to " in capture_cli_logs.text


def test_split_gse_gsm_to_xyz_components() -> None:
    # Set up.
    raw_data = [
        {
            "met_in_utc": "2025-05-02T00:00:00",
            "data_gse": [1, 2, 3],
            "data_gsm": [4, 5, 6],
            "data_not_3elements_gse": 7,
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_data(
        df, IALIRT_PACKET_DEFINITION / "ialirt_4.05.yaml"
    )

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
            "met_in_utc": "2025-05-02T00:00:00",
            "data_rtn": [1, 2, 3],
            "data_not_3elements_rtn": 4,
        }
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_data(
        df, IALIRT_PACKET_DEFINITION / "ialirt_4.05.yaml"
    )

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
            "met_in_utc": "2025-05-02T00:00:00",
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
    processed_df = process_ialirt_data(
        df, IALIRT_PACKET_DEFINITION / "ialirt_4.05.yaml"
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


def test_process_mag_data_and_ignore_mixed_format_rows() -> None:
    # Set up.
    raw_data = [
        {
            "met_in_utc": "2025-05-02T00:00:00",
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
            "mag_B_GSM": [1, 2, 3],
            "mag_B_RTN": [4, 5, 6],
        },
        {
            "met": 498689725,
            "ttj2000ns": 814265793369384064,
            "apid": 478,
            "met_in_utc": "2025-10-20T20:55:24",
            "spice_kernels": {
                "planetary_constants": "pck00011.tpc",
                "science_frames": "imap_science_100.tf",
                "leapseconds": "naif0012.tls",
                "imap_frames": "imap_100.tf",
                "ephemeris_predicted": "imap_pred_od004_20251002_20251113_v01.bsp",
                "spacecraft_clock": "imap_sclk_0021.tsc",
                "planetary_ephemeris": "de440.bsp",
            },
            "last_modified": "2025-10-20T20:55:24.185384+00:00",
        },
    ]

    df = pd.DataFrame(raw_data)

    # Exercise.
    processed_df = process_ialirt_data(
        df, IALIRT_PACKET_DEFINITION / "ialirt_4.05.yaml"
    )

    # Verify.
    assert processed_df.at[0, "mag_hk_mode"] == "Normal"
    assert processed_df.at[0, "mag_B_GSM_x"] == 1
    assert processed_df.at[0, "mag_B_GSM_y"] == 2
    assert processed_df.at[0, "mag_B_GSM_z"] == 3
    assert processed_df.at[0, "mag_B_RTN_r"] == 4
    assert processed_df.at[0, "mag_B_RTN_t"] == 5
    assert processed_df.at[0, "mag_B_RTN_n"] == 6
