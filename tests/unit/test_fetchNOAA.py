"""Tests for NOAA data processing functions."""

from datetime import datetime
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from imap_mag.download.FetchNOAA import (
    FetchNOAA,
    _process_noaa_mag,
    _process_noaa_plasma,
)
from imap_mag.io import FileFinder
from imap_mag.io.file import NOAAPathHandler

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fetch_noaa(tmp_path: Path) -> FetchNOAA:
    """Minimal FetchNOAA instance with mocked external dependencies."""
    return FetchNOAA(
        data_access=mock.create_autospec(NOAARTSWApiClient, spec_set=True),
        work_folder=tmp_path,
        datastore_finder=mock.create_autospec(FileFinder, spec_set=True),
    )


# ---------------------------------------------------------------------------
# _process_noaa_mag
# ---------------------------------------------------------------------------


def test_process_noaa_mag_all_columns_present() -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00"],
            "bx_gsm": [1.1],
            "by_gsm": [2.2],
            "bz_gsm": [3.3],
            "theta_gsm": [10.0],
            "phi_gsm": [20.0],
            "extra_col": [99.9],  # should be dropped
        }
    )

    # Exercise.
    result = _process_noaa_mag(data)

    # Verify.
    assert list(result.columns) == [
        "time_tag",
        "bx_gsm",
        "by_gsm",
        "bz_gsm",
        "theta_gsm",
        "phi_gsm",
    ]
    assert "extra_col" not in result.columns
    assert len(result) == 1


def test_process_noaa_mag_missing_column_raises() -> None:
    # Set up - omit 'bz_gsm'.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00"],
            "bx_gsm": [1.1],
            "by_gsm": [2.2],
            # bz_gsm intentionally absent
            "theta_gsm": [10.0],
            "phi_gsm": [20.0],
        }
    )

    # Exercise & verify.
    with pytest.raises(KeyError):
        _process_noaa_mag(data)


def test_process_noaa_mag_preserves_values() -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00", "2024-01-01 00:01:00"],
            "bx_gsm": [1.0, -1.0],
            "by_gsm": [2.0, -2.0],
            "bz_gsm": [3.0, -3.0],
            "theta_gsm": [45.0, 90.0],
            "phi_gsm": [180.0, 270.0],
        }
    )

    # Exercise.
    result = _process_noaa_mag(data)

    # Verify.
    assert all(result["bx_gsm"].tolist() == data["bx_gsm"])
    assert all(result["bz_gsm"].tolist() == data["bz_gsm"])
    assert all(result["theta_gsm"].tolist() == data["theta_gsm"])


# ---------------------------------------------------------------------------
# _process_noaa_plasma
# ---------------------------------------------------------------------------


def test_process_noaa_plasma_all_columns_present_and_renamed() -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00"],
            "proton_speed": [400.0],
            "proton_temperature": [1e5],
            "proton_density": [5.0],
            "extra_col": [99.9],  # should be dropped
        }
    )

    # Exercise.
    result = _process_noaa_plasma(data)

    # Verify - proton_ prefix removed, extra column dropped.
    assert list(result.columns) == ["time_tag", "speed", "temperature", "density"]
    assert "extra_col" not in result.columns
    assert "proton_speed" not in result.columns
    assert len(result) == 1


def test_process_noaa_plasma_missing_column_raises() -> None:
    # Set up - omit 'proton_density'.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00"],
            "proton_speed": [400.0],
            "proton_temperature": [1e5],
            # proton_density intentionally absent
        }
    )

    # Exercise & verify.
    with pytest.raises(KeyError):
        _process_noaa_plasma(data)


def test_process_noaa_plasma_preserves_values() -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": ["2024-01-01 00:00:00", "2024-01-01 00:01:00"],
            "proton_speed": [400.0, 450.0],
            "proton_temperature": [1e5, 2e5],
            "proton_density": [5.0, 6.0],
        }
    )

    # Exercise.
    result = _process_noaa_plasma(data)

    # Verify renamed columns carry the original values.
    assert all(result["speed"].tolist() == data["proton_speed"])
    assert all(result["temperature"].tolist() == data["proton_temperature"])
    assert all(result["density"].tolist() == data["proton_density"])
    assert all(result["time_tag"].tolist() == data["time_tag"])


# ---------------------------------------------------------------------------
# FetchNOAA._get_index_as_datetime
# ---------------------------------------------------------------------------


def test_get_index_as_datetime_converts_multiple_timestamps(
    fetch_noaa: FetchNOAA,
) -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": [
                "2026-07-21T08:00:00",
                "2026-07-21T09:00:00",
                "2026-07-21T10:00:00",
            ]
        }
    )

    # Exercise.
    result = fetch_noaa._get_index_as_datetime(data)

    # Verify - all entries are converted to plain Python datetimes in order.
    assert list(result) == [
        datetime(2026, 7, 21, 8, 0, 0),
        datetime(2026, 7, 21, 9, 0, 0),
        datetime(2026, 7, 21, 10, 0, 0),
    ]


def test_get_index_as_datetime_unparseable_string_raises(fetch_noaa: FetchNOAA) -> None:
    # Set up.
    data = pd.DataFrame(
        {"time_tag": ["2026-07-21T08:00:00", "not-a-date", "2026-07-21T10:00:00"]}
    )

    # Exercise & verify.
    with pytest.raises((ValueError, Exception)):
        fetch_noaa._get_index_as_datetime(data)


# ---------------------------------------------------------------------------
# Helpers for _add_to_files tests
# ---------------------------------------------------------------------------


def _write_csv(path: Path, data: pd.DataFrame) -> None:
    """Write a CSV in the same format FetchNOAA._add_to_files produces."""
    df = data.copy()
    df.drop_duplicates(subset="time_tag", keep="last", inplace=True)
    df.sort_values(by="time_tag", inplace=True)
    df.set_index("time_tag", inplace=True, drop=True)
    df = df.reindex(sorted(df.columns), axis="columns")
    df.to_csv(path, mode="w", header=True, index=True)


# ---------------------------------------------------------------------------
# FetchNOAA._add_to_files
# ---------------------------------------------------------------------------


def test_add_to_files_no_existing_file_creates_new_file_in_work_folder(
    fetch_noaa: FetchNOAA, tmp_path: Path
) -> None:
    # Set up.
    data = pd.DataFrame(
        {
            "time_tag": ["2026-07-21T08:00:00", "2026-07-21T09:00:00"],
            "bx_gsm": [1.0, 2.0],
        }
    )
    fetch_noaa._datastore_finder.find_by_handler.return_value = None  # type: ignore

    # Exercise.
    result = fetch_noaa._add_to_files("SOLAR1", "mag", data)

    # Verify - new file created in work folder with header and all rows.
    expected_path = tmp_path / "SOLAR1_mag_noaa_20260721.csv"
    assert list(result.keys()) == [expected_path]
    handler = result[expected_path]
    assert isinstance(handler, NOAAPathHandler)
    assert handler.mission == "SOLAR1"
    assert handler.instrument == "mag"
    assert handler.content_date == datetime(
        2026, 7, 21, 9, 0, 0
    )  # max of input timestamps
    written = pd.read_csv(expected_path)
    assert list(written["time_tag"]) == ["2026-07-21T08:00:00", "2026-07-21T09:00:00"]
    assert list(written["bx_gsm"]) == [1.0, 2.0]


def test_add_to_files_appends_when_new_timestamps_are_strictly_newer(
    fetch_noaa: FetchNOAA, tmp_path: Path
) -> None:
    # Set up - existing file has T1; new data has T2 > T1, same columns.
    existing_file = tmp_path / "existing.csv"
    _write_csv(
        existing_file,
        pd.DataFrame({"time_tag": ["2026-07-21T08:00:00"], "bx_gsm": [1.0]}),
    )
    fetch_noaa._datastore_finder.find_by_handler.return_value = existing_file  # type: ignore

    new_data = pd.DataFrame({"time_tag": ["2026-07-21T09:00:00"], "bx_gsm": [2.0]})

    # Exercise.
    result = fetch_noaa._add_to_files("SOLAR1", "mag", new_data)

    # Verify - existing file is updated in-place; both rows are present.
    assert existing_file in result
    written = pd.read_csv(existing_file)
    assert list(written["time_tag"]) == ["2026-07-21T08:00:00", "2026-07-21T09:00:00"]
    assert list(written["bx_gsm"]) == [1.0, 2.0]


def test_add_to_files_rewrites_file_and_deduplicates_when_timestamps_overlap(
    fetch_noaa: FetchNOAA, tmp_path: Path
) -> None:
    # Set up - existing file has T1, T2; new data has T2 (duplicate) and T3.
    existing_file = tmp_path / "existing.csv"
    _write_csv(
        existing_file,
        pd.DataFrame(
            {
                "time_tag": ["2026-07-21T08:00:00", "2026-07-21T09:00:00"],
                "bx_gsm": [1.0, 2.0],
            }
        ),
    )
    fetch_noaa._datastore_finder.find_by_handler.return_value = existing_file  # type: ignore

    new_data = pd.DataFrame(
        {
            "time_tag": ["2026-07-21T09:00:00", "2026-07-21T10:00:00"],
            "bx_gsm": [99.0, 3.0],  # 99.0 replaces the old T2 value
        }
    )

    # Exercise.
    result = fetch_noaa._add_to_files("SOLAR1", "mag", new_data)

    # Verify - file rewritten with T1, T2 (new value), T3; no duplicate rows.
    assert existing_file in result
    written = pd.read_csv(existing_file)
    assert list(written["time_tag"]) == [
        "2026-07-21T08:00:00",
        "2026-07-21T09:00:00",
        "2026-07-21T10:00:00",
    ]
    assert list(written["bx_gsm"]) == [1.0, 99.0, 3.0]


def test_add_to_files_rewrites_file_when_new_data_has_more_columns(
    fetch_noaa: FetchNOAA, tmp_path: Path
) -> None:
    # Set up - existing file has only bx_gsm; new data also has by_gsm.
    existing_file = tmp_path / "existing.csv"
    _write_csv(
        existing_file,
        pd.DataFrame({"time_tag": ["2026-07-21T08:00:00"], "bx_gsm": [1.0]}),
    )
    fetch_noaa._datastore_finder.find_by_handler.return_value = existing_file  # type: ignore

    new_data = pd.DataFrame(
        {
            "time_tag": ["2026-07-21T09:00:00"],
            "bx_gsm": [2.0],
            "by_gsm": [3.0],
        }
    )

    # Exercise.
    result = fetch_noaa._add_to_files("SOLAR1", "mag", new_data)

    # Verify - file rewritten with both rows; existing row has NaN for by_gsm.
    assert existing_file in result
    written = pd.read_csv(existing_file)
    assert list(written["time_tag"]) == ["2026-07-21T08:00:00", "2026-07-21T09:00:00"]
    assert list(written["bx_gsm"]) == [1.0, 2.0]
    assert pd.isna(
        written.loc[written["time_tag"] == "2026-07-21T08:00:00", "by_gsm"].iloc[0]
    )
    assert (
        written.loc[written["time_tag"] == "2026-07-21T09:00:00", "by_gsm"].iloc[0]
        == 3.0
    )


def test_add_to_files_multiple_days_produce_multiple_files(
    fetch_noaa: FetchNOAA, tmp_path: Path
) -> None:
    # Set up - data spans two calendar days.
    data = pd.DataFrame(
        {
            "time_tag": [
                "2026-07-21T08:00:00",
                "2026-07-21T09:00:00",
                "2026-07-22T08:00:00",
            ],
            "bx_gsm": [1.0, 2.0, 3.0],
        }
    )
    fetch_noaa._datastore_finder.find_by_handler.side_effect = [None, None]  # type: ignore

    # Exercise.
    result = fetch_noaa._add_to_files("SOLAR1", "mag", data)

    # Verify - one file per day, each in the work folder.
    assert len(result) == 2
    assert tmp_path / "SOLAR1_mag_noaa_20260721.csv" in result
    assert tmp_path / "SOLAR1_mag_noaa_20260722.csv" in result


# ---------------------------------------------------------------------------
# FetchNOAA.download_csv
# ---------------------------------------------------------------------------


def test_download_csv_invalid_spacecraft_raises(fetch_noaa: FetchNOAA) -> None:
    with pytest.raises(ValueError, match="BAD_CRAFT"):
        fetch_noaa.download_csv(spacecraft="BAD_CRAFT", instrument="mag")  # type: ignore


def test_download_csv_invalid_instrument_raises(fetch_noaa: FetchNOAA) -> None:
    with pytest.raises(ValueError, match="bad_instrument"):
        fetch_noaa.download_csv(spacecraft="SOLAR1", instrument="bad_instrument")  # type: ignore


def test_download_csv_no_data_returns_empty_dict(fetch_noaa: FetchNOAA) -> None:
    # Set up.
    fetch_noaa._data_access.get_data.return_value = []  # type: ignore

    # Exercise.
    result = fetch_noaa.download_csv(spacecraft="SOLAR1", instrument="mag")

    # Verify.
    assert result == {}
    fetch_noaa._data_access.get_data.assert_called_once_with(  # type: ignore
        spacecraft="SOLAR1", instrument="mag"
    )


@mock.patch("imap_mag.download.FetchNOAA._process_noaa_mag")
def test_download_csv_mag_calls_process_mag_and_add_to_files(
    mock_process_mag: mock.Mock, fetch_noaa: FetchNOAA
) -> None:
    # Set up.
    raw_data = [{"time_tag": "2026-07-21T08:00:00", "bx_gsm": 1.0}]
    fetch_noaa._data_access.get_data.return_value = raw_data  # type: ignore
    mock_process_mag.return_value = mock.sentinel.processed

    with mock.patch.object(
        fetch_noaa, "_add_to_files", return_value=mock.sentinel.files
    ) as mock_add:
        # Exercise.
        result = fetch_noaa.download_csv(spacecraft="SOLAR1", instrument="mag")

    # Verify.
    mock_process_mag.assert_called_once()
    mock_add.assert_called_once_with("SOLAR1", "mag", mock.sentinel.processed)
    assert result is mock.sentinel.files


@mock.patch("imap_mag.download.FetchNOAA._process_noaa_plasma")
def test_download_csv_plasma_calls_process_plasma_and_add_to_files(
    mock_process_plasma: mock.Mock, fetch_noaa: FetchNOAA
) -> None:
    # Set up.
    raw_data = [{"time_tag": "2026-07-21T08:00:00", "proton_speed": 400.0}]
    fetch_noaa._data_access.get_data.return_value = raw_data  # type: ignore
    mock_process_plasma.return_value = mock.sentinel.processed

    with mock.patch.object(
        fetch_noaa, "_add_to_files", return_value=mock.sentinel.files
    ) as mock_add:
        # Exercise.
        result = fetch_noaa.download_csv(spacecraft="ACE", instrument="plasma")

    # Verify.
    mock_process_plasma.assert_called_once()
    mock_add.assert_called_once_with("ACE", "plasma", mock.sentinel.processed)
    assert result is mock.sentinel.files
