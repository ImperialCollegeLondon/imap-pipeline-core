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
