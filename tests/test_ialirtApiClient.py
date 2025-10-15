"""Tests for `IALiRTDataAccess` class."""

import json
import os
from datetime import datetime

import ialirt_data_access
import pytest
from pydantic import SecretStr

from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from tests.util.miscellaneous import temp_datastore  # noqa: F401


def test_ialirt_data_access_constructor_sets_config() -> None:
    # Set up.
    auth_code = SecretStr("some_auth_code")
    data_access_url = "https://some_test_url"

    # Exercise.
    _ = IALiRTApiClient(auth_code, data_access_url)

    # Verify.
    assert ialirt_data_access.config["API_KEY"] == auth_code.get_secret_value()
    assert ialirt_data_access.config["DATA_ACCESS_URL"] == data_access_url


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_download_ialirt_data_in_chunks(
    wiremock_manager,
    capture_cli_logs,
) -> None:
    # Set up.
    response_chunk1: list[dict] = [
        {
            "mag_B_GSE": [-1.53, -3.033, 0.539],
            "mag_hk_status": {
                "fib_temp": 2464,
                "mode": 6,
                "hk1v5c_warn": False,
                "hk3v3": 2880,
                "fob_range": 2,
            },
            "mag_theta_B_GSM": 25.017,
            "met_in_utc": "2025-10-14T03:00:10",
            "last_modified": "2025-10-14T03:01:02",
        },
    ]
    response_chunk2: list[dict] = [
        {
            "mag_B_GSE": [4.187, 0.687, 0.757],
            "mag_hk_status": {
                "fib_temp": 2464,
                "mode": 5,
                "hk1v5c_warn": False,
                "hk3v3": 2880,
                "fob_range": 2,
            },
            "mag_theta_B_GSM": 6.732,
            "met_in_utc": "2025-10-14T03:01:00",
            "last_modified": "2025-10-14T03:02:03",
        },
    ]
    response_chunk3: list[dict] = [
        {
            "mag_B_GSE": [0.596, 3.986, -1.569],
            "mag_hk_status": {
                "fib_temp": 2464,
                "mode": 4,
                "hk1v5c_warn": False,
                "hk3v3": 2880,
                "fob_range": 2,
            },
            "mag_theta_B_GSM": 6.732,
            "met_in_utc": "2025-10-14T03:02:00",
            "last_modified": "2025-10-14T03:02:03",
        },
    ]

    wiremock_manager.add_string_mapping(
        "/ialirt-db-query?met_in_utc_start=2025-10-14T03%3A00%3A00&met_in_utc_end=2025-10-14T03%3A03%3A00",
        json.dumps(response_chunk1),
    )
    wiremock_manager.add_string_mapping(
        "/ialirt-db-query?met_in_utc_start=2025-10-14T03%3A00%3A11&met_in_utc_end=2025-10-14T03%3A03%3A00",
        json.dumps(response_chunk2),
    )
    wiremock_manager.add_string_mapping(
        "/ialirt-db-query?met_in_utc_start=2025-10-14T03%3A01%3A01&met_in_utc_end=2025-10-14T03%3A03%3A00",
        json.dumps(response_chunk3),
    )
    wiremock_manager.add_string_mapping(
        "/ialirt-db-query?met_in_utc_start=2025-10-14T03%3A02%3A01&met_in_utc_end=2025-10-14T03%3A03%3A00",
        json.dumps({}),  # empty response means no more data
    )

    ialirt_data_access = IALiRTApiClient(
        auth_code=None, sdc_url=wiremock_manager.get_url().rstrip("/")
    )

    # Exercise.
    downloaded_data: list[dict] = ialirt_data_access.get_all_by_dates(
        start_date=datetime(2025, 10, 14, 3, 0, 0),
        end_date=datetime(2025, 10, 14, 3, 3, 0),
    )

    # Verify.
    assert len(downloaded_data) == 3

    assert downloaded_data[0]["met_in_utc"] == "2025-10-14T03:00:10"
    assert downloaded_data[1]["met_in_utc"] == "2025-10-14T03:01:00"
    assert downloaded_data[2]["met_in_utc"] == "2025-10-14T03:02:00"

    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 03:00:00 and 2025-10-14 03:00:10."
        in capture_cli_logs.text
    )
    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 03:00:11 and 2025-10-14 03:01:00."
        in capture_cli_logs.text
    )
    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 03:01:01 and 2025-10-14 03:02:00."
        in capture_cli_logs.text
    )
    assert (
        "No more data to download between 2025-10-14 03:02:01 and 2025-10-14 03:03:00."
        in capture_cli_logs.text
    )
