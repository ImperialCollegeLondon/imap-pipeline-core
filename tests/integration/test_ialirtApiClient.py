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
    # Mock data 1h apart
    response_chunk1: list[dict] = [
        {
            "mag_B_GSE": [-1.53, -3.033, 0.539],
            "mag_theta_B_GSM": 25.017,
            "time_utc": "2025-10-14T03:15:00",
        },
    ]
    response_chunk2: list[dict] = [
        {
            "mag_B_GSE": [4.187, 0.687, 0.757],
            "mag_theta_B_GSM": 6.732,
            "time_utc": "2025-10-14T04:15:00",
        },
    ]
    response_chunk3: list[dict] = [
        {
            "mag_B_GSE": [0.596, 3.986, -1.569],
            "mag_theta_B_GSM": 6.732,
            "time_utc": "2025-10-14T05:15:00",
        },
    ]

    # 1 hour increments
    wiremock_manager.add_string_mapping(
        "/space-weather?instrument=mag&time_utc_start=2025-10-14T03%3A00%3A00&time_utc_end=2025-10-14T04%3A00%3A00",
        json.dumps(
            {"meta": {"count": 1, "instrument": "mag"}, "data": response_chunk1}
        ),
    )
    wiremock_manager.add_string_mapping(
        "/space-weather?instrument=mag&time_utc_start=2025-10-14T04%3A00%3A00&time_utc_end=2025-10-14T05%3A00%3A00",
        json.dumps(
            {"meta": {"count": 1, "instrument": "mag"}, "data": response_chunk2}
        ),
    )
    wiremock_manager.add_string_mapping(
        "/space-weather?instrument=mag&time_utc_start=2025-10-14T05%3A00%3A00&time_utc_end=2025-10-14T06%3A00%3A00",
        json.dumps(
            {"meta": {"count": 1, "instrument": "mag"}, "data": response_chunk3}
        ),
    )

    ialirt_data_access = IALiRTApiClient(
        auth_code=None, sdc_url=wiremock_manager.get_url().rstrip("/")
    )

    downloaded_data: list[dict] = ialirt_data_access.get_all_by_dates(
        instrument="mag",
        start_date=datetime(2025, 10, 14, 3, 0, 0),
        end_date=datetime(2025, 10, 14, 6, 0, 0),
        max_hours_per_chunk=1,
    )

    # Verify.
    assert len(downloaded_data) == 3

    assert downloaded_data[0]["time_utc"] == "2025-10-14T03:15:00"
    assert downloaded_data[1]["time_utc"] == "2025-10-14T04:15:00"
    assert downloaded_data[2]["time_utc"] == "2025-10-14T05:15:00"

    # Verify the logs correctly report the 1-hour chunk boundaries
    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 03:00:00 and 2025-10-14 04:00:00."
        in capture_cli_logs.text
    )
    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 04:00:00 and 2025-10-14 05:00:00."
        in capture_cli_logs.text
    )
    assert (
        "Downloaded 1 records from I-ALiRT between 2025-10-14 05:00:00 and 2025-10-14 06:00:00."
        in capture_cli_logs.text
    )
