import json
import os
from datetime import datetime, timedelta
from urllib.parse import quote

import pytest

from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.pollIALiRT import poll_ialirt_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_HOUR,
    NOW,
    TODAY,
    YESTERDAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_fixture import prefect_test_fixture  # noqa: F401


def define_available_ialirt_mappings(
    wiremock_manager,
    start_date: datetime,
    end_date: datetime,
):
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    query_response: list[dict] = [
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
            "met_in_utc": start_date_str,
            "last_modified": start_date_str,
            "extra_field1": "extra_value1",
        },
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
            "met_in_utc": end_date_str,
            "last_modified": end_date_str,
            "extra_field2": "extra_value2",
        },
    ]

    wiremock_manager.add_string_mapping(
        f"/ialirt-db-query?met_in_utc_start={quote(start_date_str)}&met_in_utc_end={quote(end_date_str)}",
        json.dumps(query_response),
        priority=1,
    )


def verify_available_ialirt(
    database,
    progress_timestamp: datetime,
    actual_timestamp: datetime,
):
    # Database.
    workflow_progress = database.get_workflow_progress("MAG_IALIRT")

    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == progress_timestamp

    # Files.
    check_file_existence(actual_timestamp)


def check_file_existence(actual_timestamp: datetime):
    datastore = AppSettings().data_store  # type: ignore
    data_folder = os.path.join(datastore, "ialirt", actual_timestamp.strftime("%Y/%m"))
    cdf_file = f"imap_ialirt_{actual_timestamp.strftime('%Y%m%d')}.csv"

    assert os.path.exists(os.path.join(data_folder, cdf_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    # Set up.
    wiremock_manager.reset()

    define_available_ialirt_mappings(wiremock_manager, YESTERDAY, END_OF_HOUR)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(wait_for_new_data_to_arrive=False)

    # Verify.
    verify_available_ialirt(
        test_database,
        END_OF_HOUR.replace(microsecond=0),  # I-ALiRT does not use microsecond accuracy
        TODAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_continue_from_previous_download(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    # Set up.
    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)

    workflow_progress = test_database.get_workflow_progress("MAG_IALIRT")
    workflow_progress.record_successful_download(progress_timestamp)

    test_database.save(workflow_progress)
    wiremock_manager.reset()

    define_available_ialirt_mappings(wiremock_manager, progress_timestamp, END_OF_HOUR)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(wait_for_new_data_to_arrive=False)

    # Verify.
    verify_available_ialirt(
        test_database,
        END_OF_HOUR.replace(microsecond=0),  # I-ALiRT does not use microsecond accuracy
        TODAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_specify_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    # Set up.
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)

    wiremock_manager.reset()

    define_available_ialirt_mappings(wiremock_manager, start_date, end_date)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(
            wait_for_new_data_to_arrive=False, start_date=start_date, end_date=end_date
        )

    # Verify.
    verify_available_ialirt(
        test_database,
        end_date,
        start_date,
    )
