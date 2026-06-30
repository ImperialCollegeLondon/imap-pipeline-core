import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from imap_mag.config.AppSettings import AppSettings
from imap_mag.io.file.IALiRTPathHandler import IALiRTPathHandler
from imap_mag.util import DatetimeProvider, Environment
from imap_mag.util.constants import CONSTANTS
from prefect_server.pollIALiRT import (
    FetchByDatesRunParameters,
    poll_ialirt_flow,
)
from tests.util.database import test_database  # noqa: F401
from tests.util.prefect_test_utils import (  # noqa: F401
    mock_teams_webhook_block,
    prefect_test_fixture,
)

NOW = datetime.now(UTC)
YESTERDAY = NOW - timedelta(days=1)
END_OF_HOUR = NOW.replace(minute=59, second=59, microsecond=0)
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)


def check_file_existence(
    actual_timestamp: datetime, folder_name: str, file_prefix: str
):
    datastore = AppSettings().data_store  # type: ignore
    data_folder = os.path.join(
        datastore, folder_name, actual_timestamp.strftime("%Y/%m")
    )
    cdf_file = f"{file_prefix}_{actual_timestamp.strftime('%Y%m%d')}.csv"

    assert os.path.exists(os.path.join(data_folder, cdf_file)), (
        f"Expected file {cdf_file} not found in {data_folder}"
    )


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
            "mag_theta_B_GSM": 25.017,
            "time_utc": start_date_str,
            "extra_field1": "extra_value1",
        },
        {
            "mag_B_GSE": [4.187, 0.687, 0.757],
            "mag_theta_B_GSM": 6.732,
            "time_utc": end_date_str,
            "extra_field2": "extra_value2",
        },
    ]

    wiremock_manager.add_string_mapping(
        r"/space-weather\?instrument=mag&.*",
        json.dumps({"meta": {"count": 2, "instrument": "mag"}, "data": query_response}),
        is_pattern=True,
        priority=1,
    )


def define_fallback_mapping(wiremock_manager):
    """
    Prevents the data downloader from infinite-looping on un-mocked instruments.
    """
    future_date_str = (datetime.now(UTC) + timedelta(days=365)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    wiremock_manager.add_string_mapping(
        r"/space-weather\?instrument=.*",
        json.dumps(
            {
                "meta": {"count": 1, "instrument": "unknown"},
                "data": [{"time_utc": future_date_str}],
            }
        ),
        is_pattern=True,
        priority=2,
    )


def datastore_csv_path(instrument: str, date: datetime) -> str:
    """Return the absolute datastore path of the CSV the pipeline should produce for  ``instrument`` and ``date``."""
    datastore_root = AppSettings().data_store  # type: ignore

    handler = IALiRTPathHandler(instrument=instrument, content_date=date)
    return os.path.join(
        datastore_root, handler.get_folder_structure(), handler.get_filename()
    )


def assert_file_exists(instrument: str, date: datetime) -> None:
    """Assert that the pipeline produced a CSV in the datastore for ``date``."""
    path = datastore_csv_path(instrument, date)
    assert os.path.exists(path), f"Expected file not found at {path}"


def verify_available_ialirt(
    database,
    expected_progress_timestamp: datetime,
    actual_timestamp: datetime,
    expected_check_time: datetime,
):
    workflow_progress = database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_PROGRESS_ID
    )
    last_checked = workflow_progress.get_last_checked_date()

    if last_checked.tzinfo is None and expected_check_time.tzinfo is not None:
        expected_check_time = expected_check_time.replace(tzinfo=None)

    last_checked = last_checked.replace(microsecond=0)
    expected_check_time = expected_check_time.replace(microsecond=0)

    time_difference = abs((last_checked - expected_check_time).total_seconds())

    assert time_difference < 60

    expected_progress_timestamp = expected_progress_timestamp.replace(tzinfo=None)

    assert workflow_progress.get_progress_timestamp() == expected_progress_timestamp


pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",  # type: ignore
    reason="Wiremock test containers will not work on Windows Github Runner",
)


@pytest.mark.timeout(10)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_first_ever_run_mag(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    wiremock_manager.reset()
    define_fallback_mapping(wiremock_manager)
    define_available_ialirt_mappings(wiremock_manager, YESTERDAY, END_OF_HOUR)

    with (
        patch(
            "prefect_server.pollIALiRT.get_secret_or_env_var", new_callable=AsyncMock
        ) as mock_secret,
        patch("prefect_server.pollIALiRT.VALID_IALIRT_INSTRUMENTS", ["mag"]),
        patch("prefect_server.pollIALiRT.VALID_IALIRT_HK_INSTRUMENTS", []),
    ):
        mock_secret.return_value = "12345"

        with Environment(
            IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
            PREFECT_TEST_MODE="1",
        ):
            bounded_parameters = FetchByDatesRunParameters(
                start_date=YESTERDAY, end_date=END_OF_HOUR
            )
            await poll_ialirt_flow(
                run_parameters=bounded_parameters,
                wait_for_new_data_to_arrive=False,
                plot_last_3_days=False,
                datetime_provider=DatetimeProvider(fixed_now=NOW),
            )

    verify_available_ialirt(
        database=test_database,
        expected_progress_timestamp=END_OF_HOUR.replace(microsecond=0),
        actual_timestamp=TODAY,
        expected_check_time=NOW,
    )

    assert_file_exists("mag", TODAY)
