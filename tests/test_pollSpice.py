import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from imap_mag.util import DatetimeProvider, Environment
from prefect_server.pollSpice import poll_spice_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    END_OF_TODAY,
    NOW,
    TODAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401

PROGRESS_ITEM_ID = "SPICE"


def define_available_spice_data_sdc_mappings(
    wiremock_manager,
    ingest_start_day: datetime,
    ingest_end_date: datetime,
    ingestion_timestamp: datetime,
    test_spice_file: Path,
):
    """Set up wiremock mappings for available SPICE data."""
    start_date_str = ingest_start_day.strftime("%Y%m%d")
    end_date_str = ingest_end_date.strftime("%Y%m%d")

    query_response: list[dict] = [
        {
            "file_name": "sclk/imap_sclk_0032.tsc",
            "file_root": "imap_sclk_.tsc",
            "kernel_type": "spacecraft_clock",
            "version": 32,
            "min_date_j2000": 315576066.1839245,
            "max_date_j2000": 4575787269.183866,
            "file_intervals_j2000": [[315576066.1839245, 4575787269.183866]],
            "min_date_datetime": "2010-01-01, 00:00:00",
            "max_date_datetime": "2145-01-01, 00:00:00",
            "file_intervals_datetime": [["2010-01-01T00:00:00", "2145-01-01T00:00:00"]],
            "min_date_sclk": "1/0000000000:00000",
            "max_date_sclk": "1/4260214608:42276",
            "file_intervals_sclk": [["1/0000000000:00000", "1/4260214608:42276"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": ingestion_timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
            "timestamp": ingestion_timestamp.timestamp(),
        }
    ]

    wiremock_manager.add_string_mapping(
        f"/spice-query?start_ingest_date={start_date_str}&end_ingest_date={end_date_str}",
        json.dumps(query_response),
        priority=1,
    )

    # Add download endpoint for the SPICE file
    wiremock_manager.add_file_mapping(
        "/download/imap/spice/sclk/imap_sclk_0032.tsc",
        test_spice_file,
    )


def define_unavailable_spice_data_sdc_mappings(wiremock_manager):
    """Set up wiremock mappings for no available SPICE data."""
    wiremock_manager.add_string_mapping(
        "/spice-query\\?start_ingest_date=\\d{8}&end_ingest_date=\\d{8}",
        json.dumps([]),
        is_pattern=True,
        priority=2,
    )


def check_spice_file_existence(datastore: Path, should_exist: bool = True):
    """Check if SPICE file exists in datastore."""
    spice_file = datastore / "spice" / "sclk" / "imap_sclk_0032.tsc"
    if should_exist:
        assert spice_file.exists(), f"Expected SPICE file to exist at {spice_file}"
    else:
        assert not spice_file.exists(), (
            f"Expected SPICE file to NOT exist at {spice_file}"
        )


@pytest.mark.skipif(
    bool(os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows"),
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spice_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    """Test downloading a new SPICE file for the first time (no previous progress)."""
    # Set up.
    ingestion_timestamp = DatetimeProvider.now() - timedelta(hours=14)

    # Use the actual test SPICE file
    test_spice_file = Path("tests/test_data/spice/imap_sclk_0032.tsc")
    destination_spice_file = clean_datastore / "spice" / "sclk" / "imap_sclk_0032.tsc"
    if destination_spice_file.exists():
        destination_spice_file.unlink()

    wiremock_manager.reset()

    # Some data is available for SPICE from beginning of IMAP to end of today
    define_available_spice_data_sdc_mappings(
        wiremock_manager,
        BEGINNING_OF_IMAP,
        END_OF_TODAY,
        ingestion_timestamp,
        test_spice_file,
    )

    # Exercise.
    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url(),
        IMAP_API_KEY="12345",
    ):
        await poll_spice_flow()

    # Verify.
    # Database should be updated with progress
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    # this assert should be within 2 seconds of NOW
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == ingestion_timestamp

    # File should exist in datastore
    check_spice_file_existence(clean_datastore, should_exist=True)


@pytest.mark.skipif(
    bool(os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows"),
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spice_autoflow_no_new_data(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    """Test that nothing is downloaded when there is no new data (file already downloaded)."""
    # Set up.
    # Simulate that we already downloaded data up to this timestamp
    previous_progress_timestamp = datetime(2025, 4, 2, 13, 37, 9)

    wiremock_manager.reset()

    # Set previous progress in database
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(previous_progress_timestamp)
    test_database.save(workflow_progress)

    # No new data is available (empty response)
    define_unavailable_spice_data_sdc_mappings(wiremock_manager)

    # Exercise.
    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url(),
        IMAP_API_KEY="12345",
    ):
        await poll_spice_flow()

    # Verify.
    # Database progress should be updated with last checked date but progress timestamp should remain the same
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    # Progress timestamp should not change since no new data was found
    assert workflow_progress.get_progress_timestamp() == previous_progress_timestamp

    # No files should be downloaded
    check_spice_file_existence(clean_datastore, should_exist=False)


@pytest.mark.skipif(
    bool(os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows"),
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spice_autoflow_download_newer_file(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    clean_datastore,
):
    """Test downloading a newer file and verify progress is updated correctly."""
    # Set up.
    # Previous progress timestamp
    previous_progress_timestamp = TODAY + timedelta(hours=5, minutes=30)
    # New ingestion timestamp (newer than previous)
    new_ingestion_timestamp = previous_progress_timestamp + timedelta(
        hours=2, minutes=37, seconds=9
    )

    # Use the actual test SPICE file
    test_spice_file = Path("tests/test_data/spice/imap_sclk_0032.tsc")

    wiremock_manager.reset()

    # Set previous progress in database
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(previous_progress_timestamp)
    test_database.save(workflow_progress)

    # New data is available from the previous progress timestamp to end of today
    define_available_spice_data_sdc_mappings(
        wiremock_manager,
        previous_progress_timestamp,
        END_OF_TODAY,
        new_ingestion_timestamp,
        test_spice_file,
    )

    # Exercise.
    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url(),
        IMAP_API_KEY="12345",
    ):
        await poll_spice_flow()

    # Verify.
    # Database should be updated with new progress timestamp
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == new_ingestion_timestamp

    # File should exist in datastore
    check_spice_file_existence(clean_datastore, should_exist=True)
