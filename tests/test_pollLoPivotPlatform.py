import os
import re
from datetime import datetime, timedelta

import pytest

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    ProgressUpdateMode,
)
from imap_mag.data_pipelines.LoPivotPlatformPipeline import LoPivotPlatformPipeline
from imap_mag.util import Environment
from imap_mag.util.Subsystem import Subsystem
from prefect_server.pollLoPivotPlatform import (
    poll_lo_pivot_platform_flow,
)
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    NOW,
    TODAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401

SAMPLE_CSV_WITH_DATA = (
    "time,value\n"
    "2026-02-11T14:00:00.000,45.123\n"
    "2026-02-11T14:01:00.000,45.234\n"
    "2026-02-11T14:02:00.000,45.345\n"
)

SAMPLE_CSV_EMPTY = "time,value\n"

WEBTCAD_URL_ENDPOINT_PATH = "/AnalogTelemetryItem_SID1"

PROGRESS_ITEM_ID = LoPivotPlatformPipeline.PROGRESS_ITEM_ID


def define_available_latis_mapping(
    wiremock_manager,
    start_date: datetime,
    end_date: datetime,
    csv_content: str,
):
    """Add WireMock mapping for a WebTCAD LaTiS CSV download."""
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    url = (
        f"{WEBTCAD_URL_ENDPOINT_PATH}.csv"
        f"?TMID=58350"
        f"&time,value"
        f"&time%3E={start_str}"
        f"&time%3C={end_str}"
        f"&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
    )

    wiremock_manager.add_string_mapping(
        url,
        csv_content,
        priority=1,
    )


def define_unavailable_latis_mapping(wiremock_manager):
    """Add WireMock fallback mapping that returns empty CSV for any unmatched request."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{WEBTCAD_URL_ENDPOINT_PATH}.csv?TMID=58350") + r".*",
        SAMPLE_CSV_EMPTY,
        is_pattern=True,
        priority=2,
    )


def check_file_existence(date: datetime, negate=False):
    """Verify that a CSV file exists in the datastore for the given date."""

    datastore_path = AppSettings().data_store
    imap_lo_file_start = f"imap_{Subsystem.LO.short_name}_l1_{HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE.descriptor}"

    csv_folder = os.path.join(
        datastore_path,
        "hk/lo/l1",
        HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE.descriptor,
        date.strftime("%Y/%m"),
    )
    csv_file = f"{imap_lo_file_start}_{date.strftime('%Y%m%d')}_v001.csv"

    if negate:
        assert not os.path.exists(os.path.join(csv_folder, csv_file)), (
            f"File {csv_file} should not exist in {csv_folder}"
        )
    else:
        assert os.path.exists(os.path.join(csv_folder, csv_file)), (
            f"Expected file {csv_file} not found in {csv_folder}"
        )


def check_file_not_exists(date: datetime):
    """Verify that a CSV file does not exist in the datastore for the given date."""
    check_file_existence(date, negate=True)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On first run with no progress, download from beginning of IMAP to today."""
    wiremock_manager.reset()

    # Data available for BEGINNING_OF_IMAP day only
    define_available_latis_mapping(
        wiremock_manager,
        BEGINNING_OF_IMAP,
        BEGINNING_OF_IMAP + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    # No data for other days
    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(
        wiremock_manager, prefect_test_fixture=prefect_test_fixture
    )

    # Verify file exists for BEGINNING_OF_IMAP
    check_file_existence(BEGINNING_OF_IMAP)

    # Verify workflow progress was updated
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW, (
        "Last checked date should be set during the flow to the current now time"
    )
    assert workflow_progress.get_progress_timestamp() == BEGINNING_OF_IMAP


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_without_a_database(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On first run with no progress, download from beginning of IMAP to today."""
    wiremock_manager.reset()

    # Data available for BEGINNING_OF_IMAP day only
    define_available_latis_mapping(
        wiremock_manager,
        BEGINNING_OF_IMAP,
        BEGINNING_OF_IMAP + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    # No data for other days
    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(
        wiremock_manager, use_database=False, prefect_test_fixture=prefect_test_fixture
    )

    # Verify file exists for BEGINNING_OF_IMAP
    check_file_existence(BEGINNING_OF_IMAP)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_continue_from_previous(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On subsequent run, start from day after last progress."""
    wiremock_manager.reset()

    # Set progress to BEGINNING_OF_IMAP (already downloaded that day)
    progress_timestamp = BEGINNING_OF_IMAP.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    # The next day to download is BEGINNING_OF_IMAP + 1 day (= YESTERDAY)
    next_day = BEGINNING_OF_IMAP + timedelta(days=1)

    define_available_latis_mapping(
        wiremock_manager,
        next_day,
        next_day + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(wiremock_manager)

    # Verify file exists for the next day
    check_file_existence(next_day)

    # Verify workflow progress was updated to the next day
    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == next_day


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_no_new_data(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When all days return empty CSV, progress timestamp stays unchanged."""
    wiremock_manager.reset()

    # Only empty data available
    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(wiremock_manager)

    # Verify workflow progress was checked but not advanced
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_manual_date_range(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Manually specify start_date and end_date to download specific days."""
    wiremock_manager.reset()

    start_date = datetime(2026, 1, 15)
    end_date = datetime(2026, 1, 16)

    define_available_latis_mapping(
        wiremock_manager,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_available_latis_mapping(
        wiremock_manager,
        end_date,
        end_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(wiremock_manager, start_date, end_date)

    # Verify files exist for both days
    check_file_existence(start_date)
    check_file_existence(end_date)

    # Verify workflow progress was updated (use_database=True for non-force mode)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == end_date


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_force_redownload_does_download_file_already_downloaded_in_progress(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    wiremock_manager.reset()

    start_date = datetime(2026, 1, 15)
    end_date = datetime(2026, 1, 15)
    current_progress_date = end_date + timedelta(days=1)
    # already downloaded start_date
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(current_progress_date)
    test_database.save(workflow_progress)

    define_available_latis_mapping(
        wiremock_manager,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(
        wiremock_manager, start_date, end_date, force_redownload=True
    )

    # Verify file exists
    check_file_existence(start_date)

    # Verify workflow progress was NOT updated (force redownload mode)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_progress_timestamp() == current_progress_date


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_dont_update_progress_if_option_not_to_is_set(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    wiremock_manager.reset()

    start_date = datetime(2026, 1, 15)
    end_date = datetime(2026, 1, 15)
    current_progress_date = start_date - timedelta(days=1)
    # already downloaded start_date
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(current_progress_date)
    test_database.save(workflow_progress)

    define_available_latis_mapping(
        wiremock_manager,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(
        wiremock_manager,
        start_date,
        end_date,
        do_not_update_progress=True,
    )

    # Verify file exists
    check_file_existence(start_date)

    # Verify workflow progress was NOT updated (do not update progress mode)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_progress_timestamp() == current_progress_date


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_already_up_to_date(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When progress is already at today, nothing should be downloaded."""
    wiremock_manager.reset()

    # Set progress to today (already fully up to date)
    progress_timestamp = TODAY.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(wiremock_manager)

    # Progress should remain the same
    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_progress_timestamp() == progress_timestamp


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_lo_pivot_platform_skips_days_without_data(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Days with empty data should be skipped but later days with data should be saved."""
    wiremock_manager.reset()

    day1 = datetime(2026, 1, 10)
    day2 = datetime(2026, 1, 11)
    day3 = datetime(2026, 1, 12)

    # day1 has no data, day2 has data, day3 has no data
    define_available_latis_mapping(
        wiremock_manager,
        day1,
        day1 + timedelta(days=1),
        SAMPLE_CSV_EMPTY,
    )
    define_available_latis_mapping(
        wiremock_manager,
        day2,
        day2 + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_available_latis_mapping(
        wiremock_manager,
        day3,
        day3 + timedelta(days=1),
        SAMPLE_CSV_EMPTY,
    )
    define_unavailable_latis_mapping(wiremock_manager)

    await execute_flow_under_test(wiremock_manager, day1, day3)

    # Only day2 should have a file
    check_file_not_exists(day1)
    check_file_existence(day2)
    check_file_not_exists(day3)

    # Progress should be set to day2 (last day with data)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_progress_timestamp() == day2


async def execute_flow_under_test(
    wiremock_manager,
    start_date=None,
    end_date=None,
    force_redownload=False,
    use_database=True,
    prefect_test_fixture=None,  # noqa: F811
    do_not_update_progress=False,
):
    with Environment(
        MAG_FETCH_WEBTCAD_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        # If prefect_test_fixture is provided, it means we are running within a Prefect test and should call the flow function directly. Otherwise, we are running the test without Prefect and should call the flow as a normal async function.
        func_under_test = (
            poll_lo_pivot_platform_flow
            if prefect_test_fixture
            else poll_lo_pivot_platform_flow.fn
        )

        mode = (
            ProgressUpdateMode.NEVER_UPDATE_PROGRESS
            if do_not_update_progress
            else ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER
        )

        if not start_date and not end_date and not force_redownload:
            # For the default automatic mode test, we want to run the flow as it would normally be run without parameters
            await func_under_test(
                run_parameters=AutomaticRunParameters(progress_mode=mode),
                use_database=use_database,
            )
        else:
            await func_under_test(
                run_parameters=FetchByDatesRunParameters(
                    start_date=start_date,
                    end_date=end_date,
                    force_redownload=force_redownload,
                    progress_mode=mode,
                ),
                use_database=use_database,
            )
