import os
from datetime import datetime, timedelta

import pytest

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from prefect_server.pollLoPivotPlatform import poll_lo_pivot_platform_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    NOW,
    TODAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401
from tests.util.webtcad_flow_helpers import (
    SAMPLE_CSV_EMPTY,
    SAMPLE_CSV_WITH_DATA,
    assert_file_exists,
    assert_file_not_exists,
    define_available_latis_mapping,
    define_unavailable_latis_mapping,
    execute_webtcad_flow,
)

ITEM = HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE
PROGRESS_ITEM_ID = ITEM.name


async def execute_flow_under_test(wiremock_manager, **kwargs):
    await execute_webtcad_flow(
        poll_lo_pivot_platform_flow,
        wiremock_manager=wiremock_manager,
        **kwargs,
    )


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

    define_available_latis_mapping(
        wiremock_manager,
        ITEM,
        BEGINNING_OF_IMAP,
        BEGINNING_OF_IMAP + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(
        wiremock_manager, prefect_test_fixture=prefect_test_fixture
    )

    assert_file_exists(ITEM, BEGINNING_OF_IMAP)

    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
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
    """First run with no database still produces files in the datastore."""
    wiremock_manager.reset()

    define_available_latis_mapping(
        wiremock_manager,
        ITEM,
        BEGINNING_OF_IMAP,
        BEGINNING_OF_IMAP + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(
        wiremock_manager,
        use_database=False,
        prefect_test_fixture=prefect_test_fixture,
    )

    assert_file_exists(ITEM, BEGINNING_OF_IMAP)


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

    progress_timestamp = BEGINNING_OF_IMAP.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    next_day = BEGINNING_OF_IMAP + timedelta(days=1)

    define_available_latis_mapping(
        wiremock_manager,
        ITEM,
        next_day,
        next_day + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(wiremock_manager)

    assert_file_exists(ITEM, next_day)

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

    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(wiremock_manager)

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

    for day in (start_date, end_date):
        define_available_latis_mapping(
            wiremock_manager,
            ITEM,
            day,
            day + timedelta(days=1),
            SAMPLE_CSV_WITH_DATA,
        )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(
        wiremock_manager, start_date=start_date, end_date=end_date
    )

    assert_file_exists(ITEM, start_date)
    assert_file_exists(ITEM, end_date)

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
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(current_progress_date)
    test_database.save(workflow_progress)

    define_available_latis_mapping(
        wiremock_manager,
        ITEM,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(
        wiremock_manager,
        start_date=start_date,
        end_date=end_date,
        force_redownload=True,
    )

    assert_file_exists(ITEM, start_date)

    # Force redownload mode must NOT advance the existing newer progress timestamp
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
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(current_progress_date)
    test_database.save(workflow_progress)

    define_available_latis_mapping(
        wiremock_manager,
        ITEM,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(
        wiremock_manager,
        start_date=start_date,
        end_date=end_date,
        do_not_update_progress=True,
    )

    assert_file_exists(ITEM, start_date)

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

    progress_timestamp = TODAY.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(wiremock_manager)

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

    define_available_latis_mapping(
        wiremock_manager, ITEM, day1, day1 + timedelta(days=1), SAMPLE_CSV_EMPTY
    )
    define_available_latis_mapping(
        wiremock_manager, ITEM, day2, day2 + timedelta(days=1), SAMPLE_CSV_WITH_DATA
    )
    define_available_latis_mapping(
        wiremock_manager, ITEM, day3, day3 + timedelta(days=1), SAMPLE_CSV_EMPTY
    )
    define_unavailable_latis_mapping(wiremock_manager, ITEM)

    await execute_flow_under_test(wiremock_manager, start_date=day1, end_date=day3)

    assert_file_not_exists(ITEM, day1)
    assert_file_exists(ITEM, day2)
    assert_file_not_exists(ITEM, day3)

    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_progress_timestamp() == day2
