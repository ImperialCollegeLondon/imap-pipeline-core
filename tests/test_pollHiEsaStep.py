import os
from datetime import datetime, timedelta, timezone

import pytest

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from prefect_server.pollHiEsaStep import (
    poll_hi45_esa_step_flow,
    poll_hi90_esa_step_flow,
)
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    NOW,
    TODAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401
from tests.util.webtcad_flow_helpers import (
    SAMPLE_CSV_WITH_DATA,
    assert_file_exists,
    assert_file_not_exists,
    define_available_latis_mapping,
    define_unavailable_latis_mapping,
    execute_webtcad_flow,
)

# (item, flow) pairs - parametrise every test across both Hi-45 and Hi-90 to
# guarantee both flows are exercised for every behaviour.
HI_FLOWS = [
    (HKWebTCADItems.HI45_ESA_STEP, poll_hi45_esa_step_flow),
    (HKWebTCADItems.HI90_ESA_STEP, poll_hi90_esa_step_flow),
]


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize("item,flow", HI_FLOWS)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_first_ever_run_with_automatic_run_parameters(
    item,
    flow,
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """With AutomaticRunParameters and no progress, download from beginning of IMAP only the day with data."""
    wiremock_manager.reset()

    define_available_latis_mapping(
        wiremock_manager,
        item,
        BEGINNING_OF_IMAP,
        BEGINNING_OF_IMAP + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_webtcad_flow(
        flow,
        wiremock_manager=wiremock_manager,
        prefect_test_fixture=prefect_test_fixture,
    )

    assert_file_exists(item, BEGINNING_OF_IMAP)

    workflow_progress = test_database.get_workflow_progress(item.name)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == BEGINNING_OF_IMAP


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize("item,flow", HI_FLOWS)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_automatic_run_with_no_args_downloads_only_new_data(
    item,
    flow,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """With AutomaticRunParameters and existing progress, only later days are downloaded."""
    wiremock_manager.reset()

    progress_timestamp = BEGINNING_OF_IMAP.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(item.name)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    next_day = BEGINNING_OF_IMAP + timedelta(days=1)

    define_available_latis_mapping(
        wiremock_manager,
        item,
        next_day,
        next_day + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )
    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_webtcad_flow(flow, wiremock_manager=wiremock_manager)

    assert_file_not_exists(item, BEGINNING_OF_IMAP)
    assert_file_exists(item, next_day)

    updated_progress = test_database.get_workflow_progress(item.name)
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == next_day


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize("item,flow", HI_FLOWS)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_manual_date_range_naive_dates(
    item,
    flow,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Manually specify start_date and end_date (naive datetimes) to download specific days."""
    wiremock_manager.reset()

    start_date = datetime(2026, 1, 15)
    end_date = datetime(2026, 1, 16)

    for day in (start_date, end_date):
        define_available_latis_mapping(
            wiremock_manager,
            item,
            day,
            day + timedelta(days=1),
            SAMPLE_CSV_WITH_DATA,
        )
    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_webtcad_flow(
        flow,
        wiremock_manager=wiremock_manager,
        start_date=start_date,
        end_date=end_date,
    )

    assert_file_exists(item, start_date)
    assert_file_exists(item, end_date)

    workflow_progress = test_database.get_workflow_progress(item.name)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == end_date


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize("item,flow", HI_FLOWS)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_manual_date_range_with_timezones(
    item,
    flow,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Date ranges with explicit (different) timezones are still resolved to the supplied calendar days."""
    wiremock_manager.reset()

    # The pipeline truncates to date-only resolution, so a tz-aware datetime on
    # 2026-01-15 (UTC+5) is treated as the 15th regardless of the literal UTC offset.
    start_date = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    end_date = datetime(2026, 1, 16, 23, 59, 0, tzinfo=timezone(timedelta(hours=-3)))

    for day in (datetime(2026, 1, 15), datetime(2026, 1, 16)):
        define_available_latis_mapping(
            wiremock_manager,
            item,
            day,
            day + timedelta(days=1),
            SAMPLE_CSV_WITH_DATA,
        )
    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_webtcad_flow(
        flow,
        wiremock_manager=wiremock_manager,
        start_date=start_date,
        end_date=end_date,
    )

    assert_file_exists(item, datetime(2026, 1, 15))
    assert_file_exists(item, datetime(2026, 1, 16))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize("item,flow", HI_FLOWS)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_already_up_to_date(
    item,
    flow,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When progress is at TODAY, AutomaticRunParameters is a no-op."""
    wiremock_manager.reset()

    progress_timestamp = TODAY.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(item.name)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_webtcad_flow(flow, wiremock_manager=wiremock_manager)

    updated_progress = test_database.get_workflow_progress(item.name)
    assert updated_progress.get_progress_timestamp() == progress_timestamp
