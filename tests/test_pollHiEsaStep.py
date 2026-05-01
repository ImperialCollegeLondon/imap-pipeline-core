import os
import re
from datetime import datetime, timedelta, timezone

import pytest

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    ProgressUpdateMode,
)
from imap_mag.util import Environment
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

SAMPLE_CSV_WITH_DATA = (
    "time,value\n"
    "2026-02-11T14:00:00.000,1\n"
    "2026-02-11T14:01:00.000,2\n"
    "2026-02-11T14:02:00.000,3\n"
)

SAMPLE_CSV_EMPTY = "time,value\n"

WEBTCAD_URL_ENDPOINT_PATH = "/AnalogTelemetryItem_SID1"


def progress_item_for(item: HKWebTCADItems) -> str:
    return f"{item.instrument.short_name.upper()}_ESA_STEP"


def define_available_latis_mapping(
    wiremock_manager,
    item: HKWebTCADItems,
    start_date: datetime,
    end_date: datetime,
    csv_content: str,
):
    """Add WireMock mapping for a WebTCAD LaTiS CSV download for the given TMID."""
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    url = (
        f"{WEBTCAD_URL_ENDPOINT_PATH}.csv"
        f"?TMID={item.tmid}"
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


def define_unavailable_latis_mapping(wiremock_manager, item: HKWebTCADItems):
    """Add WireMock fallback mapping that returns empty CSV for any unmatched request."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{WEBTCAD_URL_ENDPOINT_PATH}.csv?TMID={item.tmid}") + r".*",
        SAMPLE_CSV_EMPTY,
        is_pattern=True,
        priority=2,
    )


def check_file_existence(item: HKWebTCADItems, date: datetime, negate=False):
    """Verify that a CSV file exists in the datastore for the given date."""

    datastore_path = AppSettings().data_store
    file_start = f"imap_{item.instrument.short_name}_l1_{item.descriptor}"

    csv_folder = os.path.join(
        datastore_path,
        f"hk/{item.instrument.short_name}/l1",
        item.descriptor,
        date.strftime("%Y/%m"),
    )
    csv_file = f"{file_start}_{date.strftime('%Y%m%d')}_v001.csv"

    if negate:
        assert not os.path.exists(os.path.join(csv_folder, csv_file)), (
            f"File {csv_file} should not exist in {csv_folder}"
        )
    else:
        assert os.path.exists(os.path.join(csv_folder, csv_file)), (
            f"Expected file {csv_file} not found in {csv_folder}"
        )


def flow_for(item: HKWebTCADItems):
    if item == HKWebTCADItems.HI45_ESA_STEP:
        return poll_hi45_esa_step_flow
    if item == HKWebTCADItems.HI90_ESA_STEP:
        return poll_hi90_esa_step_flow
    raise ValueError(f"Unsupported HKWebTCADItems item: {item}")


async def execute_flow_under_test(
    wiremock_manager,
    item: HKWebTCADItems,
    start_date=None,
    end_date=None,
    force_redownload=False,
    use_database=True,
    prefect_test_fixture=None,  # noqa: F811
    do_not_update_progress=False,
):
    flow = flow_for(item)

    with Environment(
        MAG_FETCH_WEBTCAD_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        func_under_test = flow if prefect_test_fixture else flow.fn

        mode = (
            ProgressUpdateMode.NEVER_UPDATE_PROGRESS
            if do_not_update_progress
            else ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER
        )

        if not start_date and not end_date and not force_redownload:
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


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "item",
    [HKWebTCADItems.HI45_ESA_STEP, HKWebTCADItems.HI90_ESA_STEP],
)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_first_ever_run_with_automatic_run_parameters(
    item,
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

    await execute_flow_under_test(
        wiremock_manager, item, prefect_test_fixture=prefect_test_fixture
    )

    check_file_existence(item, BEGINNING_OF_IMAP)

    workflow_progress = test_database.get_workflow_progress(progress_item_for(item))
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == BEGINNING_OF_IMAP


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "item",
    [HKWebTCADItems.HI45_ESA_STEP, HKWebTCADItems.HI90_ESA_STEP],
)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_automatic_run_with_no_args_downloads_only_new_data(
    item,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When AutomaticRunParameters is used and progress already exists, only days after the last progress are downloaded."""
    wiremock_manager.reset()

    # Mark progress as already at BEGINNING_OF_IMAP (end-of-day)
    progress_timestamp = BEGINNING_OF_IMAP.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(progress_item_for(item))
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

    # Pass None as run_parameters via AutomaticRunParameters (the flow signature requires the model)
    await execute_flow_under_test(wiremock_manager, item)

    # The day before BEGINNING_OF_IMAP is *not* re-downloaded
    check_file_existence(item, BEGINNING_OF_IMAP, negate=True)
    # The next day after progress is downloaded
    check_file_existence(item, next_day)

    updated_progress = test_database.get_workflow_progress(progress_item_for(item))
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == next_day


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "item",
    [HKWebTCADItems.HI45_ESA_STEP, HKWebTCADItems.HI90_ESA_STEP],
)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_manual_date_range_naive_dates(
    item,
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

    await execute_flow_under_test(wiremock_manager, item, start_date, end_date)

    check_file_existence(item, start_date)
    check_file_existence(item, end_date)

    workflow_progress = test_database.get_workflow_progress(progress_item_for(item))
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == end_date


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "item",
    [HKWebTCADItems.HI45_ESA_STEP, HKWebTCADItems.HI90_ESA_STEP],
)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_manual_date_range_with_timezones(
    item,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Date ranges in different timezones should still target the correct UTC days."""
    wiremock_manager.reset()

    # 2026-01-15 in UTC+5 == 2026-01-14 19:00 UTC, but the date-only resolution
    # used by the pipeline truncates to the midnight of the supplied day, so
    # the requests will cover 2026-01-15 and 2026-01-16 against WebTCAD.
    start_date = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    end_date = datetime(2026, 1, 16, 23, 59, 0, tzinfo=timezone(timedelta(hours=-3)))

    naive_start = start_date.replace(tzinfo=None)
    naive_end = end_date.replace(tzinfo=None)

    for day in (
        datetime(naive_start.year, naive_start.month, naive_start.day),
        datetime(naive_end.year, naive_end.month, naive_end.day),
    ):
        define_available_latis_mapping(
            wiremock_manager,
            item,
            day,
            day + timedelta(days=1),
            SAMPLE_CSV_WITH_DATA,
        )
    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_flow_under_test(wiremock_manager, item, start_date, end_date)

    check_file_existence(item, datetime(2026, 1, 15))
    check_file_existence(item, datetime(2026, 1, 16))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "item",
    [HKWebTCADItems.HI45_ESA_STEP, HKWebTCADItems.HI90_ESA_STEP],
)
@pytest.mark.asyncio
async def test_poll_hi_esa_step_already_up_to_date(
    item,
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When progress is at TODAY, AutomaticRunParameters does nothing."""
    wiremock_manager.reset()

    progress_timestamp = TODAY.replace(hour=23, minute=59, second=59)
    workflow_progress = test_database.get_workflow_progress(progress_item_for(item))
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    define_unavailable_latis_mapping(wiremock_manager, item)

    await execute_flow_under_test(wiremock_manager, item)

    updated_progress = test_database.get_workflow_progress(progress_item_for(item))
    assert updated_progress.get_progress_timestamp() == progress_timestamp
