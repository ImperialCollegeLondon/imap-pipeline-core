import os
import re
from datetime import datetime, timedelta

import pytest

from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.pollWebTCADLaTiS import (
    PROGRESS_ITEM_ID,
    SC_HK_DESCRIPTOR,
    poll_webtcad_latis_flow,
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
        f"/AnalogTelemetryItem_SID1.csv"
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
        re.escape("/AnalogTelemetryItem_SID1.csv?TMID=58350") + r".*",
        SAMPLE_CSV_EMPTY,
        is_pattern=True,
        priority=2,
    )


def check_file_existence(datastore_path, date: datetime):
    """Verify that a CSV file exists in the datastore for the given date."""
    csv_folder = os.path.join(
        datastore_path,
        "hk/sc/l1",
        SC_HK_DESCRIPTOR,
        date.strftime("%Y/%m"),
    )
    csv_file = f"imap_sc_l1_{SC_HK_DESCRIPTOR}_{date.strftime('%Y%m%d')}_v001.csv"

    assert os.path.exists(
        os.path.join(csv_folder, csv_file)
    ), f"Expected file {csv_file} not found in {csv_folder}"


def check_file_not_exists(datastore_path, date: datetime):
    """Verify that no CSV file exists in the datastore for the given date."""
    csv_folder = os.path.join(
        datastore_path,
        "hk/sc/l1",
        SC_HK_DESCRIPTOR,
        date.strftime("%Y/%m"),
    )
    csv_file = f"imap_sc_l1_{SC_HK_DESCRIPTOR}_{date.strftime('%Y%m%d')}_v001.csv"

    assert not os.path.exists(
        os.path.join(csv_folder, csv_file)
    ), f"File {csv_file} should not exist in {csv_folder}"


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_first_ever_run(
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

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow()

    # Verify file exists for BEGINNING_OF_IMAP
    datastore = AppSettings().data_store
    check_file_existence(str(datastore), BEGINNING_OF_IMAP)

    # Verify workflow progress was updated
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == BEGINNING_OF_IMAP.replace(
        hour=23, minute=59, second=59
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_continue_from_previous(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
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

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow()

    # Verify file exists for the next day
    datastore = AppSettings().data_store
    check_file_existence(str(datastore), next_day)

    # Verify workflow progress was updated to the next day
    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == next_day.replace(
        hour=23, minute=59, second=59
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_no_new_data(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When all days return empty CSV, progress timestamp stays unchanged."""
    wiremock_manager.reset()

    # Only empty data available
    define_unavailable_latis_mapping(wiremock_manager)

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow()

    # Verify workflow progress was checked but not advanced
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_manual_date_range(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
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

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow(
            start_date=start_date,
            end_date=end_date,
        )

    # Verify files exist for both days
    datastore = AppSettings().data_store
    check_file_existence(str(datastore), start_date)
    check_file_existence(str(datastore), end_date)

    # Verify workflow progress was updated (use_database=True for non-force mode)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == end_date.replace(
        hour=23, minute=59, second=59
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_force_redownload(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Force redownload mode should not update workflow progress."""
    wiremock_manager.reset()

    start_date = datetime(2026, 1, 15)
    end_date = datetime(2026, 1, 15)

    define_available_latis_mapping(
        wiremock_manager,
        start_date,
        start_date + timedelta(days=1),
        SAMPLE_CSV_WITH_DATA,
    )

    define_unavailable_latis_mapping(wiremock_manager)

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow(
            start_date=start_date,
            end_date=end_date,
            force_redownload=True,
        )

    # Verify file exists
    datastore = AppSettings().data_store
    check_file_existence(str(datastore), start_date)

    # Verify workflow progress was NOT updated (force redownload mode)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() is None
    assert workflow_progress.get_progress_timestamp() is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_already_up_to_date(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
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

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow()

    # Progress should remain the same
    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_progress_timestamp() == progress_timestamp


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_webtcad_latis_skips_days_without_data(
    wiremock_manager,
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
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

    with Environment(
        IMAP_WEBTCAD_LATIS_URL=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_webtcad_latis_flow(
            start_date=day1,
            end_date=day3,
        )

    # Only day2 should have a file
    datastore = AppSettings().data_store
    check_file_not_exists(str(datastore), day1)
    check_file_existence(str(datastore), day2)
    check_file_not_exists(str(datastore), day3)

    # Progress should be set to day2 (last day with data)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_progress_timestamp() == day2.replace(
        hour=23, minute=59, second=59
    )
