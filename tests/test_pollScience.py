import json
import os
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from imap_mag.util import ScienceMode
from prefect_server.pollScience import poll_science_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_TODAY,
    NOW,
    TODAY,
    YESTERDAY,
    create_test_file,
    mock_datetime_provider,  # noqa: F401
    set_env,
    tidyDataFolders,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


def define_available_data_sdc_mappings(
    wiremock_manager,
    mode: ScienceMode,
    start_date: datetime,
    end_date: datetime,
    ingestion_timestamp: datetime,
    is_ingestion_date: bool = False,
):
    science_file = create_test_file(
        Path(tempfile.gettempdir()) / "science.cdf", "some super scientific content"
    )

    mode_str = mode.short_name
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    prefix = "ingestion_" if is_ingestion_date else ""

    query_response: list[dict[str, str]] = [
        {
            "file_path": f"imap/mag/l1c/{start_date.year}/{start_date.month:02}/imap_mag_l1c_{mode_str}-magi_{start_date_str}_v000.cdf",
            "instrument": "mag",
            "data_level": "l1c",
            "descriptor": f"{mode_str}-magi",
            "start_date": start_date_str,
            "repointing": None,
            "version": "v000",
            "extension": "cdf",
            "ingestion_date": ingestion_timestamp.strftime("%Y%m%d %H:%M:%S"),
        }
    ]

    wiremock_manager.add_string_mapping(
        f"/query?instrument=mag&data_level=l1c&descriptor={mode_str}-magi&{prefix}start_date={start_date_str}&{prefix}end_date={end_date_str}&extension=cdf",
        json.dumps(query_response),
        priority=1,
    )
    wiremock_manager.add_file_mapping(
        f"/download/imap/mag/l1c/{start_date.year}/{start_date.month:02}/imap_mag_l1c_{mode_str}-magi_{start_date_str}_v000.cdf",
        science_file,
    )


def define_unavailable_data_sdc_mappings(wiremock_manager):
    wiremock_manager.add_string_mapping(
        re.escape("/query?instrument=mag&data_level=")
        + r"[lL]\d\w?"
        + re.escape("&descriptor=")
        + r".*"
        + re.escape("&")
        + r"(:?ingestion_)?"
        + re.escape("start_date=")
        + r"\d{8}"
        + re.escape("&")
        + r"(:?ingestion_)?"
        + re.escape("end_date=")
        + r"\d{8}"
        + re.escape("&extension=cdf"),
        json.dumps({}),
        is_pattern=True,
        priority=2,
    )


def verify_not_requested_modes(database, not_requested_modes: list[ScienceMode]):
    for mode in not_requested_modes:
        download_progress = database.get_download_progress(mode.packet + "_L1C")

        assert download_progress.get_last_checked_date() is None
        assert download_progress.get_progress_timestamp() is None


def verify_not_available_modes(database, not_available_modes: list[ScienceMode]):
    for mode in not_available_modes:
        download_progress = database.get_download_progress(mode.packet + "_L1C")

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() is None


def verify_available_modes(
    database,
    available_modes: list[ScienceMode],
    ingestion_timestamp: datetime,
    actual_timestamp: datetime,
):
    for mode in available_modes:
        # Database.
        download_progress = database.get_download_progress(mode.packet + "_L1C")

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() == ingestion_timestamp

    # Files.
    check_file_existence(available_modes, actual_timestamp)


def check_file_existence(modes_to_check: list[ScienceMode], actual_timestamp: datetime):
    for mode in modes_to_check:
        data_folder = os.path.join(
            "output/imap/mag/l1c", actual_timestamp.strftime("%Y/%m")
        )
        cdf_file = f"imap_mag_l1c_{mode.short_name}-magi_{actual_timestamp.strftime('%Y%m%d')}_v000.cdf"

        assert os.path.exists(os.path.join(data_folder, cdf_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_science_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    ingestion_timestamp = datetime(2025, 4, 2, 13, 37, 9)

    wiremock_manager.reset()

    # Some data is available for "yesterday" and "today" for Normal mode.
    define_available_data_sdc_mappings(
        wiremock_manager,
        ScienceMode.Normal,
        YESTERDAY,
        END_OF_TODAY,
        ingestion_timestamp,
        is_ingestion_date=True,
    )

    # No data is available for any other date/packet.
    define_unavailable_data_sdc_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_SCIENCE_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await poll_science_flow()

    # Verify.
    verify_not_available_modes(test_database, [ScienceMode.Burst])
    verify_available_modes(
        test_database,
        [ScienceMode.Normal],
        ingestion_timestamp,
        YESTERDAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_science_autoflow_continue_from_previous_download(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)
    ingestion_timestamp = progress_timestamp + timedelta(hours=1, minutes=37, seconds=9)

    wiremock_manager.reset()

    # Some data is available for "today" for Normal mode.
    download_progress = test_database.get_download_progress(
        ScienceMode.Normal.packet + "_L1C"
    )
    download_progress.record_successful_download(progress_timestamp)
    test_database.save(download_progress)

    define_available_data_sdc_mappings(
        wiremock_manager,
        ScienceMode.Normal,
        progress_timestamp,
        END_OF_TODAY,
        ingestion_timestamp,
        is_ingestion_date=True,
    )

    # No data is available for any other date/packet.
    define_unavailable_data_sdc_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_SCIENCE_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await poll_science_flow()

    # Verify.
    verify_not_available_modes(test_database, [ScienceMode.Burst])
    verify_available_modes(
        test_database,
        [ScienceMode.Normal],
        ingestion_timestamp,
        progress_timestamp,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
@pytest.mark.parametrize("force_database_update", [True, False])
async def test_poll_science_specify_packets_and_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    force_database_update,
    capture_logs,
):
    # Set up.
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)

    ingestion_timestamp = datetime(2025, 4, 2, 13, 37, 9)

    wiremock_manager.reset()

    # Some data is available for the requested dates for Burst mode.
    define_available_data_sdc_mappings(
        wiremock_manager,
        ScienceMode.Burst,
        start_date,
        end_date,
        ingestion_timestamp,
        is_ingestion_date=False,
    )

    # No data is available for any other date/packet.
    define_unavailable_data_sdc_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_SCIENCE_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await poll_science_flow(
            modes=[ScienceMode.Burst],
            start_date=start_date,
            end_date=end_date,
            force_database_update=force_database_update,
        )

    # Verify.
    check_file_existence([ScienceMode.Burst], start_date)

    if force_database_update:
        assert (
            "Database cannot be updated without forcing ingestion date. Database will not be updated."
            in capture_logs.text
        )

    # Database should not be updated by default, when start and end dates are provided.
    verify_not_requested_modes(test_database, [m for m in ScienceMode])


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_science_specify_ingestion_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)

    ingestion_timestamp = datetime(2025, 4, 2, 13, 37, 9)

    wiremock_manager.reset()

    # Some data is available for the requested dates for Burst mode.
    define_available_data_sdc_mappings(
        wiremock_manager,
        ScienceMode.Burst,
        start_date,
        end_date,
        ingestion_timestamp,
        is_ingestion_date=True,
    )

    # No data is available for any other date/packet.
    define_unavailable_data_sdc_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_SCIENCE_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await poll_science_flow(
            modes=[ScienceMode.Burst],
            start_date=start_date,
            end_date=end_date,
            force_database_update=True,
            force_ingestion_date=True,
        )

    # Verify.
    verify_not_requested_modes(test_database, [ScienceMode.Normal])
    verify_available_modes(
        test_database,
        [ScienceMode.Burst],
        ingestion_timestamp,
        start_date,
    )
