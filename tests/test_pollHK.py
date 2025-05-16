import os
import re
from datetime import datetime, timedelta

import pytest

from imap_mag.util import HKPacket
from prefect_server.pollHK import poll_hk_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_TODAY,
    NOW,
    TODAY,
    enableLogging,  # noqa: F401
    mock_datetime_provider,  # noqa: F401
    set_env,
    tidyDataFolders,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


def define_unavailable_data_webpoda_mappings(wiremock_manager):
    empty_file = os.path.abspath("tests/data/2025/EMPTY_HK.pkts")

    wiremock_manager.add_file_mapping(
        re.escape("/packets/SID2/")
        + r".*"
        + re.escape(".bin?")
        + r"(?:ert|time)"
        + re.escape("%3E=")
        + r".*"
        + re.escape("&")
        + r"(?:ert|time)"
        + r".*"
        + re.escape("&project(packet)"),
        empty_file,
        is_pattern=True,
        priority=2,
    )

    wiremock_manager.add_string_mapping(
        re.escape("/packets/SID2/")
        + r".*"
        + re.escape(".csv?")
        + r"(?:ert|time)"
        + re.escape("%3E=")
        + r".*"
        + re.escape("&")
        + r"(?:ert|time)"
        + re.escape("%3C")
        + r".*"
        + re.escape("&project(")
        + r"(?:ert|time)"
        + re.escape(")&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)"),
        "time_var\n",
        is_pattern=True,
        priority=2,
    )


def verify_not_requested_hk(database, not_requested_hk: list[HKPacket]):
    for hk in not_requested_hk:
        download_progress = database.get_download_progress(hk.packet)

        assert download_progress.get_last_checked_date() is None
        assert download_progress.get_progress_timestamp() is None


def verify_not_available_hk(database, not_available_hk: list[HKPacket]):
    for hk in not_available_hk:
        download_progress = database.get_download_progress(hk.packet)

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() is None


def verify_available_hk(
    database,
    available_hk: list[HKPacket],
    ert_timestamp: datetime,
    actual_timestamp: datetime,
):
    for hk in available_hk:
        # Database.
        download_progress = database.get_download_progress(hk.packet)

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() == ert_timestamp

        # Files.
        data_folder = os.path.join("output", actual_timestamp.strftime("%Y/%m/%d"))
        bin_file = (
            f"imap_mag_{hk.packet.lstrip('MAG_').lower().replace('_', '-')}_"
            + f"{actual_timestamp.strftime('%Y%m%d')}_v000.pkts"
        )
        csv_file = (
            f"imap_mag_{hk.packet.lstrip('MAG_').lower().replace('_', '-')}_"
            + f"{actual_timestamp.strftime('%Y%m%d')}_v000.csv"
        )

        assert os.path.exists(os.path.join(data_folder, bin_file))
        assert os.path.exists(os.path.join(data_folder, csv_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    binary_file = os.path.abspath("tests/data/2025/MAG_HSK_PW.pkts")

    today = TODAY.strftime("%Y-%m-%dT%H:%M:%S")
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = datetime(2025, 4, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID11_PROCSTAT,
    ]
    not_available_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(available_hk)
    )

    wiremock_manager.reset()

    # Some data is available for "today", only for specific packets.
    for hk in available_hk:
        wiremock_manager.add_file_mapping(
            f"/packets/SID2/{hk.packet}.bin?ert%3E={today}&ert%3C{end_of_today}&project(packet)",
            binary_file,
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk.packet}.csv?ert%3E={today}&ert%3C{end_of_today}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk.packet}.csv?ert%3E={today}&ert%3C{end_of_today}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_BINARY_API_URL_BASE", wiremock_manager.get_url()),
        set_env("WEBPODA_AUTH_CODE", "12345"),
    ):
        await poll_hk_flow()

    # Verify.
    verify_not_available_hk(test_database, not_available_hk)
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_autoflow_continue_from_previous_download(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    binary_file = os.path.abspath("tests/data/2025/MAG_HSK_PW.pkts")

    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = progress_timestamp + timedelta(hours=1, minutes=37, seconds=9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID15,
    ]
    not_available_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(available_hk)
    )

    wiremock_manager.reset()

    # Some data is available for "today", only for specific packets.
    for hk in available_hk:
        download_progress = test_database.get_download_progress(hk.packet)
        download_progress.record_successful_download(progress_timestamp)
        test_database.save(download_progress)

        wiremock_manager.add_file_mapping(
            f"/packets/SID2/{hk.packet}.bin?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(packet)",
            binary_file,
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk.packet}.csv?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk.packet}.csv?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_BINARY_API_URL_BASE", wiremock_manager.get_url()),
        set_env("WEBPODA_AUTH_CODE", "12345"),
    ):
        await poll_hk_flow()

    # Verify.
    verify_not_available_hk(test_database, not_available_hk)
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
@pytest.mark.parametrize("force_database_update", [True, False])
async def test_poll_hk_specify_packets_and_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811,
    force_database_update,
):
    # Set up.
    binary_file = os.path.abspath("tests/data/2025/MAG_HSK_PW.pkts")

    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)
    actual_end_date_for_download = datetime(2025, 4, 3)

    ert_timestamp = datetime(2025, 4, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 4, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID5_SCI,
    ]
    not_available_hk: list[HKPacket] = [HKPacket.SID11_PROCSTAT, HKPacket.SID15]

    requested_hk: list[HKPacket] = available_hk + not_available_hk
    not_requested_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(requested_hk)
    )

    wiremock_manager.reset()

    # Some data is available for the requested dates, only for specific packets.
    for hk in available_hk:
        for date_pair in [
            (
                start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                end_date.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
            (
                end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                actual_end_date_for_download.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        ]:
            wiremock_manager.add_file_mapping(
                f"/packets/SID2/{hk.packet}.bin?time%3E={date_pair[0]}&time%3C{date_pair[1]}&project(packet)",
                binary_file,
                priority=1,
            )
            wiremock_manager.add_string_mapping(
                f"/packets/SID2/{hk.packet}.csv?time%3E={date_pair[0]}&time%3C{date_pair[1]}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
                f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
                priority=1,
            )
            wiremock_manager.add_string_mapping(
                f"/packets/SID2/{hk.packet}.csv?time%3E={date_pair[0]}&time%3C{date_pair[1]}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
                f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
                priority=1,
            )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with (
        set_env("MAG_FETCH_BINARY_API_URL_BASE", wiremock_manager.get_url()),
        set_env("WEBPODA_AUTH_CODE", "12345"),
    ):
        await poll_hk_flow(
            start_date=start_date,
            end_date=end_date,
            hk_packets=requested_hk,
            force_database_update=force_database_update,
        )

    # Verify.
    if force_database_update:
        verify_not_requested_hk(test_database, not_requested_hk)
        verify_not_available_hk(test_database, not_available_hk)
        verify_available_hk(
            test_database,
            available_hk,
            ert_timestamp,
            actual_timestamp,
        )
    else:
        # Database should not be updated by default, when start and end dates are provided.
        verify_not_requested_hk(test_database, [p for p in HKPacket])
